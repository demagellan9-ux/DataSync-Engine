"""
MongoDB destination connector.

MongoClient instances are expensive to create (they spawn background monitor
threads and open connection pools). This module keeps a process-level registry
of clients keyed by (uri, timeout_ms, pool settings) so every ETL cycle —
including parallel sheet runs — shares the same connection pool instead of
opening a new one.

Thread safety: _ClientRegistry uses a Lock for the lookup/create critical
section. MongoClient itself is thread-safe for concurrent operations.
"""

import threading
from typing import NamedTuple

from pymongo import MongoClient, UpdateOne

from etl_framework.connectors.base import BaseDestinationConnector
from etl_framework.logger import audit
from etl_framework.models import Document, LoadResult


# ── Global client registry ────────────────────────────────────────────────────

class _ClientKey(NamedTuple):
    uri:              str
    timeout_ms:       int
    max_pool_size:    int
    min_pool_size:    int
    max_idle_time_ms: int


class _ClientRegistry:
    """
    Process-level store of live MongoClient instances.
    Keyed by (uri + all pool/timeout settings) so profiles with different
    connection parameters each get their own correctly configured client.
    """

    def __init__(self) -> None:
        self._lock:    threading.Lock                = threading.Lock()
        self._clients: dict[_ClientKey, MongoClient] = {}

    def get(
        self,
        uri:              str,
        timeout_ms:       int,
        max_pool_size:    int,
        min_pool_size:    int,
        max_idle_time_ms: int,
    ) -> MongoClient:
        key = _ClientKey(uri, timeout_ms, max_pool_size, min_pool_size, max_idle_time_ms)
        # Fast path — no lock needed once the key is present
        if key in self._clients:
            return self._clients[key]
        with self._lock:
            # Re-check under lock to handle simultaneous first callers
            if key not in self._clients:
                self._clients[key] = MongoClient(
                    uri,
                    serverSelectionTimeoutMS=timeout_ms,
                    maxPoolSize=max_pool_size,
                    minPoolSize=min_pool_size,
                    maxIdleTimeMS=max_idle_time_ms,
                )
            return self._clients[key]

    def close_all(self) -> None:
        """Graceful shutdown — call at process exit if desired."""
        with self._lock:
            for client in self._clients.values():
                client.close()
            self._clients.clear()


_registry = _ClientRegistry()


# ── Connector ─────────────────────────────────────────────────────────────────

class MongoDBConnector(BaseDestinationConnector):
    """
    Destination connector for MongoDB.

    Borrows a shared MongoClient from the process-level registry; never opens
    or closes a client itself. Safe to instantiate multiple times (e.g. once
    per ETL cycle) without accumulating connections.
    """

    def __init__(
        self,
        uri:              str,
        database:         str,
        timeout_ms:       int = 5000,
        batch_size:       int = 0,
        max_pool_size:    int = 10,
        min_pool_size:    int = 1,
        max_idle_time_ms: int = 30000,
    ) -> None:
        self._uri              = uri
        self._database         = database
        self._timeout_ms       = timeout_ms
        self._batch_size       = batch_size
        self._max_pool_size    = max_pool_size
        self._min_pool_size    = min_pool_size
        self._max_idle_time_ms = max_idle_time_ms

    @property
    def destination_name(self) -> str:
        return f"{self._uri}{self._database}"

    def _collection(self, name: str):
        client = _registry.get(
            self._uri,
            self._timeout_ms,
            self._max_pool_size,
            self._min_pool_size,
            self._max_idle_time_ms,
        )
        return client[self._database][name]

    def load(self, documents: list[Document], collection: str) -> LoadResult:
        import time

        result = LoadResult(total=len(documents), collection=collection)
        if not documents:
            return result

        col = self._collection(collection)

        # Fetch existing hashes in one query
        incoming_keys = [
            d.meta["record_key"]
            for d in documents
            if d.meta.get("record_key") is not None
        ]
        existing_hashes: dict[str, str] = {
            doc["_meta"]["record_key"]: doc["_meta"].get("row_hash")
            for doc in col.find(
                {"_meta.record_key": {"$in": incoming_keys}},
                {"_meta.record_key": 1, "_meta.row_hash": 1, "_id": 0},
            )
        }

        operations: list[UpdateOne] = []
        for doc in documents:
            record_key = doc.meta.get("record_key")
            if record_key is None:
                result.skipped_key += 1
                continue
            if existing_hashes.get(record_key) == doc.meta.get("row_hash"):
                result.skipped_hash += 1
                continue
            operations.append(
                UpdateOne(
                    filter={"_meta.record_key": record_key},
                    update={"$set": doc.to_dict()},
                    upsert=True,
                )
            )

        if not operations:
            audit(
                "load_no_changes",
                sheet=collection,
                rows_total=result.total,
                skipped=result.skipped_hash + result.skipped_key,
            )
            return result

        t0 = time.monotonic()
        for batch in self._batches(operations):
            br = col.bulk_write(batch, ordered=False)
            result.inserted += br.upserted_count
            result.updated  += br.modified_count

        audit(
            "load_complete",
            sheet=collection,
            rows_total=result.total,
            inserted=result.inserted,
            updated=result.updated,
            skipped=result.skipped_hash + result.skipped_key,
            elapsed_ms=int((time.monotonic() - t0) * 1000),
        )

        return result

    def _batches(self, operations: list[UpdateOne]):
        """Yield operations in chunks. If batch_size is 0, yield the full list once."""
        if not self._batch_size:
            yield operations
            return
        for i in range(0, len(operations), self._batch_size):
            yield operations[i : i + self._batch_size]
