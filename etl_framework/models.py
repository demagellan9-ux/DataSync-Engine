"""
Shared data contracts passed between Source Connectors, ETL Engine, and Destination Connectors.
No layer imports from another layer's internals — only from this module.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RawSheet:
    """Output of every Source Connector — one instance per worksheet/tab."""
    name: str                          # sheet/tab name as it appears in the source
    rows: list[dict[str, Any]]         # list of raw row dicts, headers as keys, values as strings


@dataclass
class Document:
    """
    A single record ready for the Destination Connector.
    meta  — pipeline metadata (_meta in MongoDB)
    data  — normalized, transformed row data
    validation_errors — non-empty only for invalid documents
    """
    meta:              dict[str, Any]
    data:              dict[str, Any]
    validation_errors: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.validation_errors) == 0

    def to_dict(self) -> dict:
        """Serialize to the wire format expected by destination connectors."""
        doc = {"_meta": self.meta, "data": self.data}
        if self.validation_errors:
            doc["validation_errors"] = self.validation_errors
        return doc


@dataclass
class LoadResult:
    """Returned by every Destination Connector after a load operation."""
    inserted:     int = 0
    updated:      int = 0
    skipped_hash: int = 0   # unchanged records skipped by incremental sync
    skipped_key:  int = 0   # records missing a resolvable record_key
    total:        int = 0
    collection:   str = ""
