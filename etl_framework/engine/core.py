"""
Core ETL Engine — source-agnostic and destination-agnostic.

Accepts RawSheets from any source connector.
Produces (valid_documents, invalid_documents) for any destination connector.
All configuration is injected; nothing is imported from config.py directly.
"""

import time
from datetime import datetime, timezone
from typing import Any

from etl_framework.engine.aliases import apply_aliases
from etl_framework.engine.fingerprint import hash_record
from etl_framework.engine.key_detection import detect_upsert_key
from etl_framework.engine.sanitizer import sanitize
from etl_framework.engine.transforms import apply_transforms
from etl_framework.engine.validators import validate_records
from etl_framework.logger import audit
from etl_framework.models import Document, RawSheet


class ETLEngine:
    """
    Stateless processing engine. Instantiate once with config, call process() per run.
    """

    def __init__(
        self,
        alias_map:        dict[str, str],
        upsert_candidates: list[str],
        transforms:        dict[str, str],
        validations:       dict[str, list[dict]],
    ) -> None:
        self._alias_map         = alias_map
        self._upsert_candidates = upsert_candidates
        self._transforms        = transforms
        self._validations       = validations

    def process(
        self,
        sheets:   list[RawSheet],
        workbook: str,
    ) -> tuple[list[Document], list[Document]]:
        """
        Transform a list of RawSheets into (valid_documents, invalid_documents).

        Stamps a single imported_at for the entire cycle, then delegates each
        sheet to process_sheet(). Caller may instead call process_sheet() directly
        for parallel fan-out while sharing the same timestamp.
        """
        imported_at  = datetime.now(timezone.utc).isoformat()
        all_valid:   list[Document] = []
        all_invalid: list[Document] = []

        for sheet in sheets:
            valid, invalid = self.process_sheet(sheet, workbook, imported_at)
            all_valid.extend(valid)
            all_invalid.extend(invalid)

        return all_valid, all_invalid

    def process_sheet(
        self,
        sheet:              RawSheet,
        workbook:           str,
        imported_at:        str,
        valid_row_offset:   int = 0,
        invalid_row_offset: int = 0,
    ) -> tuple[list[Document], list[Document]]:
        """
        Transform and validate a single RawSheet.

        Thread-safe: reads only immutable config attributes set at construction.
        Safe to call concurrently from multiple threads with different sheets.

        Args:
            valid_row_offset:   cumulative count of valid rows processed for this sheet
                                in prior chunks. Used to generate stable row_number values
                                across chunk boundaries so record_key remains deterministic.
            invalid_row_offset: same as above for invalid rows.

        Row numbering formula: row_number = 2 + offset + enumerate_index
            - The leading 2 accounts for the 1-indexed sheet header row.
            - offset carries forward the count from previous chunks.
            - Defaults of 0 reproduce the existing non-chunked behaviour exactly.
        """
        t0 = time.monotonic()

        rows = sanitize(sheet.rows)
        rows = apply_aliases(rows, self._alias_map, sheet.name)
        rows = apply_transforms(rows, self._transforms, sheet.name)

        col_names  = list(rows[0].keys()) if rows else []
        upsert_key = detect_upsert_key(col_names, self._upsert_candidates, sheet.name)

        valid_rows, invalid_rows = validate_records(rows, self._validations, sheet.name)

        audit(
            "transform_complete",
            sheet=sheet.name,
            rows_total=len(rows),
            rows_valid=len(valid_rows),
            rows_invalid=len(invalid_rows),
            elapsed_ms=int((time.monotonic() - t0) * 1000),
        )

        valid_docs = [
            self._build_document(row, 2 + valid_row_offset + i,
                                 sheet.name, workbook, imported_at,
                                 upsert_key, is_valid=True)
            for i, row in enumerate(valid_rows)
        ]
        invalid_docs = [
            self._build_document(row, 2 + invalid_row_offset + i,
                                 sheet.name, workbook, imported_at,
                                 upsert_key, is_valid=False)
            for i, row in enumerate(invalid_rows)
        ]

        return valid_docs, invalid_docs

    # ── private ──────────────────────────────────────────────────────────────

    def _build_document(
        self,
        row:        dict[str, Any],
        row_number: int,
        sheet_name: str,
        workbook:   str,
        imported_at: str,
        upsert_key: str | None,
        is_valid:   bool,
    ) -> Document:
        key_value  = row.get(upsert_key) if upsert_key else None
        record_key = (
            f"{sheet_name}::{key_value}"
            if key_value is not None
            else f"{workbook}::{sheet_name}::{row_number}"
        )

        # Hash only clean data (without _validation_errors sentinel)
        data = {k: v for k, v in row.items() if k != "_validation_errors"}

        return Document(
            meta={
                "workbook":    workbook,
                "sheet":       sheet_name,
                "row_number":  row_number,
                "imported_at": imported_at,
                "record_key":  record_key,
                "row_hash":    hash_record(data),
            },
            data=data,
            validation_errors=row.get("_validation_errors", []) if not is_valid else [],
        )
