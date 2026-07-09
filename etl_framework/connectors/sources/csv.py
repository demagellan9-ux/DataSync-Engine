"""
CSV source connector.

extract()        — loads the full file into a single DataFrame (fast, higher memory).
extract_chunks() — uses pd.read_csv(chunksize=N) for true row-level streaming.

pd.read_csv with chunksize returns a TextFileReader iterator; each iteration
reads exactly chunk_size rows without holding the rest of the file in memory.
"""

from collections.abc import Iterator

import pandas as pd

from etl_framework.connectors.base import BaseSourceConnector
from etl_framework.logger import audit
from etl_framework.models import RawSheet


class CsvConnector(BaseSourceConnector):
    """
    Treats a single CSV file as a one-sheet source.
    sheet_name controls the RawSheet.name used in record_key generation.
    """

    def __init__(self, path: str, sheet_name: str = "csv",
                 encoding: str = "utf-8") -> None:
        self._path       = path
        self._sheet_name = sheet_name
        self._encoding   = encoding

    @property
    def source_name(self) -> str:
        return self._path

    # ── Full-load path (chunk_size == 0) ─────────────────────────────────────

    def extract(self) -> list[RawSheet]:
        df = pd.read_csv(self._path, dtype=str, encoding=self._encoding)
        df.columns = df.columns.str.strip()
        if df.empty:
            audit("extract_skip", sheet=self._sheet_name, trigger="empty file")
            return []
        rows = df.to_dict(orient="records")
        audit("extract_complete", sheet=self._sheet_name, rows_total=len(rows))
        return [RawSheet(name=self._sheet_name, rows=rows)]

    # ── Streaming path (chunk_size > 0) ──────────────────────────────────────

    def extract_chunks(self, chunk_size: int) -> Iterator[RawSheet]:
        """
        Stream the CSV file using pandas TextFileReader (pd.read_csv chunksize).

        Each yielded chunk is at most chunk_size rows. pandas buffers only the
        current chunk, so peak memory scales with chunk_size rather than file size.
        All values remain dtype=str to match the full-load path.
        """
        total_rows = 0
        reader = pd.read_csv(
            self._path,
            dtype=str,
            encoding=self._encoding,
            chunksize=chunk_size,
        )
        for df_chunk in reader:
            df_chunk.columns = df_chunk.columns.str.strip()
            if df_chunk.empty:
                continue
            rows = df_chunk.to_dict(orient="records")
            total_rows += len(rows)
            audit("extract_chunk", sheet=self._sheet_name, rows_total=len(rows))
            yield RawSheet(name=self._sheet_name, rows=rows)

        if total_rows == 0:
            audit("extract_skip", sheet=self._sheet_name, trigger="empty file")
        else:
            audit("extract_complete", sheet=self._sheet_name, rows_total=total_rows)
