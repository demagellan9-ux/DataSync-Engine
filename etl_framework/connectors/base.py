"""
Abstract connector interfaces.

Every Source Connector must implement BaseSourceConnector.
Every Destination Connector must implement BaseDestinationConnector.
The ETL engine depends only on these interfaces — never on concrete implementations.
"""

from abc import ABC, abstractmethod
from collections.abc import Iterator

from etl_framework.models import Document, LoadResult, RawSheet


class BaseSourceConnector(ABC):
    """
    Contract for all source connectors (Excel, Google Sheets, CSV, SQL, REST …).
    extract() must return one RawSheet per worksheet/tab/table found in the source.
    """

    @abstractmethod
    def extract(self) -> list[RawSheet]:
        """
        Read all available sheets from the source.
        Returns a list of RawSheet instances with string-typed cell values.
        Never raises for empty sheets — return an empty list instead.
        """

    def extract_chunks(self, chunk_size: int) -> Iterator[RawSheet]:
        """
        Stream the source as RawSheet chunks, yielding one chunk at a time to
        keep peak memory bounded. Chunks from the same sheet are yielded
        contiguously; RawSheet.name identifies which sheet each chunk belongs to.

        Default implementation wraps extract() — yields each full sheet as a
        single chunk. Concrete connectors (Excel, CSV) override this with true
        row-level streaming when chunk_size > 0.

        Args:
            chunk_size: maximum number of rows per yielded RawSheet.
        """
        yield from self.extract()

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Human-readable identifier for this source (e.g. filename or spreadsheet ID)."""


class BaseDestinationConnector(ABC):
    """
    Contract for all destination connectors (MongoDB, PostgreSQL, SQLite, REST …).
    load() receives a flat list of Documents and returns a LoadResult.
    """

    @abstractmethod
    def load(self, documents: list[Document], collection: str) -> LoadResult:
        """
        Persist documents to the destination.
        Must be idempotent: calling load() twice with the same documents
        must produce the same final state.
        """

    @property
    @abstractmethod
    def destination_name(self) -> str:
        """Human-readable identifier for this destination."""
