import pyarrow as pa

from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import BinaryIO

@dataclass
class AdapterStats:
    """Everything the engine needs from an adapter — no raw data."""
    row_count: int
    schema: pa.Schema
    null_counts: dict[str, int] = field(default_factory=dict)
    """null_counts[col] = number of nulls observed in the sample."""
    sample_rows: int = 0
    """How many rows were actually read when computing null_counts."""
    file_count: int = 0

class AbstractAdapter(ABC):

    def get_stats(
        self,
        null_rate_columns: list[str] | None = None,
        sample_fraction: float = 0.05,
    ) -> AdapterStats:
        files = self._list_files()
        if not files:
            raise FileNotFoundError(f"No files found at {self._location()}")
 
        row_count, schema = self._metadata_stats(files)
        null_counts: dict[str, int] = {}
        sample_rows = 0
 
        if null_rate_columns:
            null_counts, sample_rows = self._sample_null_counts(
                files, null_rate_columns, sample_fraction
            )
 
        return AdapterStats(
            row_count=row_count,
            schema=schema,
            null_counts=null_counts,
            sample_rows=sample_rows,
            file_count=len(files),
        )

    @abstractmethod
    def _location(self) -> str:
        """Human-readable location string for error messages."""
        ...
 
    @abstractmethod
    def _list_files(self) -> list:
        """Return all files to process."""
        ...
 
    @abstractmethod
    def _metadata_stats(self, files: list) -> tuple[int, pa.Schema]:
        """Return (row_count, schema) using cheapest method available."""
        ...
 
    @abstractmethod
    def _sample_null_counts(
        self,
        files: list,
        columns: list[str],
        sample_fraction: float,
    ) -> tuple[dict[str, int], int]:
        """Return (null_counts_per_col, total_sampled_rows)."""
        ...