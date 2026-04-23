"""Local filesystem Parquet adapter — same interface as S3ParquetAdapter."""

from __future__ import annotations

import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from load_assurance.adapters.base import AdapterStats, AbstractAdapter


class LocalParquetAdapter(AbstractAdapter):
    """Read Parquet statistics from local filesystem — mirrors S3 adapter interface."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def _location(self) -> str:
        return str(self.path)

    # def get_stats(
    #     self,
    #     null_rate_columns: list[str] | None = None,
    #     sample_fraction: float = 0.05,
    # ) -> AdapterStats:
    #     files = self._list_parquet_files()
    #     if not files:
    #         raise FileNotFoundError(f"No parquet files found at {self.path}")

    #     row_count, schema = self._metadata_stats(files)
    #     null_counts: dict[str, int] = {}
    #     sample_rows = 0

    #     if null_rate_columns:
    #         null_counts, sample_rows = self._sample_null_counts(
    #             files, null_rate_columns, sample_fraction
    #         )

    #     return AdapterStats(
    #         row_count=row_count,
    #         schema=schema,
    #         null_counts=null_counts,
    #         sample_rows=sample_rows,
    #         file_count=len(files),
    #     )

    def _list_files(self) -> list[Path]:
        if self.path.is_file():
            return [self.path]
        return sorted(self.path.glob("**/*.parquet")) + sorted(self.path.glob("**/*.parq"))

    def _metadata_stats(self, files: list[Path]) -> tuple[int, pa.Schema]:
        total_rows = 0
        schema: pa.Schema | None = None
        for f in files:
            meta = pq.read_metadata(f)
            total_rows += meta.num_rows
            if schema is None:
                schema = meta.schema.to_arrow_schema()
        assert schema is not None
        return total_rows, schema

    def _sample_null_counts(
        self,
        files: list[Path],
        columns: list[str],
        sample_fraction: float,
    ) -> tuple[dict[str, int], int]:
        n_sample = max(1, round(len(files) * sample_fraction))
        sampled = random.sample(files, n_sample)

        null_counts: dict[str, int] = {col: 0 for col in columns}
        total_rows = 0

        for f in sampled:
            table = pq.read_table(f, columns=columns)
            total_rows += len(table)
            for col in columns:
                if col in table.schema.names:
                    null_counts[col] += table.column(col).null_count

        return null_counts, total_rows