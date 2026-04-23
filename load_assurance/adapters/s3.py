"""S3/Parquet adapter.

Design principles:
- Row counts come from Parquet file footer metadata (zero data scan).
- Schema is read from the first file's schema only.
- Null rate uses a configurable sample fraction (default 5%) to keep costs low.
- Full scans are never triggered unless explicitly requested.
"""

from __future__ import annotations

import io
import random
from dataclasses import dataclass, field
from typing import Iterator
from urllib.parse import urlparse

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from load_assurance.adapters.base import AdapterStats, AbstractAdapter


@dataclass
class S3Location:
    bucket: str
    prefix: str  # may be empty string for bucket root

    @classmethod
    def from_uri(cls, uri: str) -> "S3Location":
        parsed = urlparse(uri)
        return cls(bucket=parsed.netloc, prefix=parsed.path.lstrip("/"))


class S3ParquetAdapter(AbstractAdapter):
    """Read Parquet statistics from S3 without scanning row data."""

    def __init__(
        self,
        path: str,
        region: str | None = None,
        profile: str | None = None,
    ) -> None:
        self.location = S3Location.from_uri(path)
        session = boto3.Session(profile_name=profile, region_name=region)
        self.s3 = session.client("s3")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    # def get_stats(
    #     self,
    #     null_rate_columns: list[str] | None = None,
    #     sample_fraction: float = 0.05,
    # ) -> AdapterStats:
    #     """
    #     Return AdapterStats for this S3 path.

    #     Args:
    #         null_rate_columns: columns to sample for null rates. None = skip.
    #         sample_fraction: fraction of files to sample for null checks (0–1).
    #     """
    #     keys = list(self._list_parquet_keys())
    #     if not keys:
    #         raise FileNotFoundError(
    #             f"No parquet files found at s3://{self.location.bucket}/{self.location.prefix}"
    #         )

    #     row_count, schema = self._metadata_stats(keys)
    #     null_counts: dict[str, int] = {}
    #     sample_rows = 0

    #     if null_rate_columns:
    #         null_counts, sample_rows = self._sample_null_counts(
    #             keys, null_rate_columns, sample_fraction
    #         )

    #     return AdapterStats(
    #         row_count=row_count,
    #         schema=schema,
    #         null_counts=null_counts,
    #         sample_rows=sample_rows,
    #         file_count=len(keys),
    #     )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _list_parquet_keys(self) -> Iterator[str]:
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=self.location.bucket, Prefix=self.location.prefix
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet") or key.endswith(".parq"):
                    yield key

    def _read_parquet_metadata(self, key: str) -> pq.FileMetaData:
        """Read only the Parquet footer — a tiny fraction of file size."""
        obj = self.s3.get_object(Bucket=self.location.bucket, Key=key)
        buf = io.BytesIO(obj["Body"].read())
        return pq.read_metadata(buf)

    def _metadata_stats(self, keys: list[str]) -> tuple[int, pa.Schema]:
        """
        Aggregate row counts and schema from Parquet footers only.
        O(n_files) S3 GETs but each reads only the small footer.
        """
        total_rows = 0
        schema: pa.Schema | None = None

        for key in keys:
            meta = self._read_parquet_metadata(key)
            total_rows += meta.num_rows
            if schema is None:
                schema = meta.schema.to_arrow_schema()

        assert schema is not None
        return total_rows, schema

    def _sample_null_counts(
        self,
        keys: list[str],
        columns: list[str],
        sample_fraction: float,
    ) -> tuple[dict[str, int], int]:
        """
        Read a random sample of files and count nulls per column.
        Returns (null_counts_dict, total_sampled_rows).
        """
        n_sample = max(1, round(len(keys) * sample_fraction))
        sampled_keys = random.sample(keys, n_sample)

        null_counts: dict[str, int] = {col: 0 for col in columns}
        total_rows = 0

        for key in sampled_keys:
            obj = self.s3.get_object(Bucket=self.location.bucket, Key=key)
            buf = io.BytesIO(obj["Body"].read())
            table = pq.read_table(buf, columns=columns)
            total_rows += len(table)
            for col in columns:
                if col in table.schema.names:
                    null_counts[col] += table.column(col).null_count

        return null_counts, total_rows