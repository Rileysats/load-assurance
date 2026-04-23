"""Load assurance engine.

Wires together:
  1. Adapter resolution (source + target)
  2. Stat collection (metadata-first, sample for null rates)
  3. Check execution (only enabled checks run)
  4. Result aggregation
"""

from __future__ import annotations

from dataclasses import dataclass

from load_assurance.adapters.s3 import S3ParquetAdapter
from load_assurance.adapters.local import LocalParquetAdapter
from load_assurance.adapters.base import AdapterStats, AbstractAdapter
from load_assurance.checks.null_rate import check_null_rate
from load_assurance.checks.result import CheckResult, Severity
from load_assurance.checks.row_count import check_row_count
from load_assurance.checks.schema import check_schema
from load_assurance.config import AdapterConfig, PipelineConfig, S3AdapterConfig, LocalAdapterConfig


@dataclass
class RunReport:
    pipeline: str
    results: list[CheckResult]

    @property
    def passed(self) -> bool:
        return all(r.severity != Severity.FAILURE for r in self.results)

    @property
    def failures(self) -> list[CheckResult]:
        return [r for r in self.results if r.severity == Severity.FAILURE]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if r.severity == Severity.WARNING]


def _build_adapter(config: AdapterConfig) -> AbstractAdapter:
    if isinstance(config, S3AdapterConfig):
        return S3ParquetAdapter(
            path=config.path,
            region=config.region,
            profile=config.profile,
        )
    elif isinstance(config, LocalAdapterConfig):
        return LocalParquetAdapter(
            path=config.path
        )
    raise NotImplementedError(f"No adapter implemented for: {type(config)}")


def run(config: PipelineConfig) -> RunReport:
    """Execute all enabled checks and return a RunReport."""
    checks_cfg = config.checks

    # Determine which columns need null sampling (avoids unnecessary S3 reads)
    null_cols: list[str] | None = None
    if checks_cfg.null_rate.enabled:
        null_cols = checks_cfg.null_rate.columns or None  # None = sample all cols

    src_adapter = _build_adapter(config.source)
    tgt_adapter = _build_adapter(config.target)

    # Collect stats — metadata only unless null_rate is enabled
    source_stats: AdapterStats = src_adapter.get_stats()
    target_stats: AdapterStats = tgt_adapter.get_stats(null_rate_columns=null_cols)

    results: list[CheckResult] = []

    if checks_cfg.row_count.enabled:
        results.append(check_row_count(source_stats, target_stats, checks_cfg.row_count))

    if checks_cfg.schema_check.enabled:
        results.append(check_schema(source_stats, target_stats, checks_cfg.schema_check))

    if checks_cfg.null_rate.enabled:
        results.append(check_null_rate(target_stats, checks_cfg.null_rate))

    return RunReport(pipeline=config.pipeline, results=results)