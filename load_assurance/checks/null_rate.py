"""Null rate check — uses sampled data, not full scan."""

from __future__ import annotations

from load_assurance.adapters.s3 import AdapterStats
from load_assurance.checks.result import CheckResult, Severity
from load_assurance.config import NullRateCheckConfig


def check_null_rate(
    target: AdapterStats,
    config: NullRateCheckConfig,
) -> CheckResult:
    """
    Check null rates on target data using sampled rows.
    Only runs on the target — source nulls are expected to be validated
    upstream by the producing pipeline.
    """
    if target.sample_rows == 0:
        return CheckResult(
            check_name="null_rate",
            severity=Severity.WARNING,
            message="No rows were sampled — null rate check skipped.",
            details={},
        )

    columns = config.columns or list(target.null_counts.keys())
    failures: list[str] = []
    col_details: dict[str, dict] = {}

    for col in columns:
        null_count = target.null_counts.get(col, 0)
        null_pct = null_count / target.sample_rows
        col_details[col] = {
            "null_count": null_count,
            "sample_rows": target.sample_rows,
            "null_pct": round(null_pct, 6),
            "max_null_pct": config.max_null_pct,
        }
        if null_pct > config.max_null_pct:
            failures.append(
                f"Column '{col}': null_pct={null_pct:.4%} exceeds max={config.max_null_pct:.4%}"
            )

    details = {"sampled_rows": target.sample_rows, "columns": col_details}

    if failures:
        return CheckResult(
            check_name="null_rate",
            severity=Severity.FAILURE,
            message=f"Null rate check failed on {len(failures)} column(s).",
            details=details,
        )

    return CheckResult(
        check_name="null_rate",
        severity=Severity.OK,
        message=f"Null rates within threshold across {len(columns)} column(s).",
        details=details,
    )