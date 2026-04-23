"""Row count check — compares source vs target using metadata only."""

from __future__ import annotations

from load_assurance.adapters.base import AdapterStats
from load_assurance.checks.result import CheckResult, Severity
from load_assurance.config import RowCountCheckConfig


def check_row_count(
    source: AdapterStats,
    target: AdapterStats,
    config: RowCountCheckConfig,
) -> CheckResult:
    src, tgt = source.row_count, target.row_count

    if src == 0 and tgt == 0:
        return CheckResult(
            check_name="row_count",
            severity=Severity.OK,
            message="Both source and target are empty.",
            details={"source_rows": 0, "target_rows": 0},
        )

    if src == 0:
        return CheckResult(
            check_name="row_count",
            severity=Severity.FAILURE,
            message="Source has 0 rows — cannot validate target.",
            details={"source_rows": 0, "target_rows": tgt},
        )

    diff_pct = abs(tgt - src) / src * 100

    details = {
        "source_rows": src,
        "target_rows": tgt,
        "diff_rows": tgt - src,
        "diff_pct": round(diff_pct, 4),
        "tolerance_pct": config.tolerance_pct,
    }

    if diff_pct <= config.tolerance_pct:
        return CheckResult(
            check_name="row_count",
            severity=Severity.OK,
            message=f"Row counts match within tolerance ({diff_pct:.4f}% diff).",
            details=details,
        )

    return CheckResult(
        check_name="row_count",
        severity=Severity.FAILURE,
        message=(
            f"Row count mismatch: source={src:,}, target={tgt:,} "
            f"({diff_pct:.4f}% diff, tolerance={config.tolerance_pct}%)"
        ),
        details=details,
    )