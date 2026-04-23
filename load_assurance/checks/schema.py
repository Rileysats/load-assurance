"""Schema check — compares source vs target Parquet schemas."""

from __future__ import annotations

import pyarrow as pa

from load_assurance.adapters.s3 import AdapterStats
from load_assurance.checks.result import CheckResult, Severity
from load_assurance.config import SchemaCheckConfig


def check_schema(
    source: AdapterStats,
    target: AdapterStats,
    config: SchemaCheckConfig,
) -> CheckResult:
    src_schema: pa.Schema = source.schema
    tgt_schema: pa.Schema = target.schema

    issues: list[str] = []

    src_fields = {f.name: f for f in src_schema}
    tgt_fields = {f.name: f for f in tgt_schema}

    # Columns missing from target
    missing_in_target = set(src_fields) - set(tgt_fields)
    for col in sorted(missing_in_target):
        issues.append(f"Column '{col}' present in source but missing in target.")

    # Extra columns in target (warning, not necessarily a failure)
    extra_in_target = set(tgt_fields) - set(src_fields)
    extra_warnings = [
        f"Column '{col}' present in target but not in source." for col in sorted(extra_in_target)
    ]

    # Type mismatches on shared columns
    for col in set(src_fields) & set(tgt_fields):
        src_type = src_fields[col].type
        tgt_type = tgt_fields[col].type
        if src_type != tgt_type:
            issues.append(
                f"Column '{col}': type mismatch — source={src_type}, target={tgt_type}"
            )

        if not config.ignore_nullable:
            src_nullable = src_fields[col].nullable
            tgt_nullable = tgt_fields[col].nullable
            if src_nullable != tgt_nullable:
                issues.append(
                    f"Column '{col}': nullable mismatch — "
                    f"source={src_nullable}, target={tgt_nullable}"
                )

    details = {
        "source_columns": len(src_fields),
        "target_columns": len(tgt_fields),
        "missing_in_target": sorted(missing_in_target),
        "extra_in_target": sorted(extra_in_target),
        "issues": issues,
    }

    if issues:
        return CheckResult(
            check_name="schema",
            severity=Severity.FAILURE,
            message=f"Schema check failed with {len(issues)} issue(s).",
            details=details,
        )

    if extra_warnings:
        return CheckResult(
            check_name="schema",
            severity=Severity.WARNING,
            message=f"Schema OK but target has {len(extra_in_target)} extra column(s).",
            details=details,
        )

    return CheckResult(
        check_name="schema",
        severity=Severity.OK,
        message="Schemas match.",
        details=details,
    )