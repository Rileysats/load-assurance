
"""Shared result types for all checks."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Severity(str, Enum):
    OK = "OK"
    WARNING = "WARNING"
    FAILURE = "FAILURE"


@dataclass
class CheckResult:
    check_name: str
    severity: Severity
    message: str
    details: dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.severity == Severity.OK