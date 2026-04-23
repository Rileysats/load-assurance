"""Config parsing and validation using Pydantic v2."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Check configs
# ---------------------------------------------------------------------------

class RowCountCheckConfig(BaseModel):
    enabled: bool = True
    tolerance_pct: float = Field(default=0.0, ge=0.0, le=100.0)
    """Acceptable % difference between source and target row counts."""


class SchemaCheckConfig(BaseModel):
    enabled: bool = True
    ignore_nullable: bool = False
    """If True, nullable differences between source/target are not a failure."""


class NullRateCheckConfig(BaseModel):
    enabled: bool = False
    columns: list[str] = Field(default_factory=list)
    """Columns to check. Empty = check all columns."""
    max_null_pct: float = Field(default=0.01, ge=0.0, le=1.0)


class ChecksConfig(BaseModel):
    row_count: RowCountCheckConfig = Field(default_factory=RowCountCheckConfig)
    schema_check: SchemaCheckConfig = Field(default_factory=SchemaCheckConfig)
    null_rate: NullRateCheckConfig = Field(default_factory=NullRateCheckConfig)


# ---------------------------------------------------------------------------
# Adapter configs
# ---------------------------------------------------------------------------

class S3AdapterConfig(BaseModel):
    type: Literal["s3"]
    path: str
    """s3://bucket/prefix/ or s3://bucket/key.parquet"""
    format: Literal["parquet"] = "parquet"
    region: str | None = None
    profile: str | None = None
    """AWS profile name (optional, falls back to env/instance creds)."""

    @model_validator(mode="after")
    def validate_path(self) -> "S3AdapterConfig":
        if not self.path.startswith("s3://"):
            raise ValueError(f"S3 path must start with s3://, got: {self.path}")
        return self


AdapterConfig = S3AdapterConfig  # union point — add SnowflakeAdapterConfig etc. later


# ---------------------------------------------------------------------------
# Top-level pipeline config
# ---------------------------------------------------------------------------

class PipelineConfig(BaseModel):
    pipeline: str
    source: AdapterConfig
    target: AdapterConfig
    checks: ChecksConfig = Field(default_factory=ChecksConfig)

    @model_validator(mode="before")
    @classmethod
    def route_adapter_type(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Inject the correct adapter model based on `type` field."""
        for key in ("source", "target"):
            adapter = values.get(key)
            if isinstance(adapter, dict):
                adapter_type = adapter.get("type")
                if adapter_type == "s3":
                    values[key] = S3AdapterConfig(**adapter)
                else:
                    raise ValueError(f"Unsupported adapter type: {adapter_type!r}")
        return values


def load_config(path: str | Path) -> PipelineConfig:
    """Load and validate a YAML pipeline config."""
    raw = yaml.safe_load(Path(path).read_text())
    return PipelineConfig(**raw)