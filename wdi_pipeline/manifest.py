from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from wdi_pipeline.exceptions import ManifestValidationError

logger = logging.getLogger(__name__)

_KNOWN_TYPES = {"worldbank_indicator"}
_KNOWN_FORMATS = {"csv", "parquet"}


@dataclass
class ColumnDef:
    name: str
    type: str  # DuckDB type string (VARCHAR, INTEGER, DOUBLE, ...)


@dataclass
class SchemaConfig:
    columns: list[ColumnDef]


@dataclass
class ExportConfig:
    filename: str
    format: str = "csv"


@dataclass
class SqlConfig:
    file: Path
    params: dict[str, str] = field(default_factory=dict)


@dataclass
class SourceConfig:
    type: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class JobConfig:
    name: str
    source: SourceConfig
    sql: SqlConfig
    export: ExportConfig
    schema: SchemaConfig
    enabled: bool = True


@dataclass
class ManifestConfig:
    output_root: Path
    jobs: list[JobConfig]

    def enabled_jobs(self) -> list[JobConfig]:
        return [j for j in self.jobs if j.enabled]


def load_manifest(manifest_path: str | Path, base_dir: Path | None = None) -> ManifestConfig:
    """Parse and validate manifest.yaml.

    Args:
        manifest_path: Path to the manifest YAML file.
        base_dir: Base directory for resolving relative SQL file paths.
                  Defaults to the manifest file's parent directory.
    """
    path = Path(manifest_path)
    if not path.exists():
        raise ManifestValidationError(f"Manifest file not found: {path}")

    with path.open() as fh:
        raw = yaml.safe_load(fh)

    if not isinstance(raw, dict):
        raise ManifestValidationError("Manifest must be a YAML mapping.")

    base_dir = base_dir or path.parent
    defaults = raw.get("defaults", {})
    output_root = Path(defaults.get("output_root", "outputs/"))
    default_format = defaults.get("export_format", "csv").lower()

    raw_jobs = raw.get("jobs", [])
    if not isinstance(raw_jobs, list):
        raise ManifestValidationError("'jobs' must be a list.")

    jobs: list[JobConfig] = []
    seen_names: set[str] = set()

    for i, raw_job in enumerate(raw_jobs):
        job = _parse_job(raw_job, i, base_dir, default_format)
        if job.name in seen_names:
            raise ManifestValidationError(
                f"Duplicate job name: '{job.name}'"
            )
        seen_names.add(job.name)
        jobs.append(job)

    return ManifestConfig(
        output_root=output_root,
        jobs=jobs,
    )


def _parse_job(
    raw: Any, idx: int, base_dir: Path, default_format: str
) -> JobConfig:
    if not isinstance(raw, dict):
        raise ManifestValidationError(f"Job at index {idx} must be a mapping.")

    name = raw.get("name")
    if not name:
        raise ManifestValidationError(f"Job at index {idx} is missing 'name'.")

    enabled = raw.get("enabled", True)

    # source
    raw_source = raw.get("source")
    if not isinstance(raw_source, dict):
        raise ManifestValidationError(f"Job '{name}': 'source' must be a mapping.")
    src_type = raw_source.get("type")
    if src_type not in _KNOWN_TYPES:
        raise ManifestValidationError(
            f"Job '{name}': unknown source type '{src_type}'. "
            f"Known types: {sorted(_KNOWN_TYPES)}"
        )
    source = SourceConfig(type=src_type, params=raw_source.get("params") or {})

    # sql
    raw_sql = raw.get("sql")
    if not isinstance(raw_sql, dict):
        raise ManifestValidationError(f"Job '{name}': 'sql' must be a mapping.")
    sql_file_rel = raw_sql.get("file")
    if not sql_file_rel:
        raise ManifestValidationError(f"Job '{name}': 'sql.file' is required.")
    sql_file = (base_dir / sql_file_rel).resolve()
    if not sql_file.exists():
        raise ManifestValidationError(
            f"Job '{name}': SQL file not found: {sql_file}"
        )
    sql_params = raw_sql.get("params") or {}
    sql_cfg = SqlConfig(file=sql_file, params={k: str(v) for k, v in sql_params.items()})

    # export
    raw_export = raw.get("export") or {}
    fmt = raw_export.get("format", default_format).lower()
    if fmt not in _KNOWN_FORMATS:
        raise ManifestValidationError(
            f"Job '{name}': unknown export format '{fmt}'. "
            f"Known formats: {sorted(_KNOWN_FORMATS)}"
        )
    filename = raw_export.get("filename") or name
    export_cfg = ExportConfig(filename=filename, format=fmt)

    # schema
    raw_schema = raw.get("schema")
    if not isinstance(raw_schema, dict):
        raise ManifestValidationError(f"Job '{name}': 'schema' must be a mapping.")
    schema_file_rel = raw_schema.get("file")
    if not schema_file_rel:
        raise ManifestValidationError(f"Job '{name}': 'schema.file' is required.")
    schema_file = (base_dir / schema_file_rel).resolve()
    if not schema_file.exists():
        raise ManifestValidationError(
            f"Job '{name}': schema file not found: {schema_file}"
        )
    with schema_file.open() as fh:
        raw_schema_data = yaml.safe_load(fh)
    if not isinstance(raw_schema_data, dict) or "columns" not in raw_schema_data:
        raise ManifestValidationError(
            f"Job '{name}': schema file must contain a 'columns' list."
        )
    columns: list[ColumnDef] = []
    for col_entry in raw_schema_data["columns"]:
        if not isinstance(col_entry, dict) or "name" not in col_entry or "type" not in col_entry:
            raise ManifestValidationError(
                f"Job '{name}': each schema column must have 'name' and 'type'."
            )
        columns.append(ColumnDef(name=col_entry["name"], type=col_entry["type"]))
    schema_cfg = SchemaConfig(columns=columns)

    return JobConfig(
        name=name,
        source=source,
        sql=sql_cfg,
        export=export_cfg,
        schema=schema_cfg,
        enabled=enabled,
    )
