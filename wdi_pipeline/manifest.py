"""a validated parser
manifest.yaml（設定ファイル）を読み込んで、
型付きの ManifestConfig / JobConfig に変換しつつ、
エラーを早期に出す"バリデーション付きパーサ

This is a validated parser that loads manifest.yaml,
converts it into typed dataclasses,
and fails fast with clear errors.

全体像
YAML（文字列の設定）を読む
期待する形（dict/list/必須キー）か検証する
SQLファイルとschemaファイルのパスを解決して存在確認する
JobConfig という "実行可能な構造体" に変換する
enabled: true のジョブだけ取り出せるようにもする
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from wdi_pipeline.exceptions import ManifestValidationError

_KNOWN_FORMATS = {"csv", "parquet"}

# YAMLを "ただのdict" で持ち回すとキー間違いのリスクがあるため、型付きの箱（dataclass） に詰め替える。
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
class JobConfig:
    job_id: str
    connector_params: dict[str, Any]
    sql: SqlConfig
    export: ExportConfig
    schema: SchemaConfig
    enabled: bool = True


@dataclass
class ManifestConfig:
    output_root: Path
    jobs: list[JobConfig]
    default_format: str = "csv"

    def enabled_jobs(self) -> list[JobConfig]:
        return [j for j in self.jobs if j.enabled]


def load_manifest(manifest_path: str | Path, base_dir: Path | None = None) -> ManifestConfig:
    """Parse and validate manifest.yaml.

    Args:
        manifest_path: Path to the manifest YAML file.
        base_dir: Base directory for resolving relative SQL file paths.
                Defaults to the manifest file's parent directory.
    """
    # パス検証
    path = Path(manifest_path)
    if not path.exists():
        raise ManifestValidationError(f"Manifest file not found: {path}")

    # YAML読み込み
    with path.open() as fh:
        try:
            raw = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise ManifestValidationError(f"Manifest YAML parse error: {exc}") from exc

    # "manifestはdictであるべき" を検証
    if not isinstance(raw, dict):
        raise ManifestValidationError("Manifest must be a YAML mapping.")

    # base_dir の決定/SQLやschemaの相対パスを manifestの場所基準で解決するため
    base_dir = base_dir or path.parent

    # defaults 読み込み
    defaults = raw.get("defaults", {})
    # output_root is relative to the CWD at invocation time, not to this manifest file.
    # Override with --output-root on the CLI to specify an absolute path.
    output_root = Path(defaults.get("output_root", "outputs/"))
    default_format = defaults.get("export_format", "csv").lower()

    # jobs の検証 + ループ/jobs は必ず list
    raw_jobs = raw.get("jobs", [])
    if not isinstance(raw_jobs, list):
        raise ManifestValidationError("'jobs' must be a list.")

    jobs: list[JobConfig] = []
    seen_ids: set[str] = set()

    for i, raw_job in enumerate(raw_jobs):
        job = _parse_job(raw_job, i, base_dir, default_format) # 1個ずつパース

        # job_id の重複を禁止
        if job.job_id in seen_ids:
            raise ManifestValidationError(
                f"Duplicate job id: '{job.job_id}'"
            )
        seen_ids.add(job.job_id)
        jobs.append(job)

    return ManifestConfig(
        output_root=output_root,
        jobs=jobs,
        default_format=default_format,
    )


def _parse_job(
    raw: Any, idx: int, base_dir: Path, default_format: str
) -> JobConfig:

    # job が dict か？
    if not isinstance(raw, dict):
        raise ManifestValidationError(f"Job at index {idx} must be a mapping.")

    # job_id 必須
    job_id = raw.get("job_id")
    if not job_id:
        raise ManifestValidationError(f"Job at index {idx} is missing 'job_id'.")

    # enabled（デフォルト True）
    enabled = raw.get("enabled", True)

    # connector_params（無ければ {}）
    raw_connector_params = raw.get("connector_params") or {}
    if not isinstance(raw_connector_params, dict):
        raise ManifestValidationError(f"Job '{job_id}': 'connector_params' must be a mapping.")

    # sql（必須、file 必須、存在チェック）
    raw_sql = raw.get("sql")
    if not isinstance(raw_sql, dict):
        raise ManifestValidationError(f"Job '{job_id}': 'sql' must be a mapping.")
    sql_file_rel = raw_sql.get("file")
    if not sql_file_rel:
        raise ManifestValidationError(f"Job '{job_id}': 'sql.file' is required.")
    sql_file = (base_dir / sql_file_rel).resolve()
    if not sql_file.exists():
        raise ManifestValidationError(
            f"Job '{job_id}': SQL file not found: {sql_file}"
        )
    sql_params = raw_sql.get("params") or {}
    sql_cfg = SqlConfig(file=sql_file, params={k: str(v) for k, v in sql_params.items()})

    # export（任意、format検証、filenameはjob_idにフォールバック）
    raw_export = raw.get("export") or {}
    fmt = raw_export.get("format", default_format).lower()
    if fmt not in _KNOWN_FORMATS:
        raise ManifestValidationError(
            f"Job '{job_id}': unknown export format '{fmt}'. "
            f"Known formats: {sorted(_KNOWN_FORMATS)}"
        )
    filename = raw_export.get("filename") or job_id
    export_cfg = ExportConfig(filename=filename, format=fmt)

    # schema（必須、file必須、存在チェック、内容検証）
    raw_schema = raw.get("schema")
    if not isinstance(raw_schema, dict):
        raise ManifestValidationError(f"Job '{job_id}': 'schema' must be a mapping.")
    schema_file_rel = raw_schema.get("file")
    if not schema_file_rel:
        raise ManifestValidationError(f"Job '{job_id}': 'schema.file' is required.")
    schema_file = (base_dir / schema_file_rel).resolve()
    if not schema_file.exists():
        raise ManifestValidationError(
            f"Job '{job_id}': schema file not found: {schema_file}"
        )
    with schema_file.open() as fh:
        try:
            raw_schema_data = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise ManifestValidationError(
                f"Job '{job_id}': schema YAML parse error in {schema_file}: {exc}"
            ) from exc
    if not isinstance(raw_schema_data, dict) or "columns" not in raw_schema_data:
        raise ManifestValidationError(
            f"Job '{job_id}': schema file must contain a 'columns' list."
        )
    columns: list[ColumnDef] = []
    for col_entry in raw_schema_data["columns"]:
        if not isinstance(col_entry, dict) or "name" not in col_entry or "type" not in col_entry:
            raise ManifestValidationError(
                f"Job '{job_id}': each schema column must have 'name' and 'type'."
            )
        columns.append(ColumnDef(name=col_entry["name"], type=col_entry["type"]))
    schema_cfg = SchemaConfig(columns=columns)

    return JobConfig(
        job_id=job_id,
        connector_params=raw_connector_params,
        sql=sql_cfg,
        export=export_cfg,
        schema=schema_cfg,
        enabled=enabled,
    )
