"""Tests for src/manifest.py"""
from __future__ import annotations

import pytest

from wdi_pipeline.manifest import load_manifest
from wdi_pipeline.exceptions import ManifestValidationError


def _wb_job(job_id: str, min_year: str = "2000") -> str:
    return (
        f"  - job_id: {job_id}\n"
        f"    connector_params:\n"
        f"      indicator_code: NY.GDP.MKTP.CD\n"
        f"      country_code: JPN\n"
        f"    sql:\n"
        f"      file: queries/timeseries.sql\n"
        f"      params:\n"
        f"        min_year: \"{min_year}\"\n"
        f"    export:\n"
        f"      filename: {job_id}\n"
        f"    schema:\n"
        f"      file: schemas/timeseries.yaml\n"
    )


def test_load_valid_manifest(tmp_manifest):
    path = tmp_manifest(_wb_job("gdp_jpn"))
    cfg = load_manifest(path, base_dir=path.parent)
    assert len(cfg.jobs) == 1
    assert cfg.jobs[0].job_id == "gdp_jpn"
    assert cfg.jobs[0].connector_params["indicator_code"] == "NY.GDP.MKTP.CD"
    assert cfg.jobs[0].connector_params["country_code"] == "JPN"
    assert cfg.jobs[0].sql.params["min_year"] == "2000"
    assert cfg.jobs[0].export.format == "csv"  # default


def test_duplicate_id_raises(tmp_manifest):
    yaml_jobs = _wb_job("gdp_jpn") + _wb_job("gdp_jpn")
    path = tmp_manifest(yaml_jobs)
    with pytest.raises(ManifestValidationError, match="Duplicate job id"):
        load_manifest(path, base_dir=path.parent)



def test_missing_sql_file_raises(tmp_path):
    (tmp_path / "manifest.yaml").write_text(
        "defaults:\n"
        "  output_root: outputs/\n"
        "jobs:\n"
        "  - job_id: gdp_jpn\n"
        "    connector_params:\n"
        "      indicator_code: NY.GDP.MKTP.CD\n"
        "      country_code: JPN\n"
        "    sql:\n"
        "      file: queries/does_not_exist.sql\n"
        "      params: {}\n"
        "    export:\n"
        "      filename: gdp_jpn\n"
    )
    with pytest.raises(ManifestValidationError, match="SQL file not found"):
        load_manifest(tmp_path / "manifest.yaml", base_dir=tmp_path)


def test_schema_file_not_found_raises(tmp_path):
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "timeseries.sql").write_text("SELECT 1")
    (tmp_path / "manifest.yaml").write_text(
        "defaults:\n"
        "  output_root: outputs/\n"
        "jobs:\n"
        "  - job_id: test_job\n"
        "    connector_params: {}\n"
        "    sql:\n"
        "      file: queries/timeseries.sql\n"
        "      params: {}\n"
        "    export:\n"
        "      filename: test_job\n"
        "    schema:\n"
        "      file: schemas/does_not_exist.yaml\n"
    )
    with pytest.raises(ManifestValidationError, match="schema file not found"):
        load_manifest(tmp_path / "manifest.yaml", base_dir=tmp_path)


def test_unknown_export_format_raises(tmp_manifest):
    yaml_jobs = (
        "  - job_id: bad_fmt\n"
        "    connector_params:\n"
        "      indicator_code: NY.GDP.MKTP.CD\n"
        "      country_code: JPN\n"
        "    sql:\n"
        "      file: queries/timeseries.sql\n"
        "      params:\n"
        "        min_year: \"2000\"\n"
        "    export:\n"
        "      filename: bad_fmt\n"
        "      format: xlsx\n"
        "    schema:\n"
        "      file: schemas/timeseries.yaml\n"
    )
    path = tmp_manifest(yaml_jobs)
    with pytest.raises(ManifestValidationError, match="unknown export format"):
        load_manifest(path, base_dir=path.parent)


def test_malformed_manifest_yaml_raises(tmp_path):
    (tmp_path / "manifest.yaml").write_text("key: [unclosed")
    with pytest.raises(ManifestValidationError, match="YAML parse error"):
        load_manifest(tmp_path / "manifest.yaml", base_dir=tmp_path)


def test_enabled_false_excluded(tmp_manifest):
    yaml_jobs = (
        _wb_job("job_a")
        + "  - job_id: job_b\n"
          "    enabled: false\n"
          "    connector_params:\n"
          "      indicator_code: SP.POP.TOTL\n"
          "      country_code: WLD\n"
          "    sql:\n"
          "      file: queries/timeseries.sql\n"
          "      params:\n"
          "        min_year: \"2000\"\n"
          "    export:\n"
          "      filename: job_b\n"
          "    schema:\n"
          "      file: schemas/timeseries.yaml\n"
    )
    path = tmp_manifest(yaml_jobs)
    cfg = load_manifest(path, base_dir=path.parent)
    assert len(cfg.jobs) == 2
    enabled = cfg.enabled_jobs()
    assert len(enabled) == 1
    assert enabled[0].job_id == "job_a"
