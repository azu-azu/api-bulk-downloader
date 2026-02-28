"""Tests for src/manifest.py"""
import pytest
from pathlib import Path

from wdi_pipeline.manifest import load_manifest
from wdi_pipeline.exceptions import ManifestValidationError


def _wb_job(name: str, min_year: str = "2000") -> str:
    return (
        f"  - name: {name}\n"
        f"    source:\n"
        f"      type: worldbank_indicator\n"
        f"      params:\n"
        f"        indicator_code: NY.GDP.MKTP.CD\n"
        f"        country_code: JPN\n"
        f"    sql:\n"
        f"      file: queries/timeseries.sql\n"
        f"      params:\n"
        f"        min_year: \"{min_year}\"\n"
        f"    export:\n"
        f"      filename: {name}\n"
        f"    schema:\n"
        f"      file: schemas/timeseries.yaml\n"
    )


def test_load_valid_manifest(tmp_manifest):
    path = tmp_manifest(_wb_job("gdp_jpn"))
    cfg = load_manifest(path, base_dir=path.parent)
    assert len(cfg.jobs) == 1
    assert cfg.jobs[0].name == "gdp_jpn"
    assert cfg.jobs[0].source.type == "worldbank_indicator"
    assert cfg.jobs[0].source.params["indicator_code"] == "NY.GDP.MKTP.CD"
    assert cfg.jobs[0].sql.params["min_year"] == "2000"
    assert cfg.jobs[0].export.format == "csv"  # default


def test_duplicate_name_raises(tmp_manifest):
    yaml_jobs = _wb_job("gdp_jpn") + _wb_job("gdp_jpn")
    path = tmp_manifest(yaml_jobs)
    with pytest.raises(ManifestValidationError, match="Duplicate job name"):
        load_manifest(path, base_dir=path.parent)


def test_unknown_type_raises(tmp_manifest, tmp_path):
    # Need to create the SQL file for this job's reference
    (tmp_path / "queries").mkdir(parents=True, exist_ok=True)
    (tmp_path / "queries" / "timeseries.sql").write_text("SELECT 1")
    yaml_jobs = (
        "  - name: bad_job\n"
        "    source:\n"
        "      type: unknown_connector\n"
        "      params: {}\n"
        "    sql:\n"
        "      file: queries/timeseries.sql\n"
        "      params: {}\n"
        "    export:\n"
        "      filename: bad\n"
    )
    path = tmp_manifest(yaml_jobs)
    with pytest.raises(ManifestValidationError, match="unknown source type"):
        load_manifest(path, base_dir=path.parent)


def test_missing_sql_file_raises(tmp_path):
    (tmp_path / "manifest.yaml").write_text(
        "defaults:\n"
        "  output_root: outputs/\n"
        "jobs:\n"
        "  - name: gdp_jpn\n"
        "    source:\n"
        "      type: worldbank_indicator\n"
        "      params:\n"
        "        indicator_code: NY.GDP.MKTP.CD\n"
        "        country_code: JPN\n"
        "    sql:\n"
        "      file: queries/does_not_exist.sql\n"
        "      params: {}\n"
        "    export:\n"
        "      filename: gdp_jpn\n"
    )
    with pytest.raises(ManifestValidationError, match="SQL file not found"):
        load_manifest(tmp_path / "manifest.yaml", base_dir=tmp_path)


def test_enabled_false_excluded(tmp_manifest):
    yaml_jobs = (
        _wb_job("job_a")
        + "  - name: job_b\n"
          "    enabled: false\n"
          "    source:\n"
          "      type: worldbank_indicator\n"
          "      params:\n"
          "        indicator_code: SP.POP.TOTL\n"
          "        country_code: WLD\n"
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
    assert enabled[0].name == "job_a"
