"""Tests for run-all subcommand in cli.py"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from wdi_pipeline.cli import main


def _write_pipeline(base: Path, name: str) -> None:
    """Create a minimal pipeline directory: base/name/"""
    d = base / name
    (d / "queries").mkdir(parents=True)
    (d / "schemas").mkdir()
    (d / "queries" / "query.sql").write_text("SELECT * FROM dataset")
    (d / "schemas" / "schema.yaml").write_text(
        "columns:\n  - {name: id, type: VARCHAR}\n"
    )
    (d / "manifest.yaml").write_text(
        "defaults:\n"
        "  output_root: outputs/\n"
        "jobs:\n"
        f"  - job_id: {name}_job\n"
        "    connector_params:\n"
        "      indicator_code: NY.GDP.MKTP.CD\n"
        "      country_code: JPN\n"
        "    sql:\n"
        "      file: queries/query.sql\n"
        "      params:\n"
        '        min_year: "2000"\n'
        "    export:\n"
        f"      filename: {name}\n"
        "    schema:\n"
        "      file: schemas/schema.yaml\n"
    )


def test_run_all_dry_run(tmp_path):
    _write_pipeline(tmp_path, "pipeline_a")
    _write_pipeline(tmp_path, "pipeline_b")
    result = main([
        "run-all",
        "--pipeline-dir", str(tmp_path),
        "--dry-run",
        "--output-root", str(tmp_path / "outputs"),
    ])
    assert result == 0


def test_run_all_output_root_override(tmp_path):
    """--output-root を指定すると direct replacement（フラット）で出力される"""
    _write_pipeline(tmp_path, "pipeline_a")
    result = main([
        "run-all",
        "--pipeline-dir", str(tmp_path),
        "--dry-run",
        "--output-root", str(tmp_path / "tmp_out"),
    ])
    assert result == 0


def test_run_all_no_pipeline_dir_error(tmp_path):
    env = {k: v for k, v in os.environ.items() if k != "WDI_PIPELINE_DIR"}
    with patch.dict(os.environ, env, clear=True):
        result = main(["run-all"])
    assert result == 1


def test_run_all_empty_dir_error(tmp_path):
    result = main(["run-all", "--pipeline-dir", str(tmp_path)])
    assert result == 1


def test_run_all_env_var(tmp_path, monkeypatch):
    _write_pipeline(tmp_path, "pipeline_a")
    monkeypatch.setenv("WDI_PIPELINE_DIR", str(tmp_path))
    result = main(["run-all", "--dry-run", "--output-root", str(tmp_path / "out")])
    assert result == 0


def _write_pipeline_with_filename(base: Path, dir_name: str, filename: str) -> None:
    """Create a pipeline where export.filename is explicitly set (to test collisions)."""
    d = base / dir_name
    (d / "queries").mkdir(parents=True)
    (d / "schemas").mkdir()
    (d / "queries" / "query.sql").write_text("SELECT * FROM dataset")
    (d / "schemas" / "schema.yaml").write_text(
        "columns:\n  - {name: id, type: VARCHAR}\n"
    )
    (d / "manifest.yaml").write_text(
        "defaults:\n"
        "  output_root: outputs/\n"
        "jobs:\n"
        f"  - job_id: {dir_name}_job\n"
        "    connector_params:\n"
        "      indicator_code: NY.GDP.MKTP.CD\n"
        "      country_code: JPN\n"
        "    sql:\n"
        "      file: queries/query.sql\n"
        "      params:\n"
        '        min_year: "2000"\n'
        "    export:\n"
        f"      filename: {filename}\n"  # ← same filename → collision
        "    schema:\n"
        "      file: schemas/schema.yaml\n"
    )


def test_run_all_collision_detected(tmp_path):
    """Two pipelines with same export.filename → preflight error."""
    _write_pipeline_with_filename(tmp_path, "pipeline_x", "result")
    _write_pipeline_with_filename(tmp_path, "pipeline_y", "result")
    result = main([
        "run-all",
        "--pipeline-dir", str(tmp_path),
        "--dry-run",
        "--output-root", str(tmp_path / "out"),
    ])
    assert result == 1


def test_run_all_allow_overwrite_skips_check(tmp_path):
    """--allow-overwrite suppresses collision check."""
    _write_pipeline_with_filename(tmp_path, "pipeline_x", "result")
    _write_pipeline_with_filename(tmp_path, "pipeline_y", "result")
    result = main([
        "run-all",
        "--pipeline-dir", str(tmp_path),
        "--dry-run",
        "--output-root", str(tmp_path / "out"),
        "--allow-overwrite",
    ])
    assert result == 0
