"""Tests for src/runner.py"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from tests.conftest import FakeSession
from wdi_pipeline.connectors.worldbank_indicator import WorldBankIndicatorConnector
from wdi_pipeline.manifest import load_manifest
from wdi_pipeline.runner import run_pipeline
from wdi_pipeline.exceptions import PipelineError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_page(page: int, pages: int, data: list[dict]) -> list:
    return [
        {"page": page, "pages": pages, "per_page": 5000, "total": len(data)},
        data,
    ]


def _sample_data(n: int = 3) -> list[dict]:
    return [
        {
            "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP"},
            "country": {"id": "JP", "value": "Japan"},
            "countryiso3code": "JPN",
            "date": str(2020 + i),
            "value": float(i),
        }
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_dry_run_no_network_calls(tmp_manifest, tmp_path):
    """dry-run must not call discover or materialize."""
    path = tmp_manifest(
        "  - name: gdp_jpn\n"
        "    source:\n"
        "      type: worldbank_indicator\n"
        "      params:\n"
        "        indicator_code: NY.GDP.MKTP.CD\n"
        "        country_code: JPN\n"
        "    sql:\n"
        "      file: queries/worldbank/timeseries.sql\n"
        "      params:\n"
        "        min_year: \"2000\"\n"
        "    export:\n"
        "      filename: gdp_jpn\n"
        "    schema:\n"
        "      file: schemas/worldbank_timeseries.yaml\n"
    )
    cfg = load_manifest(path, base_dir=path.parent)
    cfg.output_root = tmp_path / "outputs"

    with patch.object(
        WorldBankIndicatorConnector, "materialize"
    ) as mock_mat, patch.object(
        WorldBankIndicatorConnector, "discover"
    ) as mock_disc:
        summaries = run_pipeline(cfg, dry_run=True)

    mock_disc.assert_not_called()
    mock_mat.assert_not_called()
    assert summaries[0].status == "skipped"


def test_probe_calls_discover_not_materialize(tmp_manifest, tmp_path):
    """probe mode must call discover() but not materialize()."""
    path = tmp_manifest(
        "  - name: gdp_jpn\n"
        "    source:\n"
        "      type: worldbank_indicator\n"
        "      params:\n"
        "        indicator_code: NY.GDP.MKTP.CD\n"
        "        country_code: JPN\n"
        "    sql:\n"
        "      file: queries/worldbank/timeseries.sql\n"
        "      params:\n"
        "        min_year: \"2000\"\n"
        "    export:\n"
        "      filename: gdp_jpn\n"
        "    schema:\n"
        "      file: schemas/worldbank_timeseries.yaml\n"
    )
    cfg = load_manifest(path, base_dir=path.parent)
    cfg.output_root = tmp_path / "outputs"

    with patch.object(
        WorldBankIndicatorConnector, "materialize"
    ) as mock_mat:
        summaries = run_pipeline(cfg, probe=True)

    mock_mat.assert_not_called()
    assert summaries[0].status == "probed"
    assert "country_code" in summaries[0].discovery_columns


def test_failed_job_does_not_stop_subsequent_jobs(tmp_manifest, tmp_path):
    """A failing job must not prevent later jobs from running."""
    yaml_jobs = (
        "  - name: job_fail\n"
        "    source:\n"
        "      type: worldbank_indicator\n"
        "      params:\n"
        "        indicator_code: INVALID.CODE\n"
        "        country_code: JPN\n"
        "    sql:\n"
        "      file: queries/worldbank/timeseries.sql\n"
        "      params:\n"
        "        min_year: \"2000\"\n"
        "    export:\n"
        "      filename: job_fail\n"
        "    schema:\n"
        "      file: schemas/worldbank_timeseries.yaml\n"
        "  - name: job_ok\n"
        "    source:\n"
        "      type: worldbank_indicator\n"
        "      params:\n"
        "        indicator_code: NY.GDP.MKTP.CD\n"
        "        country_code: JPN\n"
        "    sql:\n"
        "      file: queries/worldbank/timeseries.sql\n"
        "      params:\n"
        "        min_year: \"2000\"\n"
        "    export:\n"
        "      filename: job_ok\n"
        "    schema:\n"
        "      file: schemas/worldbank_timeseries.yaml\n"
    )
    path = tmp_manifest(yaml_jobs)
    cfg = load_manifest(path, base_dir=path.parent)
    cfg.output_root = tmp_path / "outputs"

    ok_data = _sample_data(2)
    ok_page = _make_page(1, 1, ok_data)

    def _materialize_side_effect(job, conn):
        if job.name == "job_fail":
            raise PipelineError("Simulated failure")
        # job_ok: inject rows directly
        conn.execute("DROP TABLE IF EXISTS dataset")
        conn.execute(
            "CREATE TABLE dataset ("
            "country_code VARCHAR, country_name VARCHAR, "
            "indicator_code VARCHAR, indicator_name VARCHAR, "
            "year INTEGER, value DOUBLE)"
        )
        conn.executemany(
            "INSERT INTO dataset VALUES (?, ?, ?, ?, ?, ?)",
            [["JPN", "Japan", "NY.GDP.MKTP.CD", "GDP", 2020 + i, float(i)]
             for i in range(2)],
        )

    with patch.object(
        WorldBankIndicatorConnector,
        "materialize",
        side_effect=_materialize_side_effect,
    ):
        summaries = run_pipeline(cfg)

    statuses = {s.job_name: s.status for s in summaries}
    assert statuses["job_fail"] == "failed"
    assert statuses["job_ok"] == "success"


def test_only_flag_filters_jobs(tmp_manifest, tmp_path):
    yaml_jobs = (
        "  - name: job_a\n"
        "    source:\n"
        "      type: worldbank_indicator\n"
        "      params:\n"
        "        indicator_code: NY.GDP.MKTP.CD\n"
        "        country_code: JPN\n"
        "    sql:\n"
        "      file: queries/worldbank/timeseries.sql\n"
        "      params:\n"
        "        min_year: \"2000\"\n"
        "    export:\n"
        "      filename: job_a\n"
        "    schema:\n"
        "      file: schemas/worldbank_timeseries.yaml\n"
        "  - name: job_b\n"
        "    source:\n"
        "      type: worldbank_indicator\n"
        "      params:\n"
        "        indicator_code: SP.POP.TOTL\n"
        "        country_code: WLD\n"
        "    sql:\n"
        "      file: queries/worldbank/timeseries.sql\n"
        "      params:\n"
        "        min_year: \"2000\"\n"
        "    export:\n"
        "      filename: job_b\n"
        "    schema:\n"
        "      file: schemas/worldbank_timeseries.yaml\n"
    )
    path = tmp_manifest(yaml_jobs)
    cfg = load_manifest(path, base_dir=path.parent)
    cfg.output_root = tmp_path / "outputs"

    summaries = run_pipeline(cfg, dry_run=True, only="job_b")
    assert len(summaries) == 1
    assert summaries[0].job_name == "job_b"
