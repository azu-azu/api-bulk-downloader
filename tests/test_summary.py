"""Tests for wdi_pipeline/summary.py"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from wdi_pipeline.exceptions import PipelineError
from wdi_pipeline.summary import JobSummary, make_summary


def test_make_summary_initial_state():
    s = make_summary("my_job")
    assert s.job_id == "my_job"
    assert s.status == "pending"
    assert s.started_at is not None


def test_finish_computes_duration():
    s = make_summary("my_job")
    time.sleep(0.01)
    s.finish()
    assert s.finished_at is not None
    assert s.duration_seconds is not None
    assert s.duration_seconds >= 0.0


def test_finish_sets_optional_fields(tmp_path):
    s = make_summary("my_job")
    dest = tmp_path / "out.csv"
    s.finish(rows=42, export_path=dest, discovery_columns=["a", "b"])
    assert s.rows_exported == 42
    assert s.export_path == str(dest)
    assert s.discovery_columns == ["a", "b"]
    assert s.error is None


def test_finish_sets_error_field():
    s = make_summary("my_job")
    s.finish(error="something went wrong")
    assert s.error == "something went wrong"


def test_finish_before_started_at_raises():
    """finish() on a JobSummary with no started_at must raise PipelineError."""
    s = JobSummary(job_id="x", status="pending")  # started_at defaults to None
    with pytest.raises(PipelineError, match="finish\\(\\) called before"):
        s.finish()


def test_write_produces_valid_json(tmp_path):
    s = make_summary("my_job")
    s.status = "success"
    s.finish(rows=5)
    dest = s.write(tmp_path)
    assert dest == tmp_path / "my_job_summary.json"
    data = json.loads(dest.read_text())
    assert data["job_id"] == "my_job"
    assert data["status"] == "success"
    assert data["rows_exported"] == 5
    assert data["duration_seconds"] is not None


def test_write_creates_output_dir(tmp_path):
    s = make_summary("my_job")
    s.finish()
    nested = tmp_path / "a" / "b" / "c"
    dest = s.write(nested)
    assert dest.exists()
