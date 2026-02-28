"""Tests for src/connectors/worldbank_indicator.py"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from tests.conftest import FakeSession
from wdi_pipeline.connectors.worldbank_indicator import WorldBankIndicatorConnector
from wdi_pipeline.exceptions import ConnectorError
from wdi_pipeline.manifest import (
    ColumnDef,
    ExportConfig,
    JobConfig,
    SchemaConfig,
    SqlConfig,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_page(page: int, pages: int, data: list[dict]) -> list:
    return [
        {"page": page, "pages": pages, "per_page": 5000, "total": pages * len(data)},
        data,
    ]


def _sample_record(country_iso: str, year: int, value: float | None) -> dict:
    return {
        "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP (current US$)"},
        "country": {"id": "JP", "value": "Japan"},
        "countryiso3code": country_iso,
        "date": str(year),
        "value": value,
        "unit": "",
        "obs_status": "",
        "decimal": 0,
    }


def _make_wb_job(tmp_path: Path) -> JobConfig:
    schema = SchemaConfig(columns=[
        ColumnDef("country_code", "VARCHAR"),
        ColumnDef("country_name", "VARCHAR"),
        ColumnDef("indicator_code", "VARCHAR"),
        ColumnDef("indicator_name", "VARCHAR"),
        ColumnDef("year", "INTEGER"),
        ColumnDef("value", "DOUBLE"),
    ])
    dummy_sql = tmp_path / "dummy.sql"
    dummy_sql.write_text("SELECT * FROM dataset")
    return JobConfig(
        name="test",
        connector_params={},
        sql=SqlConfig(file=dummy_sql, params={}),
        export=ExportConfig(filename="test", format="csv"),
        schema=schema,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_discover_returns_fixed_columns(tmp_path):
    conn_obj = WorldBankIndicatorConnector(
        indicator_code="NY.GDP.MKTP.CD",
        country_code="JPN",
        session=FakeSession([]),
    )
    result = conn_obj.discover(job=_make_wb_job(tmp_path))
    assert result.columns == [
        "country_code",
        "country_name",
        "indicator_code",
        "indicator_name",
        "year",
        "value",
    ]


def test_materialize_creates_dataset_table(tmp_path):
    data = [_sample_record("JPN", 2020, 5.0e12)]
    fake = FakeSession([_make_page(1, 1, data)])
    connector = WorldBankIndicatorConnector(
        indicator_code="NY.GDP.MKTP.CD",
        country_code="JPN",
        session=fake,
    )
    conn = duckdb.connect()
    connector.materialize(job=_make_wb_job(tmp_path), conn=conn)

    tables = conn.execute("SHOW TABLES").fetchall()
    assert any(t[0] == "dataset" for t in tables)

    rows = conn.execute("SELECT * FROM dataset").fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "JPN"
    assert rows[0][4] == 2020
    assert rows[0][5] == pytest.approx(5.0e12)
    conn.close()


def test_materialize_multi_page(tmp_path):
    data_p1 = [_sample_record("JPN", 2020, 1.0)]
    data_p2 = [_sample_record("JPN", 2021, 2.0)]
    fake = FakeSession([
        _make_page(1, 2, data_p1),
        _make_page(2, 2, data_p2),
    ])
    connector = WorldBankIndicatorConnector(
        indicator_code="NY.GDP.MKTP.CD",
        country_code="JPN",
        session=fake,
    )
    conn = duckdb.connect()
    connector.materialize(job=_make_wb_job(tmp_path), conn=conn)

    rows = conn.execute("SELECT year, value FROM dataset ORDER BY year").fetchall()
    assert rows == [(2020, 1.0), (2021, 2.0)]
    conn.close()


def test_materialize_empty_page_stops(tmp_path):
    """An empty data list on first page should produce an empty table."""
    fake = FakeSession([_make_page(1, 1, [])])
    connector = WorldBankIndicatorConnector(
        indicator_code="NY.GDP.MKTP.CD",
        country_code="JPN",
        session=fake,
    )
    conn = duckdb.connect()
    connector.materialize(job=_make_wb_job(tmp_path), conn=conn)
    count = conn.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
    assert count == 0
    conn.close()


def test_materialize_null_value_allowed(tmp_path):
    """NULL values from the API (missing data) must not crash the insert."""
    data = [_sample_record("JPN", 2005, None)]
    fake = FakeSession([_make_page(1, 1, data)])
    connector = WorldBankIndicatorConnector(
        indicator_code="NY.GDP.MKTP.CD",
        country_code="JPN",
        session=fake,
    )
    conn = duckdb.connect()
    connector.materialize(job=_make_wb_job(tmp_path), conn=conn)
    rows = conn.execute("SELECT value FROM dataset").fetchall()
    assert rows[0][0] is None
    conn.close()


def test_materialize_idempotent(tmp_path):
    """Calling materialize twice must not error (DROP TABLE IF EXISTS)."""
    data = [_sample_record("JPN", 2020, 5.0e12)]
    fake = FakeSession([
        _make_page(1, 1, data),
        _make_page(1, 1, data),
    ])
    connector = WorldBankIndicatorConnector(
        indicator_code="NY.GDP.MKTP.CD",
        country_code="JPN",
        session=fake,
    )
    job = _make_wb_job(tmp_path)
    conn = duckdb.connect()
    connector.materialize(job=job, conn=conn)
    connector.materialize(job=job, conn=conn)
    count = conn.execute("SELECT COUNT(*) FROM dataset").fetchone()[0]
    assert count == 1
    conn.close()
