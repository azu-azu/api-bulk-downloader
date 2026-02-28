"""Tests for src/exporter.py"""
import duckdb
import pytest
from pathlib import Path

from src.exporter import export
from src.exceptions import ExportError


def _conn_with_dataset(rows: list[tuple]) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE dataset (country_code VARCHAR, year INTEGER, value DOUBLE)"
    )
    conn.executemany("INSERT INTO dataset VALUES (?, ?, ?)", rows)
    return conn


def test_csv_export(tmp_path: Path):
    conn = _conn_with_dataset([("JPN", 2020, 5.0e12), ("JPN", 2021, 5.1e12)])
    dest = tmp_path / "out.csv"
    count = export(conn, "SELECT * FROM dataset ORDER BY year", [], dest, "csv")
    assert count == 2
    assert dest.exists()
    lines = dest.read_text().splitlines()
    assert lines[0] == "country_code,year,value"
    assert len(lines) == 3  # header + 2 data rows
    conn.close()


def test_parquet_export(tmp_path: Path):
    conn = _conn_with_dataset([("JPN", 2020, 5.0e12)])
    dest = tmp_path / "out.parquet"
    count = export(conn, "SELECT * FROM dataset", [], dest, "parquet")
    assert count == 1
    assert dest.exists()
    conn.close()


def test_row_count_correct(tmp_path: Path):
    rows = [(f"C{i}", 2000 + i, float(i)) for i in range(10)]
    conn = _conn_with_dataset(rows)
    dest = tmp_path / "out.csv"
    count = export(conn, "SELECT * FROM dataset", [], dest, "csv")
    assert count == 10
    conn.close()


def test_unsupported_format_raises(tmp_path: Path):
    conn = _conn_with_dataset([("JPN", 2020, 1.0)])
    dest = tmp_path / "out.xlsx"
    with pytest.raises(ExportError, match="Unsupported export format"):
        export(conn, "SELECT * FROM dataset", [], dest, "xlsx")
    conn.close()


def test_export_with_rendered_literal_filter(tmp_path: Path):
    """render() embeds literals directly — exporter receives no bind params."""
    from src.sql_template import render
    conn = _conn_with_dataset([("JPN", 2020, 1.0), ("USA", 2021, 2.0)])
    dest = tmp_path / "out.csv"
    sql, values = render(
        "SELECT * FROM dataset WHERE year >= {{min_year}}",
        {"min_year": "2021"},
    )
    count = export(conn, sql, values, dest, "csv")
    assert count == 1
    conn.close()


def test_output_dir_created(tmp_path: Path):
    conn = _conn_with_dataset([("JPN", 2020, 1.0)])
    deep_dest = tmp_path / "a" / "b" / "c" / "out.csv"
    export(conn, "SELECT * FROM dataset", [], deep_dest, "csv")
    assert deep_dest.exists()
    conn.close()
