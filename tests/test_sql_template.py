"""Tests for src/sql_template.py"""
import pytest

from wdi_pipeline.sql_template import render
from wdi_pipeline.exceptions import SQLTemplateError


def test_single_placeholder_integer():
    sql = render("SELECT * FROM t WHERE year >= {{min_year}}", {"min_year": "2000"})
    assert sql == "SELECT * FROM t WHERE year >= 2000"


def test_single_placeholder_string():
    sql = render("SELECT * FROM t WHERE name = {{label}}", {"label": "Japan"})
    assert sql == "SELECT * FROM t WHERE name = 'Japan'"


def test_multiple_placeholders():
    sql = render(
        "SELECT * FROM t WHERE year >= {{start}} AND year <= {{end}}",
        {"start": "2000", "end": "2020"},
    )
    assert sql == "SELECT * FROM t WHERE year >= 2000 AND year <= 2020"


def test_no_placeholders():
    sql = render("SELECT 1", {})
    assert sql == "SELECT 1"


def test_undefined_key_raises():
    with pytest.raises(SQLTemplateError, match="undefined parameter"):
        render("SELECT * FROM t WHERE x = {{missing}}", {})


def test_duplicate_placeholder_both_substituted():
    """Same key appearing twice — both occurrences get the literal value."""
    sql = render(
        "SELECT * FROM t WHERE a = {{x}} OR b = {{x}}",
        {"x": "42"},
    )
    assert sql == "SELECT * FROM t WHERE a = 42 OR b = 42"


def test_extra_params_ignored():
    """Extra params not referenced in SQL are silently ignored."""
    sql = render("SELECT 1", {"unused": "value"})
    assert sql == "SELECT 1"


def test_string_with_apostrophe_escaped():
    """Single quotes inside string params must be SQL-escaped."""
    sql = render("SELECT * FROM t WHERE name = {{nm}}", {"nm": "O'Brien"})
    assert sql == "SELECT * FROM t WHERE name = 'O''Brien'"


def test_float_value():
    sql = render("SELECT * FROM t WHERE threshold = {{t}}", {"t": "3.14"})
    assert sql == "SELECT * FROM t WHERE threshold = 3.14"
