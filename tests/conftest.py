"""Shared fixtures for the test suite."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# FakeSession — injected into WorldBankIndicatorConnector in tests
# ---------------------------------------------------------------------------

class FakeSession:
    """Deterministic HTTP session that cycles through pre-defined page payloads."""

    def __init__(self, pages: list[list]) -> None:
        self._pages = pages
        self._call = 0

    def get(self, url: str, **kwargs) -> MagicMock:
        resp = MagicMock()
        resp.json.return_value = self._pages[self._call]
        resp.raise_for_status = lambda: None
        self._call += 1
        return resp


# ---------------------------------------------------------------------------
# Helpers / shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_manifest(tmp_path: Path):
    """Factory that writes a manifest.yaml into a temp dir alongside SQL files."""

    def _make(jobs_yaml: str) -> Path:
        # Create the SQL files that most jobs reference
        (tmp_path / "queries" / "worldbank").mkdir(parents=True, exist_ok=True)
        (tmp_path / "queries" / "worldbank" / "timeseries.sql").write_text(
            "SELECT * FROM dataset WHERE year >= {{min_year}}"
        )

        # Create the schema file referenced by jobs
        (tmp_path / "schemas").mkdir(exist_ok=True)
        (tmp_path / "schemas" / "worldbank_timeseries.yaml").write_text(
            "columns:\n"
            "  - {name: country_code, type: VARCHAR}\n"
            "  - {name: country_name, type: VARCHAR}\n"
            "  - {name: indicator_code, type: VARCHAR}\n"
            "  - {name: indicator_name, type: VARCHAR}\n"
            "  - {name: year, type: INTEGER}\n"
            "  - {name: value, type: DOUBLE}\n"
        )

        manifest_text = (
            "defaults:\n"
            "  output_root: outputs/\n"
            "  export_format: csv\n"
            "jobs:\n"
            + jobs_yaml
        )
        manifest_path = tmp_path / "manifest.yaml"
        manifest_path.write_text(manifest_text)
        return manifest_path

    return _make
