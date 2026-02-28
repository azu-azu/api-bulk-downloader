from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import requests
import urllib3
from urllib3.util.retry import Retry

from src.connectors.protocol import DiscoveryResult
from src.exceptions import ConnectorError

logger = logging.getLogger(__name__)

_COLUMNS = [
    "country_code",
    "country_name",
    "indicator_code",
    "indicator_name",
    "year",
    "value",
]
_BASE_URL = "https://api.worldbank.org/v2"


def _build_session() -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist={429, 500, 502, 503, 504},
        allowed_methods={"GET"},
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


@dataclass
class WorldBankIndicatorConnector:
    indicator_code: str
    country_code: str = "all"
    per_page: int = 5000
    session: requests.Session = field(default_factory=_build_session)

    # ------------------------------------------------------------------
    # Protocol interface
    # ------------------------------------------------------------------

    def discover(self, job: object) -> DiscoveryResult:
        """Return fixed schema — no network call required."""
        return DiscoveryResult(columns=_COLUMNS)

    def materialize(self, job: object, conn: Any) -> None:  # duckdb.DuckDBPyConnection
        """Stream-insert all pages into a DuckDB TABLE named 'dataset'."""
        conn.execute("DROP TABLE IF EXISTS dataset")
        conn.execute(
            """
            CREATE TABLE dataset (
                country_code    VARCHAR,
                country_name    VARCHAR,
                indicator_code  VARCHAR,
                indicator_name  VARCHAR,
                year            INTEGER,
                value           DOUBLE
            )
            """
        )

        page = 1
        total_rows = 0
        while True:
            meta, data = self._fetch_page(page)
            if not data:
                logger.debug("Page %d returned empty data — stopping.", page)
                break
            rows = self._normalize(data)
            conn.executemany(
                "INSERT INTO dataset VALUES (?, ?, ?, ?, ?, ?)", rows
            )
            total_rows += len(rows)
            pages = int(meta.get("pages", 1))
            logger.debug("Page %d/%d — %d rows inserted.", page, pages, len(rows))
            if page >= pages:
                break
            page += 1

        logger.info(
            "materialize complete: indicator=%s country=%s total_rows=%d",
            self.indicator_code,
            self.country_code,
            total_rows,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_page(self, page: int) -> tuple[dict, list]:
        url = (
            f"{_BASE_URL}/country/{self.country_code}"
            f"/indicator/{self.indicator_code}"
        )
        params = {"format": "json", "per_page": self.per_page, "page": page}
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ConnectorError(
                f"WorldBank API request failed (indicator={self.indicator_code}, "
                f"country={self.country_code}, page={page}): {exc}"
            ) from exc

        payload = resp.json()
        if not isinstance(payload, list) or len(payload) < 2:
            raise ConnectorError(
                f"Unexpected WorldBank API response format: {payload!r}"
            )
        meta = payload[0] or {}
        data = payload[1] or []
        return meta, data

    def _normalize(self, data: list[dict]) -> list[list]:
        rows = []
        for item in data:
            country = item.get("country") or {}
            indicator = item.get("indicator") or {}
            raw_year = item.get("date")
            year = int(raw_year) if raw_year else None
            value = item.get("value")  # None allowed (DOUBLE is nullable)
            rows.append(
                [
                    item.get("countryiso3code") or country.get("id"),
                    country.get("value"),
                    indicator.get("id"),
                    indicator.get("value"),
                    year,
                    value,
                ]
            )
        return rows
