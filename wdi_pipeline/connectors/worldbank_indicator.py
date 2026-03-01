from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import duckdb
# Web APIにGET/POSTしてデータを取るためのライブラリ
import requests

# 「HTTPリクエストが失敗したときに、何回・どんな条件で・どれくらい待って再試行するか」**を決めるためのクラス
from urllib3.util.retry import Retry

# 別ファイルで定義したものを読み込み
from wdi_pipeline.connectors.protocol import DiscoveryResult
from wdi_pipeline.exceptions import ConnectorError
from wdi_pipeline.manifest import JobConfig

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.worldbank.org/v2"


# requests.Session = 通信の状態＋設定をまとめて持つ箱
# sessionを作ると、同じ設定（リトライ等）＋同じ接続をまとめて再利用できる
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

    # 「このジョブはどんな列を持つか」を返す
    def discover(self, job: JobConfig) -> DiscoveryResult:
        """Return schema columns from job config — no network call required."""
        return DiscoveryResult(columns=[c.name for c in job.schema.columns])

    # ページング取得→DuckDBへINSERT
    def materialize(self, job: JobConfig, conn: duckdb.DuckDBPyConnection) -> None:
        """Stream-insert all pages into a DuckDB TABLE named 'dataset'."""
        cols_ddl = ", ".join(f"{c.name} {c.type}" for c in job.schema.columns)

        # テーブルを作り直す
        conn.execute("DROP TABLE IF EXISTS dataset") # dataset テーブルが 既に存在してたら削除
        conn.execute(f"CREATE TABLE dataset ({cols_ddl})") # 新しく作る

        # INSERT文を準備
        # ? は DuckDB Python のパラメータプレースホルダ/列数ぶん ?, ?, ?, ... を生成
        expected_cols = len(job.schema.columns)
        placeholders = ", ".join(["?"] * expected_cols)
        insert_sql = f"INSERT INTO dataset VALUES ({placeholders})"

        # ページを1から回す
        page = 1
        total_rows = 0
        while True:
            meta, data = self._fetch_page(page) # API から meta と data を取る
            if not data:
                logger.debug("Page %d returned empty data — stopping.", page)
                break
            rows = self._normalize(data) # JSON→行配列に変換
            if rows and len(rows[0]) != expected_cols:
                raise ConnectorError(
                    f"_normalize() returned {len(rows[0])} columns but schema has "
                    f"{expected_cols}. Update _normalize() to match the schema."
                )
            conn.executemany(insert_sql, rows) # 一括INSERT
            total_rows += len(rows)
            try:
                pages = int(meta.get("pages", 1)) # 最終ページまで回す
            except (TypeError, ValueError) as exc:
                raise ConnectorError(
                    f"WorldBank API returned invalid 'pages' in meta: {meta.get('pages')!r}"
                ) from exc
            logger.info("  page %d/%d — %d rows", page, pages, len(rows))
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

    # API を叩いて JSON を検証
    def _fetch_page(self, page: int) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        url = (
            f"{_BASE_URL}/country/{self.country_code}"
            f"/indicator/{self.indicator_code}"
        )
        # params= を使うと、URLの「?key=value&...」部分を requests が安全に組み立ててくれる
        params = {"format": "json", "per_page": self.per_page, "page": page}
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise ConnectorError(
                f"WorldBank API request failed (indicator={self.indicator_code}, "
                f"country={self.country_code}, page={page}): {exc}"
            ) from exc

        try:
            payload = resp.json()
        except ValueError as exc:
            raise ConnectorError(
                f"WorldBank API returned non-JSON response (indicator={self.indicator_code}, "
                f"country={self.country_code}, page={page}): {exc}"
            ) from exc
        if not isinstance(payload, list) or len(payload) < 2:
            raise ConnectorError(
                f"Unexpected WorldBank API response format: {payload!r}"
            )
        meta = payload[0] or {}
        data = payload[1] or []
        return meta, data

    # JSONを、DuckDBの行に変換
    # NOTE: This method is hardcoded to produce exactly the 6 columns defined in
    # timeseries.yaml (country_code, country_name, indicator_code, indicator_name,
    # year, value). Any schema change in the YAML requires a matching update here.
    def _normalize(self, data: list[dict[str, Any]]) -> list[list[str | int | float | None]]:
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
