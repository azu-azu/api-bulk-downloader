"""
リトライ・指数バックオフ付きのコアストリーミングダウンローダー。

API固有のロジックは一切持たない。コネクタの詳細は connectors/ に集約する。
"""
import logging
import time
from pathlib import Path
from typing import Protocol

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from api_bulk_downloader.core import file_utils
from api_bulk_downloader.core.logger import DownloadMetrics

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# コネクタプロトコル — すべてのコネクタはこの2つのプロパティを実装する
# ---------------------------------------------------------------------------

class ConnectorProtocol(Protocol):
    """データソースコネクタが満たすべき最小インターフェース。"""

    @property
    def download_url(self) -> str:
        """ダウンロード対象リソースの完全URL。"""
        ...

    @property
    def request_headers(self) -> dict[str, str]:
        """ダウンロードリクエストに付与するHTTPヘッダ（認証情報など）。"""
        ...


# ---------------------------------------------------------------------------
# ダウンローダー
# ---------------------------------------------------------------------------

class BulkDownloader:
    """
    コネクタからリソースを1つダウンロードし、ディスクへストリーム書き込みする。
    ZIPアーカイブは自動展開し、計測値をログに出力する。
    """

    def __init__(
        self,
        connector: ConnectorProtocol,
        dest_dir: Path,
        *,
        chunk_size: int = file_utils.DEFAULT_CHUNK_SIZE,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        timeout: int = 30,
    ) -> None:
        self._connector = connector
        self._dest_dir = Path(dest_dir)
        self._chunk_size = chunk_size
        self._timeout = timeout
        self._session = self._build_session(max_retries, backoff_factor)

    # ------------------------------------------------------------------
    # 公開API
    # ------------------------------------------------------------------

    def download(self, filename: str, *, count_rows: bool = False) -> DownloadMetrics:
        """
        コネクタのリソースを *dest_dir/filename* へストリーム書き込みする。

        ダウンロードしたファイルがZIPアーカイブの場合は自動的に展開し、
        アーカイブ本体は展開先と同じディレクトリに保持する。

        Parameters
        ----------
        filename:
            *dest_dir* 内に書き込むファイル名。
        count_rows:
            True のとき、ダウンロード後に主要CSVのデータ行数を数える。
            ファイル全体を再スキャンするためデフォルトは無効。
            大規模データ（40万行超）では約2秒の追加コストがかかる。

        Returns
        -------
        DownloadMetrics
            計測値が格納されたインスタンス。
        """
        metrics = DownloadMetrics()
        dest_file = self._dest_dir / filename

        log.info("Starting download: %s → %s", self._connector.download_url, dest_file)

        response = self._get(self._connector.download_url)
        metrics.bytes_downloaded = file_utils.stream_to_file(
            response, dest_file, chunk_size=self._chunk_size
        )

        # ZIPなら展開する
        if file_utils.is_zip(dest_file):
            log.info("Archive detected — extracting to %s", self._dest_dir)
            extracted = file_utils.extract_zip(dest_file, self._dest_dir)
            if count_rows:
                csvs = [p for p in extracted if p.suffix.lower() == ".csv"]
                if csvs:
                    try:
                        primary_csv = file_utils.choose_primary_csv(csvs)
                        metrics.row_count = file_utils.count_csv_rows(primary_csv)
                        log.info(
                            "Row count (%s): %d", primary_csv.name, metrics.row_count
                        )
                    except Exception as exc:
                        log.warning("Could not count CSV rows: %s", exc)
        elif dest_file.suffix.lower() == ".csv" and count_rows:
            try:
                metrics.row_count = file_utils.count_csv_rows(dest_file)
                log.info("Row count (%s): %d", dest_file.name, metrics.row_count)
            except Exception as exc:
                log.warning("Could not count CSV rows: %s", exc)

        metrics.finish()
        metrics.log(log)
        return metrics

    # ------------------------------------------------------------------
    # 内部ヘルパー
    # ------------------------------------------------------------------

    def _get(self, url: str) -> requests.Response:
        """GETリクエストを実行してストリーミングレスポンスを返す。"""
        response = self._session.get(
            url,
            headers=self._connector.request_headers,
            stream=True,
            timeout=self._timeout,
        )
        response.raise_for_status()
        return response

    @staticmethod
    def _build_session(max_retries: int, backoff_factor: float) -> requests.Session:
        """
        一時的なエラーに対して自動リトライする requests.Session を生成する。

        HTTP 429・500・502・503・504 を対象に指数バックオフでリトライする:
            待機時間 = backoff_factor × 2^(リトライ回数 - 1)
        """
        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist={429, 500, 502, 503, 504},
            allowed_methods={"GET"},
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
