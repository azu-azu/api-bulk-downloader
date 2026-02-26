"""
Core streaming downloader with retry / exponential backoff.

Contains no API-specific logic — all connector details live in connectors/.
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
# Connector protocol — every connector must expose these two attributes
# ---------------------------------------------------------------------------

class ConnectorProtocol(Protocol):
    """Minimal interface that any data-source connector must satisfy."""

    @property
    def download_url(self) -> str:
        """Full URL to the resource being downloaded."""
        ...

    @property
    def request_headers(self) -> dict[str, str]:
        """HTTP headers to include in the download request (auth, etc.)."""
        ...


# ---------------------------------------------------------------------------
# Downloader
# ---------------------------------------------------------------------------

class BulkDownloader:
    """
    Downloads a single resource from a connector, streams it to disk,
    optionally extracts ZIP archives, and logs metrics.
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
    # Public API
    # ------------------------------------------------------------------

    def download(self, filename: str) -> DownloadMetrics:
        """
        Stream the connector's resource to *dest_dir/filename*.

        If the downloaded file is a ZIP archive it is automatically extracted
        and the archive itself is kept alongside the extracted contents.

        Returns a populated :class:`DownloadMetrics` instance.
        """
        metrics = DownloadMetrics()
        dest_file = self._dest_dir / filename

        log.info("Starting download: %s → %s", self._connector.download_url, dest_file)

        response = self._get(self._connector.download_url)
        metrics.bytes_downloaded = file_utils.stream_to_file(
            response, dest_file, chunk_size=self._chunk_size
        )

        # Unzip if necessary
        if file_utils.is_zip(dest_file):
            log.info("Archive detected — extracting to %s", self._dest_dir)
            extracted = file_utils.extract_zip(dest_file, self._dest_dir)
            csvs = [p for p in extracted if p.suffix.lower() == ".csv"]
            if csvs:
                # Prefer data CSVs (prefixed "API_") over metadata CSVs.
                # World Bank ZIPs contain both; without this we'd count the
                # 1-row Metadata_Indicator file instead of the actual dataset.
                data_csvs = [p for p in csvs if p.name.startswith("API_")]
                primary_csv = data_csvs[0] if data_csvs else csvs[0]
                try:
                    metrics.row_count = file_utils.count_csv_rows(primary_csv)
                    log.info("Row count (%s): %d", primary_csv.name, metrics.row_count)
                except Exception as exc:
                    log.warning("Could not count CSV rows: %s", exc)
        elif dest_file.suffix.lower() == ".csv":
            try:
                metrics.row_count = file_utils.count_csv_rows(dest_file)
                log.info("Row count: %d", metrics.row_count)
            except Exception as exc:
                log.warning("Could not count CSV rows: %s", exc)

        metrics.finish()
        metrics.log(log)
        return metrics

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, url: str) -> requests.Response:
        """Execute a GET request and return the streaming response."""
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
        Create a requests.Session with automatic retry on transient errors.

        Retries on HTTP 429, 500, 502, 503, 504 with exponential backoff:
            wait = backoff_factor * (2 ** (retry_number - 1))
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
