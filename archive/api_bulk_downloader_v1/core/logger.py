"""
ロギング設定とダウンロード計測値の管理。
"""
import logging
import time
from dataclasses import dataclass, field
from typing import Optional


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """ルートロガーを標準フォーマットで設定する。"""
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    return logging.getLogger("api_bulk_downloader")


@dataclass
class DownloadMetrics:
    """1回のダウンロード操作に関する時間・サイズの計測値を保持する。"""

    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    bytes_downloaded: int = 0
    row_count: Optional[int] = None

    def finish(self) -> None:
        self.end_time = time.time()

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.end_time is None:
            return None
        return round(self.end_time - self.start_time, 3)

    def log(self, logger: logging.Logger) -> None:
        logger.info(
            "Download complete | start=%.3f end=%.3f duration=%ss bytes=%d rows=%s",
            self.start_time,
            self.end_time,
            self.duration_seconds,
            self.bytes_downloaded,
            self.row_count if self.row_count is not None else "n/a",
        )
