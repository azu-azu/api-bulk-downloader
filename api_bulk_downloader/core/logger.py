"""
Logging setup and download metrics tracking.
"""
import logging
import time
from dataclasses import dataclass, field
from typing import Optional


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure root logger with a standard format."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    return logging.getLogger("api_bulk_downloader")


@dataclass
class DownloadMetrics:
    """Captures timing and volume stats for a single download operation."""

    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    bytes_downloaded: int = 0
    row_count: Optional[int] = None  # populated only for CSV files

    def finish(self) -> None:
        """Mark the download as complete and record end time."""
        self.end_time = time.time()

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.end_time is None:
            return None
        return round(self.end_time - self.start_time, 3)

    def log(self, logger: logging.Logger) -> None:
        """Emit a structured summary to the logger."""
        logger.info(
            "Download complete | "
            "start=%.3f end=%.3f duration=%ss bytes=%d rows=%s",
            self.start_time,
            self.end_time,
            self.duration_seconds,
            self.bytes_downloaded,
            self.row_count if self.row_count is not None else "n/a",
        )
