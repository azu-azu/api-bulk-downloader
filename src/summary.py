from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class JobSummary:
    job_name: str
    status: str  # "success" | "skipped" | "probed" | "failed"
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    rows_exported: Optional[int] = None
    export_path: Optional[str] = None
    discovery_columns: list[str] = field(default_factory=list)
    error: Optional[str] = None

    def finish(
        self,
        *,
        rows: Optional[int] = None,
        export_path: Optional[Path] = None,
        discovery_columns: Optional[list[str]] = None,
        error: Optional[str] = None,
    ) -> None:
        self.finished_at = _now_iso()
        if self.started_at:
            started = datetime.fromisoformat(self.started_at)
            finished = datetime.fromisoformat(self.finished_at)
            self.duration_seconds = round(
                (finished - started).total_seconds(), 3
            )
        if rows is not None:
            self.rows_exported = rows
        if export_path is not None:
            self.export_path = str(export_path)
        if discovery_columns is not None:
            self.discovery_columns = discovery_columns
        if error is not None:
            self.error = error

    def write(self, output_dir: Path) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        dest = output_dir / f"{self.job_name}_summary.json"
        with dest.open("w") as fh:
            json.dump(asdict(self), fh, indent=2)
        logger.debug("Summary written: %s", dest)
        return dest


def make_summary(job_name: str) -> JobSummary:
    return JobSummary(job_name=job_name, status="pending", started_at=_now_iso())


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
