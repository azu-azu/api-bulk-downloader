from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class JobSummary:
    job_id: str
    status: str  # "success" | "skipped" | "probed" | "failed"
    started_at: str | None = None
    finished_at: str | None = None
    duration_seconds: float | None = None
    rows_exported: int | None = None
    export_path: str | None = None
    discovery_columns: list[str] = field(default_factory=list)
    error: str | None = None

    def finish(
        self,
        *,
        rows: int | None = None,
        export_path: Path | None = None,
        discovery_columns: list[str] | None = None,
        error: str | None = None,
    ) -> None:
        self.finished_at = _now_iso()
        assert self.started_at is not None, "finish() called before started_at was set"
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
        dest = output_dir / f"{self.job_id}_summary.json"
        with dest.open("w") as fh:
            json.dump(asdict(self), fh, indent=2)
        logger.debug("Summary written: %s", dest)
        return dest


def make_summary(job_id: str) -> JobSummary:
    return JobSummary(job_id=job_id, status="pending", started_at=_now_iso())


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
