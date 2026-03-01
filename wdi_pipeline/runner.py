from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import duckdb

from wdi_pipeline.connectors.worldbank_indicator import WorldBankIndicatorConnector
from wdi_pipeline.exceptions import PipelineError
from wdi_pipeline.exporter import export
from wdi_pipeline.manifest import JobConfig, ManifestConfig
from wdi_pipeline.sql_template import render
from wdi_pipeline.summary import JobSummary, make_summary

logger = logging.getLogger(__name__)


def run_pipeline(
    manifest: ManifestConfig,
    *,
    dry_run: bool = False,
    probe: bool = False,
    only: str | None = None,
) -> list[JobSummary]:
    """Execute the pipeline for all enabled jobs (or a single named job).

    Args:
        manifest: Parsed and validated ManifestConfig.
        dry_run: If True, skip all network calls and exports.
        probe: If True, run discover() only (no materialize / export).
        only: If set, execute only the job with this id.

    Returns:
        List of JobSummary objects, one per processed job.
    """
    jobs = manifest.enabled_jobs()
    if only:
        jobs = [j for j in jobs if j.job_id == only]
        if not jobs:
            raise PipelineError(
                f"No enabled job with id '{only}' found in manifest."
            )

    output_root = manifest.output_root
    summaries: list[JobSummary] = []

    for job in jobs:
        summary = _run_job(job, output_root, dry_run=dry_run, probe=probe)
        summary.write(output_root)
        summaries.append(summary)

    return summaries


def _run_job(
    job: JobConfig,
    output_root: Path,
    *,
    dry_run: bool,
    probe: bool,
) -> JobSummary:
    summary = make_summary(job.job_id)
    logger.info("=== Job: %s ===", job.job_id)

    if dry_run:
        logger.info("[dry-run] Skipping job '%s' — no network calls.", job.job_id)
        summary.status = "skipped"
        summary.finish()
        return summary

    connector = _build_connector(job)

    try:
        # discover() is always called (no network required for worldbank)
        logger.info("  discover …")
        discovery = connector.discover(job)
        logger.info("  discover done: %d columns", len(discovery.columns))

        if probe:
            logger.info("[probe] Job '%s' — discover complete, skipping materialize.", job.job_id)
            summary.status = "probed"
            summary.finish(discovery_columns=discovery.columns)
            return summary

        # Full execution
        conn = duckdb.connect()
        try:
            logger.info("  materialize …")
            connector.materialize(job, conn)
            logger.info("  materialize done")

            sql_text = job.sql.file.read_text()
            rendered_sql = render(sql_text, job.sql.params)

            ext = job.export.format
            dest = (output_root / f"{job.export.filename}.{ext}").resolve()

            logger.info("  export …")
            rows = export(conn, rendered_sql, dest, job.export.format)
        finally:
            conn.close()

        summary.status = "success"
        summary.finish(
            rows=rows,
            export_path=dest,
            discovery_columns=discovery.columns,
        )
        logger.info("Job '%s' done — %d rows → %s  (%.2f s)", job.job_id, rows, dest, summary.duration_seconds)

    except PipelineError as exc:
        logger.error("Job '%s' failed: %s", job.job_id, exc)
        summary.status = "failed"
        summary.finish(error=str(exc))
    except Exception as exc:
        logger.exception("Unexpected error in job '%s'", job.job_id)
        summary.status = "failed"
        summary.finish(error=f"Unexpected error: {exc}")

    return summary


def _build_connector(job: JobConfig) -> Any:
    return WorldBankIndicatorConnector(**job.connector_params)
