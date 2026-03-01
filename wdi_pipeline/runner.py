"""実行エンジン
「manifestで有効なjobを順番に回して、各jobを discover → materialize → export する」
"""

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

    manifest.enabled_jobs() で 実行対象job を取り出す
    only があれば job_id一致の1件だけ に絞る（見つからなければ PipelineError）
    for job in jobs: で 順番に _run_job() を呼ぶ
    返ってきた summary を summary.write(output_root) で サマリ保存して、配列で返す

    Args:
        manifest: Parsed and validated ManifestConfig.
        dry_run: If True, skip all network calls and exports.
        probe: If True, run discover() only (no materialize / export).
        only: If set, execute only the job with this id.

    Returns:
        List of JobSummary objects, one per processed job.
    """
    # 実行jobの選別フィルタ
    jobs = manifest.enabled_jobs()
    if only:
        jobs = [j for j in jobs if j.job_id == only]
        if not jobs:
            raise PipelineError(
                f"No enabled job with id '{only}' found in manifest."
            )

    output_root = manifest.output_root
    summaries: list[JobSummary] = []

    # manifest.jobs を回す
    for job in jobs:
        summary = _run_job(job, output_root, dry_run=dry_run, probe=probe)
        summary.write(output_root)
        summaries.append(summary)

    return summaries

# 1ジョブ分の全部を担当する
def _run_job(
    job: JobConfig,
    output_root: Path,
    *,
    dry_run: bool,
    probe: bool,
) -> JobSummary:

    summary = make_summary(job.job_id)
    logger.info("=== Job: %s ===", job.job_id)

    # finish() に渡す引数を積んでおく箱。最後に1回だけ呼ぶ。
    finish_kwargs: dict[str, Any] = {}

    try:
        if dry_run:
            # 流れを確認するだけのモード（ネットワーク・export なし）
            logger.info("[dry-run] Skipping job '%s' — no network calls.", job.job_id)
            summary.status = "skipped"
        else:
            # コネクタ生成 → discover（常に実行、ネットワーク不要）
            connector = _build_connector(job)
            logger.info("  discover …")
            discovery = connector.discover(job)
            logger.info("  discover done: %d columns", len(discovery.columns))

            if probe:
                # schema/列が想定どおりかの確認モード（materialize なし）
                logger.info("[probe] Job '%s' — discover complete, skipping materialize.", job.job_id)
                summary.status = "probed"
                finish_kwargs["discovery_columns"] = discovery.columns
            else:
                # -- Full execution --
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
                finish_kwargs.update(
                    rows=rows,
                    export_path=dest,
                    discovery_columns=discovery.columns,
                )

    except PipelineError as exc:
        logger.error("Job '%s' failed: %s", job.job_id, exc)
        summary.status = "failed"
        finish_kwargs = {"error": str(exc)}
    except Exception as exc:
        logger.exception("Unexpected error in job '%s'", job.job_id)
        summary.status = "failed"
        finish_kwargs = {"error": f"Unexpected error: {exc}"}
    finally:
        summary.finish(**finish_kwargs)

    if summary.status == "success":
        logger.info(
            "Job '%s' done — %d rows → %s  (%.2f s)",
            job.job_id,
            finish_kwargs["rows"],
            finish_kwargs["export_path"],
            summary.duration_seconds,
        )

    return summary


def _build_connector(job: JobConfig) -> WorldBankIndicatorConnector:
    # 現状 WorldBank のみ。複数コネクタに対応するときは connector_type フィールド＋レジストリ方式へ。
    return WorldBankIndicatorConnector(**job.connector_params)
