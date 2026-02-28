"""CLI entry point for the batch data pipeline.

Usage:
    wdi-pipeline run     [--manifest PATH] [--output-root PATH] [--dry-run] [--probe] [--only JOB_NAME]
    wdi-pipeline run-all [--pipeline-dir PATH] [--output-root PATH] [--dry-run] [--probe]
    wdi-pipeline list    [--pipeline-dir PATH]
    wdi-pipeline gui     [--pipeline-dir PATH]

Resolution order:
    manifest path  : --manifest      >  WDI_MANIFEST (.env)      >  error
    pipeline dir   : --pipeline-dir  >  WDI_PIPELINE_DIR (.env)  >  error
    output root    : --output-root (CLI only)  >  manifest defaults.output_root

Note: --output-root behaves identically in both `run` and `run-all`.
It replaces manifest.output_root directly; output files are placed flat
in the specified directory. `run-all` performs a preflight collision check
on both export paths ({filename}.{ext}) and summary paths ({job_id}_summary.json),
and exits with an error if any two pipelines would write to the same path.
Use --allow-overwrite to disable the check (last write wins).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from wdi_pipeline.logging_setup import setup_logging
from wdi_pipeline.manifest import load_manifest
from wdi_pipeline.runner import run_pipeline

logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cli",
        description="Manifest-driven batch data pipeline.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # --- run ---
    run_p = sub.add_parser("run", help="Execute a single pipeline manifest.")
    run_p.add_argument(
        "--manifest",
        default=None,
        metavar="PATH",
        help="Path to manifest.yaml (overrides WDI_MANIFEST env var).",
    )
    run_p.add_argument(
        "--output-root",
        default=None,
        metavar="PATH",
        help=(
            "Override output directory (direct replacement of manifest output_root). "
            "Output files are placed flat in PATH."
        ),
    )
    run_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip all network calls and exports (structural check only).",
    )
    run_p.add_argument(
        "--probe",
        action="store_true",
        help="Run discover() only — print columns, skip materialize and export.",
    )
    run_p.add_argument(
        "--only",
        metavar="JOB_NAME",
        default=None,
        help="Execute only the named job.",
    )
    run_p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO).",
    )

    # --- run-all ---
    run_all_p = sub.add_parser("run-all", help="Execute all pipelines under a directory.")
    run_all_p.add_argument(
        "--pipeline-dir",
        default=None,
        metavar="PATH",
        help="Directory containing pipeline subdirs (overrides WDI_PIPELINE_DIR env var).",
    )
    run_all_p.add_argument(
        "--output-root",
        default=None,
        metavar="PATH",
        help=(
            "Override output root for all pipelines (direct replacement, same as `run`). "
            "All exports land flat in PATH. "
            "Preflight detects path collisions and exits with an error; "
            "use --allow-overwrite to disable. "
            "If omitted, each manifest's own output_root is used."
        ),
    )
    run_all_p.add_argument(
        "--allow-overwrite",
        action="store_true",
        help="Skip preflight collision check; last write wins if filenames clash.",
    )
    run_all_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip all network calls and exports.",
    )
    run_all_p.add_argument(
        "--probe",
        action="store_true",
        help="Run discover() only.",
    )
    run_all_p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    # --- list ---
    list_p = sub.add_parser("list", help="List all pipeline jobs and their configuration.")
    list_p.add_argument(
        "--pipeline-dir",
        default=None,
        metavar="PATH",
        help="Directory containing pipeline subdirs (overrides WDI_PIPELINE_DIR env var).",
    )

    # --- gui ---
    gui_p = sub.add_parser("gui", help="Launch TUI dashboard.")
    gui_p.add_argument(
        "--pipeline-dir",
        default=None,
        metavar="PATH",
        help="Directory containing pipeline subdirs (overrides WDI_PIPELINE_DIR env var).",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv()

    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        setup_logging(args.log_level)
        # Resolve manifest path: CLI > env > error
        manifest_str = args.manifest or os.environ.get("WDI_MANIFEST")
        if not manifest_str:
            print(
                "Error: manifest path is required. "
                "Use --manifest or set WDI_MANIFEST in .env.",
                file=sys.stderr,
            )
            return 1
        manifest_path = Path(manifest_str)
        manifest = load_manifest(manifest_path, base_dir=manifest_path.parent)

        # Resolve output_root: CLI > manifest default
        if args.output_root:
            manifest.output_root = Path(args.output_root)

        summaries = run_pipeline(
            manifest,
            dry_run=args.dry_run,
            probe=args.probe,
            only=args.only,
        )
        failed = [s for s in summaries if s.status == "failed"]
        if failed:
            names = ", ".join(s.job_id for s in failed)
            print(f"\nFailed jobs: {names}", file=sys.stderr)
            return 1
        return 0

    elif args.command == "run-all":
        setup_logging(args.log_level)
        pipeline_dir_str = args.pipeline_dir or os.environ.get("WDI_PIPELINE_DIR")
        if not pipeline_dir_str:
            print(
                "Error: pipeline dir is required. "
                "Use --pipeline-dir or set WDI_PIPELINE_DIR in .env.",
                file=sys.stderr,
            )
            return 1

        pipeline_dir = Path(pipeline_dir_str)
        manifests = sorted(pipeline_dir.glob("*/manifest.yaml"))
        if not manifests:
            print(f"No manifest.yaml found under '{pipeline_dir}'.", file=sys.stderr)
            return 1

        # Load all manifests first (needed for preflight collision check)
        loaded = []
        for manifest_path in manifests:
            manifest = load_manifest(manifest_path, base_dir=manifest_path.parent)
            if args.output_root:
                manifest.output_root = Path(args.output_root)
            loaded.append((manifest_path, manifest))

        # Preflight collision check (skip only if --allow-overwrite)
        # Runs even in --dry-run / --probe mode so config errors are caught early.
        #
        # Naming spec (confirmed from source):
        #   export:  output_root / f"{job.export.filename}.{ext}"   (runner.py)
        #   summary: output_root / f"{job.job_id}_summary.json"       (summary.py)
        #
        # Both paths are checked independently.
        if not args.allow_overwrite:
            seen: dict[str, str] = {}  # resolved dest path → "pipeline/job (export|summary)"
            collisions: list[str] = []
            for manifest_path, manifest in loaded:
                for job in manifest.jobs:
                    if not job.enabled:
                        continue
                    root = manifest.output_root
                    key = f"{manifest_path.parent.name}/{job.job_id}"
                    ext = "csv" if job.export.format == "csv" else "parquet"
                    export_dest = str((root / f"{job.export.filename}.{ext}").resolve())
                    summary_dest = str((root / f"{job.job_id}_summary.json").resolve())
                    for dest, kind in ((export_dest, "export"), (summary_dest, "summary")):
                        if dest in seen:
                            collisions.append(
                                f"  {dest!r}\n    <- {seen[dest]}\n    <- {key} ({kind})"
                            )
                        else:
                            seen[dest] = f"{key} ({kind})"
            if collisions:
                print("Error: output path collisions detected:", file=sys.stderr)
                for c in collisions:
                    print(c, file=sys.stderr)
                print("\nUse --allow-overwrite to disable this check.", file=sys.stderr)
                return 1

        all_failed = []
        for manifest_path, manifest in loaded:
            logger.info("=== Pipeline: %s ===", manifest_path.parent.name)
            summaries = run_pipeline(manifest, dry_run=args.dry_run, probe=args.probe)
            all_failed.extend(s for s in summaries if s.status == "failed")

        if all_failed:
            names = ", ".join(s.job_id for s in all_failed)
            print(f"\nFailed jobs: {names}", file=sys.stderr)
            return 1
        return 0

    elif args.command == "list":
        from tabulate import tabulate

        pipeline_dir_str = args.pipeline_dir or os.environ.get("WDI_PIPELINE_DIR")
        if not pipeline_dir_str:
            print(
                "Error: pipeline dir is required. "
                "Use --pipeline-dir or set WDI_PIPELINE_DIR in .env.",
                file=sys.stderr,
            )
            return 1

        pipeline_dir = Path(pipeline_dir_str)
        manifest_paths = sorted(pipeline_dir.glob("*/manifest.yaml"))
        if not manifest_paths:
            print(f"No manifest.yaml found under '{pipeline_dir}'.", file=sys.stderr)
            return 1

        rows = []
        for manifest_path in manifest_paths:
            manifest = load_manifest(manifest_path, base_dir=manifest_path.parent)
            for job in manifest.jobs:
                rows.append((
                    job.enabled,
                    job.connector_params.get("indicator_code", ""),
                    f"{job.export.filename}.{job.export.format}",
                    str(manifest.output_root),
                    ", ".join(col.name for col in job.schema.columns),
                ))

        rows.sort(key=lambda r: (not r[0], r[1]))

        table = [
            ["true" if r[0] else "false", r[1], r[2], r[3], r[4]]
            for r in rows
        ]
        headers = ["Enabled", "indicator_code", "filename", "output dir", "column names"]
        print(tabulate(table, headers=headers, tablefmt="simple"))
        return 0

    elif args.command == "gui":
        pipeline_dir_str = args.pipeline_dir or os.environ.get("WDI_PIPELINE_DIR")
        if not pipeline_dir_str:
            print(
                "Error: pipeline dir is required. "
                "Use --pipeline-dir or set WDI_PIPELINE_DIR in .env.",
                file=sys.stderr,
            )
            return 1

        from wdi_pipeline.tui import PipelineApp

        PipelineApp(pipeline_dir_str).run()
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
