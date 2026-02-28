"""CLI entry point for the batch data pipeline.

Usage:
    wdi-pipeline run --manifest pipelines/default/manifest.yaml [--dry-run] [--probe] [--only JOB_NAME]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from wdi_pipeline.logging_setup import setup_logging
from wdi_pipeline.manifest import load_manifest
from wdi_pipeline.runner import run_pipeline


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cli",
        description="Manifest-driven batch data pipeline.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Execute the pipeline.")
    run_p.add_argument(
        "--manifest",
        required=True,
        metavar="PATH",
        help="Path to manifest.yaml",
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
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    setup_logging(args.log_level)

    if args.command == "run":
        manifest_path = Path(args.manifest)
        manifest = load_manifest(manifest_path, base_dir=manifest_path.parent)
        summaries = run_pipeline(
            manifest,
            dry_run=args.dry_run,
            probe=args.probe,
            only=args.only,
        )
        failed = [s for s in summaries if s.status == "failed"]
        if failed:
            names = ", ".join(s.job_name for s in failed)
            print(f"\nFailed jobs: {names}", file=sys.stderr)
            return 1
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
