"""CLI entry point for the batch data pipeline.

司令塔
入口で全部の“解釈”を終わらせて、runner に “実行” だけ渡す
run / run-all / list / gui の4コマンドを argparse で受けて、
.env を読んで、manifest.yaml をロードして、runner.run_pipeline() を呼ぶ。
run-all のときだけ 出力先の衝突（上書き事故）を事前検知して止める

Usage:
    wdi-pipeline run     [--manifest PATH] [--output-root PATH] [--dry-run] [--probe] [--only JOB_ID]
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
import unicodedata
from pathlib import Path

from dotenv import load_dotenv

from wdi_pipeline.logging_setup import setup_logging
from wdi_pipeline.manifest import load_manifest
from wdi_pipeline.runner import run_pipeline

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Table formatter (stdlib only — no tabulate dependency)
# ---------------------------------------------------------------------------

# 端末表示幅を「全角=2、半角=1」で数える
# 日本語混じりの列名/パスを出したとき、普通の len() だと列がズレるから、それを回避する
def _display_width(s: str) -> int:
    """Return terminal display width: full-width chars count as 2, others as 1."""
    return sum(2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1 for ch in s)


# pad = padding/ 文字の後ろにスペースを足して、列の幅をそろえる
def _pad(s: str, width: int) -> str:
    # 足りない分だけ " " を足す
    return s + " " * max(0, width - _display_width(s))


# 列数を揃える → 各列の最大幅を計算 → 空白で右パディング → ヘッダ＋区切り線＋本体を印字
def _simple_table(headers: list[str], rows: list[list[str]]) -> None:
    """Print a simple two-line-header table, handling full-width characters."""
    # 列数（カラム数）をヘッダ基準で決める
    n = len(headers)

    # 各行を文字列化して、「全行を必ず n 列にする」（正規化）
    norm: list[list[str]] = []
    for r in rows:
        # True とか Path とか数字が混ざってても、全部文字列にする（print しやすくする）
        r = list(map(str, r))
        if len(r) < n:
            # 足りない列は ""（空文字）で埋める
            r = r + [""] * (n - len(r))
        elif len(r) > n:
            # 余った列は切り捨てる（表の列数を超えないように）
            r = r[:n]
        norm.append(r)

    # ヘッダもデータも含めて列幅を決めたいから合体
    all_rows = [list(map(str, headers))] + norm

    # 各列の “必要な横幅” を計算する（全角考慮）
    widths = [max(_display_width(cell) for cell in col) for col in zip(*all_rows)]

    # 1行を “列幅に合わせて整形した文字列” にする関数 fmt を定義
    def fmt(row: list[str]) -> str:
        return "  ".join(_pad(c, w) for c, w in zip(row, widths))

    # 印字（ヘッダ → 区切り線 → 本体）
    print(fmt(all_rows[0]))
    print("  ".join("-" * w for w in widths))
    for row in norm:
        print(fmt(row))


# manifest のパスを“引数 or 環境変数”で確定する関数
def _require_manifest(args_value: str | None) -> str | None:
    """Return resolved manifest path string, or None (error printed) if missing."""
    # パス候補を決める（優先順位つき）
    val = args_value or os.environ.get("WDI_MANIFEST")
    if not val:
        print(
            "Error: manifest path is required. "
            "Use --manifest or set WDI_MANIFEST in .env.",
            file=sys.stderr,
        )
    return val


# ディレクトリ確定
def _require_pipeline_dir(args_value: str | None) -> str | None:
    """Return resolved pipeline dir string, or None (error printed) if missing."""
    val = args_value or os.environ.get("WDI_PIPELINE_DIR")
    if not val:
        print(
            "Error: pipeline dir is required. "
            "Use --pipeline-dir or set WDI_PIPELINE_DIR in .env.",
            file=sys.stderr,
        )
    return val


# argparse でサブコマンドを解析
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wdi-pipeline",
        description="Manifest-driven batch data pipeline.",
    )
    # サブコマンド読み取り
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
        metavar="JOB_ID",
        default=None,
        help="Execute only the job with this job_id.",
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


# -- メイン実行 --
def main(argv: list[str] | None = None) -> int:
    load_dotenv()

    # 入力を解釈する
    parser = _build_parser()
    args = parser.parse_args(argv)

    # == run == 単体実行の最小ループ
    if args.command == "run":
        setup_logging(args.log_level)
        # Resolve manifest path: CLI > env > error
        manifest_str = _require_manifest(args.manifest)
        if not manifest_str:
            return 1
        manifest_path = Path(manifest_str)

        # manifest をロードして設定を確定する
        manifest = load_manifest(manifest_path, base_dir=manifest_path.parent)

        # Resolve output_root: CLI > manifest default
        if args.output_root:
            manifest.output_root = Path(args.output_root)

        # runner を呼ぶ（実行）
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

    # == run-all == 複数manifest一括実行 + 衝突検知
    elif args.command == "run-all":
        setup_logging(args.log_level)
        # Resolve pipeline dir: CLI > env > error
        pipeline_dir_str = _require_pipeline_dir(args.pipeline_dir)
        if not pipeline_dir_str:
            return 1

        pipeline_dir = Path(pipeline_dir_str)

        # manifest を列挙
        manifests = sorted(pipeline_dir.glob("*/manifest.yaml"))
        if not manifests:
            print(f"No manifest.yaml found under '{pipeline_dir}'.", file=sys.stderr)
            return 1

        # 一回全部ロード
        # 衝突検知（preflight）をするには、全ジョブの出力先を 事前に全件把握 する必要があるため
        loaded = []
        for manifest_path in manifests:
            manifest = load_manifest(manifest_path, base_dir=manifest_path.parent)
            if args.output_root:
                manifest.output_root = Path(args.output_root)
            loaded.append((manifest_path, manifest))

        # -- Preflight collision check (skip only if --allow-overwrite) --
        # Runs even in --dry-run / --probe mode so config errors are caught early.
        #
        # Naming spec (confirmed from source):
        #   export:  output_root / f"{job.export.filename}.{ext}"   (runner.py)
        #   summary: output_root / f"{job.job_id}_summary.json"       (summary.py)
        #
        # Both paths are checked independently.

        # preflight 離陸前点検（上書き事故の予防）
        # ファイル名が被るかどうかのチェック
        # --allow-overwrite が無い場合だけ実行
        if not args.allow_overwrite:
            seen: dict[str, str] = {}  # resolved dest path → "pipeline/job (export|summary)"
            collisions: list[str] = []
            for manifest_path, manifest in loaded:
                for job in manifest.jobs:
                    if not job.enabled:
                        continue
                    root = manifest.output_root
                    key = f"{manifest_path.parent.name}/{job.job_id}"
                    ext = job.export.format

                    # 2種類チェック
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

        # runner を呼ぶ（run_pipeline を順に回す）
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

    # 設定一覧表示
    # list = report（確認用のスナップショット）
    elif args.command == "list":
        pipeline_dir_str = _require_pipeline_dir(args.pipeline_dir)
        if not pipeline_dir_str:
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
        _simple_table(headers, table)
        return 0

    # TUI起動
    # gui = control panel（運用・編集の操作盤）
    elif args.command == "gui":
        pipeline_dir_str = _require_pipeline_dir(args.pipeline_dir)
        if not pipeline_dir_str:
            return 1

        from wdi_pipeline.tui import PipelineApp

        PipelineApp(pipeline_dir_str).run()
        return 0


# sys.exit(0) → 成功
# sys.exit(1) → 失敗
if __name__ == "__main__":
    sys.exit(main())
