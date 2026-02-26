"""
API Bulk Downloader のエントリーポイント。

使い方
------
    # 単一指標（小サイズ、約100KB）
    python -m api_bulk_downloader.main --indicator NY.GDP.MKTP.CD

    # WDI全量バルクダウンロード（大サイズ、圧縮約50MB・解凍約500MB）
    python -m api_bulk_downloader.main --wdi
"""
import argparse
import logging
import sys
from pathlib import Path

from api_bulk_downloader.connectors.worldbank import (
    WorldBankConnector,
    WorldBankWDIConnector,
)
from api_bulk_downloader.core import config
from api_bulk_downloader.core.downloader import BulkDownloader
from api_bulk_downloader.core.logger import setup_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="API Bulk Downloader — 大規模データセットを安全にディスクへストリーム保存する。"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--indicator",
        default="NY.GDP.MKTP.CD",
        help="World Bank指標コード（デフォルト: NY.GDP.MKTP.CD — GDP・米ドル現在価格）",
    )
    mode.add_argument(
        "--wdi",
        action="store_true",
        help=(
            "World Development Indicators 全量バルクZIPをダウンロードする "
            "（圧縮約50MB・解凍約500MB）。大ファイルのストリーミング練習用。"
        ),
    )
    parser.add_argument(
        "--dest",
        default=None,
        help="ダウンロード先ディレクトリ（デフォルト: ./downloads）",
    )
    parser.add_argument(
        "--set-dest",
        metavar="PATH",
        help=(
            "出力先ディレクトリを設定ファイルに保存して終了する "
            f"（保存先: {config.CONFIG_PATH}）"
        ),
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=8192,
        help="ストリーミングのチャンクサイズ（バイト、デフォルト: 8192）",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="HTTPリトライ上限回数（デフォルト: 3）",
    )
    parser.add_argument(
        "--count-rows",
        action="store_true",
        help="ダウンロード後にCSVのデータ行数を数える（デフォルト: 無効）。",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="DEBUGレベルのログを有効にする",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger = setup_logging(log_level)

    # --set-dest: 設定を保存してすぐ終了
    if args.set_dest:
        dest_path = Path(args.set_dest).resolve()
        config.save_dest(dest_path)
        print(f"設定を保存しました: {dest_path}  →  {config.CONFIG_PATH}")
        return 0

    # dest 解決: CLI > 設定ファイル > ビルトインデフォルト
    if args.dest:
        dest_dir = Path(args.dest)
        source = "CLI引数 --dest"
    elif (cfg_dest := config.load_dest()) is not None:
        dest_dir = cfg_dest
        source = f"設定ファイル ({config.CONFIG_PATH})"
    else:
        dest_dir = Path("downloads")
        source = "ビルトインデフォルト"
    logger.info("出力先 : %s  [%s]", dest_dir, source)

    if args.wdi:
        connector = WorldBankWDIConnector()
        logger.info("Connector : WorldBank WDI (全量バルクデータセット)")
    else:
        connector = WorldBankConnector(indicator=args.indicator)
        logger.info("Connector : WorldBank | indicator=%s", args.indicator)

    downloader = BulkDownloader(
        connector=connector,
        dest_dir=dest_dir,
        chunk_size=args.chunk_size,
        max_retries=args.retries,
    )

    filename = connector.suggested_filename()
    logger.info("Destination : %s/%s", dest_dir, filename)

    try:
        metrics = downloader.download(filename, count_rows=args.count_rows)
    except Exception as exc:
        logger.error("Download failed: %s", exc)
        return 1

    logger.info(
        "Summary — bytes=%d | rows=%s | duration=%ss",
        metrics.bytes_downloaded,
        metrics.row_count if metrics.row_count is not None else "n/a",
        metrics.duration_seconds,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
