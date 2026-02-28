"""
API Bulk Downloader のエントリーポイント。
"""
import argparse
import logging
import sys
from pathlib import Path

from api_bulk_downloader.connectors.worldbank import WorldBankConnector, WorldBankWDIConnector
from api_bulk_downloader.core.downloader import BulkDownloader
from api_bulk_downloader.core.logger import setup_logging


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="api-bulk-downloader",
        description="API からデータをバルクダウンロードする。",
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--indicator",
        metavar="CODE",
        help="World Bank インジケーターコード (例: NY.GDP.MKTP.CD)",
    )
    source.add_argument(
        "--wdi",
        action="store_true",
        help="World Bank WDI 全量バルク ZIP をダウンロードする",
    )
    parser.add_argument(
        "--dest",
        default="downloads/",
        metavar="DIR",
        help="保存先ディレクトリ (デフォルト: downloads/)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=8192,
        metavar="BYTES",
        help="ストリーミングチャンクサイズ (デフォルト: 8192)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        metavar="N",
        help="最大リトライ回数 (デフォルト: 3)",
    )
    parser.add_argument(
        "--count-rows",
        action="store_true",
        help="ダウンロード後に CSV 行数をカウントする",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="DEBUG レベルのログを出力する",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    setup_logging(logging.DEBUG if args.verbose else logging.INFO)

    dest_dir = Path(args.dest)
    if args.wdi:
        connector = WorldBankWDIConnector()
    else:
        connector = WorldBankConnector(indicator=args.indicator)

    downloader = BulkDownloader(
        connector=connector,
        dest_dir=dest_dir,
        chunk_size=args.chunk_size,
        max_retries=args.retries,
    )
    downloader.download(connector.suggested_filename(), count_rows=args.count_rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
