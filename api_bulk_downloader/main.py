"""
Entry point for the API Bulk Downloader.

Usage
-----
    # Single indicator (small, ~100 KB)
    python -m api_bulk_downloader.main --indicator NY.GDP.MKTP.CD

    # Full WDI bulk download (large, ~50 MB compressed / ~500 MB uncompressed)
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
from api_bulk_downloader.core.downloader import BulkDownloader
from api_bulk_downloader.core.logger import setup_logging


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="API Bulk Downloader — stream large datasets to disk safely."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--indicator",
        default="NY.GDP.MKTP.CD",
        help="World Bank indicator code (default: NY.GDP.MKTP.CD — GDP current USD)",
    )
    mode.add_argument(
        "--wdi",
        action="store_true",
        help=(
            "Download the full World Development Indicators bulk ZIP "
            "(~50 MB compressed, ~500 MB uncompressed). "
            "Use this for large-file streaming practice."
        ),
    )
    parser.add_argument(
        "--dest",
        default="downloads",
        help="Destination directory for downloaded files (default: ./downloads)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=8192,
        help="Streaming chunk size in bytes (default: 8192)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Maximum number of HTTP retries (default: 3)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG-level logging",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger = setup_logging(log_level)

    dest_dir = Path(args.dest)

    if args.wdi:
        connector = WorldBankWDIConnector()
        logger.info("Connector : WorldBank WDI (full bulk dataset)")
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
        metrics = downloader.download(filename)
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
