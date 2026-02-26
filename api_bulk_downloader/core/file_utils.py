"""
File handling utilities: streaming writes, zip extraction, row counting.
"""
import csv
import logging
import zipfile
from pathlib import Path
from typing import Iterator

import requests

log = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE: int = 8 * 1024  # 8 KB


def stream_to_file(
    response: requests.Response,
    dest: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> int:
    """
    Write a streaming HTTP response to *dest* without loading it into memory.

    Returns the total number of bytes written.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with dest.open("wb") as fh:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:  # filter out keep-alive empty chunks
                fh.write(chunk)
                total += len(chunk)
    log.debug("Wrote %d bytes to %s", total, dest)
    return total


def extract_zip(zip_path: Path, dest_dir: Path) -> list[Path]:
    """
    Extract all members of a ZIP archive into *dest_dir*.

    Returns a list of extracted file paths.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            zf.extract(name, dest_dir)
            extracted.append(dest_dir / name)
            log.debug("Extracted: %s", name)
    log.info("Extracted %d file(s) from %s", len(extracted), zip_path.name)
    return extracted


def is_zip(path: Path) -> bool:
    """Return True if *path* is a valid ZIP file."""
    return zipfile.is_zipfile(path)


def count_csv_rows(path: Path, has_header: bool = True) -> int:
    """
    Count data rows in a CSV file.

    Skips the header row when *has_header* is True.
    """
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        if has_header:
            next(reader, None)  # consume header without counting it
        return sum(1 for _ in reader)


def find_csvs(directory: Path) -> list[Path]:
    """Return all CSV files found directly under *directory*."""
    return list(directory.glob("*.csv"))
