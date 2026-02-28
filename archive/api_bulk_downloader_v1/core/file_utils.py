"""
ファイル操作ユーティリティ: ストリーミング書き込み・ZIP展開・行数カウント。
"""
import csv
import logging
import zipfile
from pathlib import Path

import requests

log = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE: int = 8 * 1024  # 8 KB


def stream_to_file(
    response: requests.Response,
    dest: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> int:
    dest.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with dest.open("wb") as fh:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                fh.write(chunk)
                total += len(chunk)
    log.debug("Wrote %d bytes to %s", total, dest)
    return total


def extract_zip(zip_path: Path, dest_dir: Path) -> list[Path]:
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
    return zipfile.is_zipfile(path)


def count_csv_rows(path: Path, has_header: bool = True) -> int:
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        if has_header:
            next(reader, None)
        return sum(1 for _ in reader)


def choose_primary_csv(candidates: list[Path]) -> Path:
    """CSVリストから主要なデータファイルを選ぶ。

    優先順: API_ で始まる最大サイズ → API_ 以外の最大サイズ → それ以外の最大サイズ
    """
    if not candidates:
        raise ValueError("No CSV candidates provided.")
    api_files = [p for p in candidates if p.name.startswith("API_")]
    if api_files:
        return max(api_files, key=lambda p: p.stat().st_size)
    non_meta = [p for p in candidates if not p.name.startswith("Metadata")]
    if non_meta:
        return max(non_meta, key=lambda p: p.stat().st_size)
    return max(candidates, key=lambda p: p.stat().st_size)
