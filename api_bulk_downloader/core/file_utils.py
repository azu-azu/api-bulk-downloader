"""
ファイル操作ユーティリティ: ストリーミング書き込み・ZIP展開・行数カウント。
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
    HTTPストリーミングレスポンスをメモリに全展開せず *dest* へ書き込む。

    書き込んだ総バイト数を返す。
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with dest.open("wb") as fh:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:  # キープアライブの空チャンクを除外
                fh.write(chunk)
                total += len(chunk)
    log.debug("Wrote %d bytes to %s", total, dest)
    return total


def extract_zip(zip_path: Path, dest_dir: Path) -> list[Path]:
    """
    ZIPアーカイブの全メンバーを *dest_dir* へ展開する。

    展開したファイルパスのリストを返す。
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
    """*path* が有効なZIPファイルであれば True を返す。"""
    return zipfile.is_zipfile(path)


def count_csv_rows(path: Path, has_header: bool = True) -> int:
    """
    CSVファイルのデータ行数を数える。

    *has_header* が True のときはヘッダ行をスキップしてカウントする。
    """
    with path.open(newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        if has_header:
            next(reader, None)  # ヘッダをカウントせず読み飛ばす
        return sum(1 for _ in reader)


def find_csvs(directory: Path) -> list[Path]:
    """*directory* 直下にあるCSVファイルをすべて返す。"""
    return list(directory.glob("*.csv"))
