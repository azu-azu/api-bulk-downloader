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


def choose_primary_csv(csv_paths: list[Path]) -> Path:
    """
    CSV候補リストからデータ本体ファイルを選んで返す。

    優先順位:
      1. ``API_`` で始まるファイル（World Bank データ本体）
      2. ``Metadata_`` で始まらないファイル
      3. 最大ファイルサイズ（同順位の場合の最終フォールバック）

    Parameters
    ----------
    csv_paths:
        候補CSVファイルのリスト（空不可）。

    Raises
    ------
    ValueError
        *csv_paths* が空のとき。
    """
    if not csv_paths:
        raise ValueError("csv_paths must not be empty")

    # Rule 1: API_ プレフィックス
    api_csvs = [p for p in csv_paths if p.name.startswith("API_")]
    if api_csvs:
        return max(api_csvs, key=lambda p: p.stat().st_size)

    # Rule 2: Metadata_ 以外
    non_meta = [p for p in csv_paths if not p.name.startswith("Metadata_")]
    if non_meta:
        return max(non_meta, key=lambda p: p.stat().st_size)

    # Rule 3: 最大ファイルサイズ（全てMetadata_の場合）
    return max(csv_paths, key=lambda p: p.stat().st_size)
