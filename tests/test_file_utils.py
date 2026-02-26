"""
file_utils.choose_primary_csv() の単体テスト。

カバーするケース:
  1. API_ / Metadata_ 混在  → API_ を選ぶ
  2. API_ が複数          → 最大サイズの API_ を選ぶ
  3. API_ なし            → Metadata_ 以外を選ぶ
  4. 全て Metadata_       → 最大サイズを選ぶ（Rule 3）
  5. 候補が 1 件          → それを返す
  6. 空リスト            → ValueError
"""
import pytest

from api_bulk_downloader.core.file_utils import choose_primary_csv


def make_csv(tmp_path, name: str, size: int):
    """指定サイズのダミーCSVファイルを作成して Path を返す。"""
    p = tmp_path / name
    p.write_bytes(b"x" * size)
    return p


# ---------------------------------------------------------------------------
# ケース 1: API_ / Metadata_ 混在 → API_ を選ぶ
# ---------------------------------------------------------------------------

def test_api_preferred_over_metadata(tmp_path):
    api   = make_csv(tmp_path, "API_NY.GDP.MKTP.CD_DS2_en_csv_v2.csv", 1000)
    meta1 = make_csv(tmp_path, "Metadata_Country.csv",                   500)
    meta2 = make_csv(tmp_path, "Metadata_Indicator.csv",                 200)

    assert choose_primary_csv([meta1, api, meta2]) == api


# ---------------------------------------------------------------------------
# ケース 2: API_ が複数 → 最大サイズを選ぶ
# ---------------------------------------------------------------------------

def test_multiple_api_files_returns_largest(tmp_path):
    small = make_csv(tmp_path, "API_small.csv",  500)
    large = make_csv(tmp_path, "API_large.csv", 2000)
    other = make_csv(tmp_path, "API_mid.csv",   1000)

    assert choose_primary_csv([small, large, other]) == large


# ---------------------------------------------------------------------------
# ケース 3: API_ なし → Metadata_ 以外を選ぶ
# ---------------------------------------------------------------------------

def test_non_metadata_preferred_when_no_api(tmp_path):
    data = make_csv(tmp_path, "WDI_data.csv",        1500)
    meta = make_csv(tmp_path, "Metadata_Country.csv",  300)

    assert choose_primary_csv([meta, data]) == data


# ---------------------------------------------------------------------------
# ケース 4: 全て Metadata_ → 最大サイズを選ぶ（Rule 3 フォールバック）
# ---------------------------------------------------------------------------

def test_all_metadata_returns_largest(tmp_path):
    small = make_csv(tmp_path, "Metadata_Country.csv",   100)
    large = make_csv(tmp_path, "Metadata_Indicator.csv", 800)

    assert choose_primary_csv([small, large]) == large


# ---------------------------------------------------------------------------
# ケース 5: 候補 1 件 → それを返す
# ---------------------------------------------------------------------------

def test_single_candidate_returned(tmp_path):
    only = make_csv(tmp_path, "data.csv", 100)

    assert choose_primary_csv([only]) == only


# ---------------------------------------------------------------------------
# ケース 6: 空リスト → ValueError
# ---------------------------------------------------------------------------

def test_empty_list_raises(tmp_path):
    with pytest.raises(ValueError):
        choose_primary_csv([])
