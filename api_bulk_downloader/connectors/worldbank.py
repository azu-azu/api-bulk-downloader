"""
World Bank Open Data コネクタ。

2種類のコネクタを提供する:

* ``WorldBankConnector``     — 単一指標のダウンロード（1件あたり約100KB〜1MB）。
* ``WorldBankWDIConnector``  — World Development Indicators 全量バルクZIP
                               （圧縮約50MB・解凍約500MB）。
                               大ファイルのストリーミング練習にはこちらを使う。

いずれのエンドポイントも認証不要。

参考:
    https://datahelpdesk.worldbank.org/knowledgebase/articles/898581
"""
import logging
from dataclasses import dataclass, field
from urllib.parse import urlencode, quote

log = logging.getLogger(__name__)

_WB_INDICATOR_BASE = "https://api.worldbank.org/v2/en/indicator"

# WDI全量バルクダウンロード — 全指標 × 全国 × 全年を1つのZIPで提供。
# 圧縮: 約50MB  |  解凍後: 約500MB
_WDI_BULK_URL = (
    "https://databankfiles.worldbank.org/public/ddpext_download/WDI_CSV.zip"
)


@dataclass
class WorldBankConnector:
    """
    単一のWorld Bank指標をバルクCSVでダウンロードするコネクタ。

    簡易テスト向け。大ファイルのストリーミング練習には
    :class:`WorldBankWDIConnector` を使うこと。

    Parameters
    ----------
    indicator:
        World Bank指標コード。例: ``"NY.GDP.MKTP.CD"``（GDP・米ドル現在価格）。
    extra_params:
        URLに追加するクエリパラメータ（省略可）。
        例: ``{"mrv": "10"}`` で直近10件のみ取得。
    """

    indicator: str
    extra_params: dict[str, str] = field(default_factory=dict)

    @property
    def download_url(self) -> str:
        """
        単一指標のバルクCSVダウンロードURLを構築する。

        指標データと2つのメタデータCSVを含むZIPアーカイブが返される。
        """
        params = {"downloadformat": "csv", **self.extra_params}
        url = f"{_WB_INDICATOR_BASE}/{quote(self.indicator, safe='')}?{urlencode(params)}"
        log.debug("World Bank indicator URL: %s", url)
        return url

    @property
    def request_headers(self) -> dict[str, str]:
        return {"Accept": "application/zip"}

    def suggested_filename(self) -> str:
        safe = self.indicator.replace(".", "_")
        return f"worldbank_{safe}.zip"


@dataclass
class WorldBankWDIConnector:
    """
    World Development Indicators（WDI）全量バルクダウンロードコネクタ。

    全指標・全国・全年のデータを1つのZIPでダウンロードする。
    圧縮約50MB・解凍約500MB と大きく、ストリーミングやチャンク書き込み、
    大規模CSV処理の練習に最適。

    URLはWorld Bankが固定しているためパラメータ不要。
    """

    @property
    def download_url(self) -> str:
        log.debug("World Bank WDI bulk URL: %s", _WDI_BULK_URL)
        return _WDI_BULK_URL

    @property
    def request_headers(self) -> dict[str, str]:
        return {"Accept": "application/zip"}

    def suggested_filename(self) -> str:
        return "worldbank_WDI.zip"
