"""
World Bank Open Data コネクタ。
"""
import logging
from dataclasses import dataclass, field
from urllib.parse import urlencode, quote

log = logging.getLogger(__name__)

_WB_INDICATOR_BASE = "https://api.worldbank.org/v2/en/indicator"
_WDI_BULK_URL = "https://databankfiles.worldbank.org/public/ddpext_download/WDI_CSV.zip"


@dataclass
class WorldBankConnector:
    """指定したインジケーターを CSV/ZIP でダウンロードするコネクタ。"""

    indicator: str
    extra_params: dict[str, str] = field(default_factory=dict)

    @property
    def download_url(self) -> str:
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
    """WDI 全量バルク ZIP をダウンロードするコネクタ。"""

    @property
    def download_url(self) -> str:
        log.debug("World Bank WDI bulk URL: %s", _WDI_BULK_URL)
        return _WDI_BULK_URL

    @property
    def request_headers(self) -> dict[str, str]:
        return {"Accept": "application/zip"}

    def suggested_filename(self) -> str:
        return "worldbank_WDI.zip"
