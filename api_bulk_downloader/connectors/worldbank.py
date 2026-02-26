"""
World Bank Open Data connectors.

Two connectors are provided:

* ``WorldBankConnector``     — single indicator download (~100 KB–1 MB each).
* ``WorldBankWDIConnector``  — full World Development Indicators bulk ZIP
                               (~50 MB compressed / ~500 MB uncompressed).
                               Use this one for large-file streaming practice.

Authentication is not required for either endpoint.

Reference:
    https://datahelpdesk.worldbank.org/knowledgebase/articles/898581
"""
import logging
from dataclasses import dataclass, field
from urllib.parse import urlencode, quote

log = logging.getLogger(__name__)

_WB_INDICATOR_BASE = "https://api.worldbank.org/v2/en/indicator"

# Full WDI bulk download — all indicators × all countries × all years in one ZIP.
# Compressed: ~50 MB  |  Uncompressed: ~500 MB
_WDI_BULK_URL = (
    "https://databankfiles.worldbank.org/public/ddpext_download/WDI_CSV.zip"
)


@dataclass
class WorldBankConnector:
    """
    Connector for a single World Bank indicator (bulk CSV download).

    Suitable for quick tests. For large-file streaming practice use
    :class:`WorldBankWDIConnector` instead.

    Parameters
    ----------
    indicator:
        World Bank indicator code, e.g. ``"NY.GDP.MKTP.CD"`` (GDP current USD).
    extra_params:
        Optional query-string parameters appended to the URL
        (e.g. ``{"mrv": "10"}`` for most-recent 10 values).
    """

    indicator: str
    extra_params: dict[str, str] = field(default_factory=dict)

    @property
    def download_url(self) -> str:
        """
        Construct the World Bank bulk CSV download URL for a single indicator.

        Returns a ZIP archive with the indicator data and two metadata CSVs.
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
    Connector for the full World Development Indicators (WDI) bulk download.

    Downloads ALL indicators for ALL countries and years in a single ZIP.
    Compressed size is ~50 MB; uncompressed ~500 MB — making it ideal for
    practising streaming, chunked writes, and large CSV processing.

    No parameters required; the URL is fixed by the World Bank.
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
