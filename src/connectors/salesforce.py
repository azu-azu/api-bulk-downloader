from __future__ import annotations

from typing import Any

from src.connectors.protocol import DiscoveryResult
from src.exceptions import ConnectorError


class SalesforceReportConnector:
    """Stub connector — raises NotImplementedError until implemented."""

    def __init__(self, report_id: str, **kwargs: Any) -> None:
        self.report_id = report_id

    def discover(self, job: object) -> DiscoveryResult:
        raise NotImplementedError(
            f"SalesforceReportConnector is not yet implemented "
            f"(report_id={self.report_id!r})"
        )

    def materialize(self, job: object, conn: Any) -> None:
        raise NotImplementedError(
            f"SalesforceReportConnector is not yet implemented "
            f"(report_id={self.report_id!r})"
        )
