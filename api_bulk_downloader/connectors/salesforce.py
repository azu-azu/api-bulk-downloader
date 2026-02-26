"""
Salesforce Bulk API 2.0 connector — placeholder.

When implemented this connector will:
1. Authenticate via OAuth 2.0 (username-password or JWT bearer flow).
2. Create a Bulk API 2.0 query job.
3. Poll the job until it reaches "JobComplete" state.
4. Return the results endpoint so the core downloader can stream the CSV.

Only the interface skeleton is provided here to keep the architecture
consistent with the WorldBankConnector.
"""
import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class SalesforceConnector:
    """
    Placeholder connector for Salesforce Bulk API 2.0.

    Parameters
    ----------
    instance_url:
        Salesforce instance base URL, e.g. ``"https://myorg.my.salesforce.com"``.
    access_token:
        OAuth 2.0 bearer token obtained from the token endpoint.
    soql:
        SOQL query to execute, e.g. ``"SELECT Id, Name FROM Account"``.
    """

    instance_url: str
    access_token: str
    soql: str

    # ------------------------------------------------------------------
    # ConnectorProtocol interface
    # ------------------------------------------------------------------

    @property
    def download_url(self) -> str:
        """
        Return the Bulk API 2.0 results URL.

        TODO: implement the full job-creation → polling → results pipeline.
        """
        raise NotImplementedError(
            "SalesforceConnector is not yet implemented. "
            "Complete the OAuth flow and job lifecycle to enable this connector."
        )

    @property
    def request_headers(self) -> dict[str, str]:
        """Bearer token auth header for Salesforce REST / Bulk API."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Future helpers (stubs)
    # ------------------------------------------------------------------

    def _authenticate(self) -> str:
        """Exchange credentials for an access token. (not implemented)"""
        raise NotImplementedError

    def _create_query_job(self) -> str:
        """Submit a Bulk API 2.0 query job and return the job ID. (not implemented)"""
        raise NotImplementedError

    def _poll_until_complete(self, job_id: str) -> None:
        """Block until the job reaches JobComplete or fails. (not implemented)"""
        raise NotImplementedError

    def _get_results_url(self, job_id: str) -> str:
        """Build the results download URL for a completed job. (not implemented)"""
        raise NotImplementedError
