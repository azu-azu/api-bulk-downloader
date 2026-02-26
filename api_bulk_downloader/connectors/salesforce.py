"""
Salesforce Bulk API 2.0 コネクタ — プレースホルダー。

実装時にはこのコネクタが以下を担う:
1. OAuth 2.0 で認証する（ユーザー名・パスワードフローまたはJWTベアラーフロー）。
2. Bulk API 2.0 クエリジョブを作成する。
3. ジョブが "JobComplete" 状態になるまでポーリングする。
4. コアダウンローダーがCSVをストリーム取得できるよう結果エンドポイントを返す。

WorldBankConnector とアーキテクチャを統一するため、
ここではインターフェースの骨格のみを提供する。
"""
import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass
class SalesforceConnector:
    """
    Salesforce Bulk API 2.0 のプレースホルダーコネクタ。

    Parameters
    ----------
    instance_url:
        Salesforce インスタンスのベースURL。
        例: ``"https://myorg.my.salesforce.com"``
    access_token:
        トークンエンドポイントから取得したOAuth 2.0ベアラートークン。
    soql:
        実行するSOQLクエリ。例: ``"SELECT Id, Name FROM Account"``
    """

    instance_url: str
    access_token: str
    soql: str

    # ------------------------------------------------------------------
    # ConnectorProtocol インターフェース
    # ------------------------------------------------------------------

    @property
    def download_url(self) -> str:
        """
        Bulk API 2.0 の結果取得URLを返す。

        TODO: ジョブ作成 → ポーリング → 結果取得 のパイプラインを実装する。
        """
        raise NotImplementedError(
            "SalesforceConnector は未実装です。"
            "OAuthフローとジョブライフサイクルを実装してからこのコネクタを使用してください。"
        )

    @property
    def request_headers(self) -> dict[str, str]:
        """Salesforce REST / Bulk API 用のベアラートークン認証ヘッダ。"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # 将来実装するヘルパー（スタブ）
    # ------------------------------------------------------------------

    def _authenticate(self) -> str:
        """認証情報をアクセストークンに交換する。（未実装）"""
        raise NotImplementedError

    def _create_query_job(self) -> str:
        """Bulk API 2.0 クエリジョブを送信してジョブIDを返す。（未実装）"""
        raise NotImplementedError

    def _poll_until_complete(self, job_id: str) -> None:
        """ジョブが JobComplete または失敗状態になるまでブロックする。（未実装）"""
        raise NotImplementedError

    def _get_results_url(self, job_id: str) -> str:
        """完了したジョブの結果ダウンロードURLを組み立てる。（未実装）"""
        raise NotImplementedError
