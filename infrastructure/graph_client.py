import logging

import msal
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, RetryError
from urllib3.util.retry import (
    Retry,
)


class GraphApiClient:
    """Microsoft Graph APIへのアクセスを担当するクラス"""

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self.client_id = client_id
        self.authority = f"https://login.microsoftonline.com/{tenant_id}"
        self.scopes = ["https://graph.microsoft.com/.default"]

        # MSALの機密クライアントアプリケーションを初期化
        self.app = msal.ConfidentialClientApplication(
            self.client_id, authority=self.authority, client_credential=client_secret
        )

        # --- HTTPセッションと自動リトライの設定を追加 ---
        self.session = requests.Session()

        # 429(レート制限), 500, 502, 503, 504エラー時に自動リトライ
        # backoff_factor=2 の場合: 2s, 4s, 8s, 16s, 32s... と待機時間が増加
        # ※ 429エラー時にRetry-Afterヘッダーがあれば、それを優先して待機してくれます
        retries = Retry(
            total=10,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=[
                "HEAD",
                "GET",
                "OPTIONS",
                "POST",
            ],
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _get_access_token(self) -> str:
        """MSALを使用してアクセストークンを取得する"""
        # キャッシュからトークンを取得
        result = self.app.acquire_token_for_client(scopes=self.scopes)

        if "access_token" in result:
            return result["access_token"]
        else:
            error_desc = result.get("error_description", "Unknown error")
            logging.error(f"Failed to acquire token: {error_desc}")
            raise Exception(f"Authentication failed: {error_desc}")

    def _get_headers(self) -> dict:
        """APIリクエスト用の共通ヘッダーを生成する"""
        token = self._get_access_token()
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def start_search_job(self, start_time: str, end_time: str) -> str:
        """検索ジョブを開始し、Job IDを返す"""
        logging.info(f"Graph API: Starting search from {start_time} to {end_time}")

        # 監査ログ検索用エンドポイント
        url = "https://graph.microsoft.com/beta/security/auditLog/queries"
        headers = self._get_headers()

        # 検索条件
        payload = {"filterStartDateTime": start_time, "filterEndDateTime": end_time}

        try:
            response = self.session.post(url, headers=headers, json=payload)
            response.raise_for_status()
            return response.json().get("id")
        except RetryError as e:
            # HTTPセッションのリトライ上限に達した場合
            logging.error(f"Graph API HTTP Retry Limit Exceeded: {e}")
            raise Exception(
                "Graph API Rate Limit (429) persists. Need orchestrator retry."
            )
        except HTTPError as e:
            logging.error(f"Graph API HTTP Error: {e}")
            raise

    def get_job_status(self, job_id: str) -> str:
        """ジョブのステータスを取得する"""
        logging.info(f"Graph API: Checking status for {job_id}")

        # ステータス取得用エンドポイント
        url = f"https://graph.microsoft.com/beta/security/auditLog/queries/{job_id}"
        headers = self._get_headers()

        # GETリクエスト
        response = self.session.get(url, headers=headers)
        response.raise_for_status()

        return response.json().get("status")

    def fetch_logs_pages(self, job_id: str):
        """
        完了したジョブからログをページ単位で取得するジェネレータ。
        大量データをメモリに溜め込まず、1ページ(value配列)ごとに yield で返す。
        """
        logging.info(f"Graph API: Fetching logs for {job_id} (Paging mode)")

        # ログ取得用エンドポイント
        base_url = f"https://graph.microsoft.com/beta/security/auditLog/queries/{job_id}/records"
        headers = self._get_headers()
        next_link = base_url

        page_count = 1
        while next_link:
            logging.info(f"Fetching page {page_count} from: {next_link}")

            try:
                response = self.session.get(next_link, headers=headers)
                response.raise_for_status()
            except RetryError as e:
                logging.error(
                    f"Graph API HTTP Retry Limit Exceeded on page {page_count}: {e}"
                )
                raise Exception(
                    f"Failed to fetch page {page_count} due to persistent 429 errors."
                )

            data = response.json()
            records = data.get("value", [])

            if records:
                yield records

            next_link = data.get("@odata.nextLink")
            page_count += 1
