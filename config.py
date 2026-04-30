import os


class AppConfig:
    """アプリケーション全体の設定を管理するクラス"""

    @property
    def polling_interval_seconds(self) -> int:
        return int(os.environ.get("POLLING_INTERVAL_SECONDS", "60"))

    @property
    def polling_timeout_hours(self) -> int:
        return int(os.environ.get("POLLING_TIMEOUT_HOURS", "2"))

    @property
    def blob_container_name(self) -> str:
        return os.environ.get("BLOB_CONTAINER_NAME", "auditlogs")

    @property
    def blob_connection_string(self) -> str:
        return os.environ.get("AzureWebJobsStorage", "")

    @property
    def tenant_id(self) -> str:
        return os.environ.get("TENANT_ID", "")

    @property
    def client_id(self) -> str:
        return os.environ.get("CLIENT_ID", "")

    @property
    def client_secret(self) -> str:
        # ※本番環境ではKey Vaultから取得することを推奨します
        return os.environ.get("CLIENT_SECRET", "")


# 設定のシングルトンインスタンス
config = AppConfig()
