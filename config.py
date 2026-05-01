import os


class AppConfig:
    """アプリケーション全体の設定を管理するクラス"""

    @property
    def polling_interval_seconds(self) -> int:
        return int(os.environ.get("POLLING_INTERVAL_SECONDS"))

    @property
    def polling_timeout_hours(self) -> int:
        return int(os.environ.get("POLLING_TIMEOUT_HOURS"))

    @property
    def blob_container_name(self) -> str:
        return os.environ.get("BLOB_CONTAINER_NAME")

    @property
    def blob_connection_string(self) -> str:
        return os.environ.get("BLOB_CONNECTION_STRING")

    @property
    def tenant_id(self) -> str:
        return os.environ.get("TENANT_ID", "")

    @property
    def client_id(self) -> str:
        return os.environ.get("CLIENT_ID", "")

    @property
    def client_secret(self) -> str:
        return os.environ.get("CLIENT_SECRET")


# 設定のシングルトンインスタンス
config = AppConfig()
