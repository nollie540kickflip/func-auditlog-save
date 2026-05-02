import json
import logging

from azure.storage.blob import BlobServiceClient, ContentSettings


class BlobStorageClient:
    """Azure Blob Storageへのアクセスを担当するクラス"""

    def __init__(self, connection_string: str, container_name: str):
        logging.info("Initializing BlobStorageClient...")
        if not connection_string:
            raise ValueError(
                "Blob Storageの接続文字列(connection_string)が設定されていません。"
            )

        self.container_name = container_name

        # 接続文字列を使用してBlobServiceClientを初期化
        self.blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )

        # 初期化時にコンテナの存在確認と作成を行う
        self._ensure_container_exists()

    def _ensure_container_exists(self) -> None:
        """コンテナが存在しない場合は自動的に作成する内部メソッド"""
        try:
            container_client = self.blob_service_client.get_container_client(
                self.container_name
            )
            if not container_client.exists():
                container_client.create_container()
                logging.info(
                    f"Blob Storage: Created new container '{self.container_name}'."
                )
        except Exception as e:
            # 権限不足などで作成確認に失敗した場合でも、後続の処理を止めないようにwarningに留める
            logging.warning(
                f"Blob Storage: Could not verify or create container '{self.container_name}'. Error: {e}"
            )

    def append_jsonl(self, blob_name: str, records: list) -> None:
        """
        リスト内の辞書データを JSON Lines (JSONL) 形式に変換し、
        Append Blob に追記する。巨大ログのストリーミング保存に最適。
        """
        logging.info(
            f"Appending {len(records)} records to blob '{blob_name}' in container '{self.container_name}'..."
        )
        try:
            # 汎用クライアントを取得
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=blob_name
            )

            # 初回のみBlobを作成 (存在しない場合のみ)
            if not blob_client.exists():
                content_settings = ContentSettings(content_type="application/jsonl")
                # create_append_blob で明示的に Append Blob として初期化する
                blob_client.create_append_blob(content_settings=content_settings)
                logging.info(f"Blob Storage: Created new Append Blob '{blob_name}'.")

            # リスト内の各レコードを1行のJSON文字列に変換 (改行区切り)
            jsonl_data = ""
            for record in records:
                jsonl_data += json.dumps(record, ensure_ascii=False) + "\n"

            # 追記実行
            if jsonl_data:
                blob_client.append_block(jsonl_data.encode("utf-8"))
                logging.info(
                    f"Blob Storage: Appended {len(records)} records to '{blob_name}'."
                )

        except Exception as e:
            logging.error(
                f"Blob Storage: Failed to append to '{blob_name}'. Error: {e}"
            )
            raise
