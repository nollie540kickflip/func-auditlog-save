import json
import logging

from azure.storage.blob import BlobServiceClient, ContentSettings


class BlobStorageClient:
    """Azure Blob Storageへのアクセスを担当するクラス"""

    def __init__(self, connection_string: str, container_name: str):
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

    def save_json(self, blob_name: str, data: dict) -> None:
        """
        JSONデータをBlobストレージに保存する

        :param blob_name: 保存するファイル名 (例: 'audit_logs_20260501T000000Z.json')
        :param data: 保存する辞書形式のデータ
        """
        logging.info(
            f"Blob Storage: Saving '{blob_name}' to container '{self.container_name}'."
        )

        try:
            # 操作対象のBlobクライアントを取得
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=blob_name
            )

            # Pythonの辞書(dict)をJSON文字列に変換
            # ensure_ascii=False: 日本語などのマルチバイト文字をエスケープせずに保存する
            # indent=2: ログを人間が読みやすいように整形（ストレージ容量を極限まで削りたい場合は外してください）
            json_str = json.dumps(data, ensure_ascii=False, indent=2)

            # Azureポータル等でプレビュー・ダウンロードした際にJSONとして認識されるようメタデータを設定
            content_settings = ContentSettings(content_type="application/json")

            # 文字列をUTF-8のバイト列にエンコードしてアップロード
            # overwrite=True: 同名のファイルが存在する場合は上書きする（冪等性の担保）
            blob_client.upload_blob(
                data=json_str.encode("utf-8"),
                overwrite=True,
                content_settings=content_settings,
            )

            logging.info(f"Blob Storage: Successfully saved '{blob_name}'.")

        except Exception as e:
            logging.error(f"Blob Storage: Failed to save '{blob_name}'. Error: {e}")
            raise  # 例外を再スローしてDurable Functions側に失敗を検知させる
