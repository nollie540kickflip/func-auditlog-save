from datetime import datetime, timedelta

import azure.durable_functions as df
import azure.functions as func

from config import config
from infrastructure.blob_client import BlobStorageClient
from infrastructure.graph_client import GraphApiClient

myApp = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

# クライアントのインスタンス化
graph_client = GraphApiClient(
    tenant_id=config.tenant_id,
    client_id=config.client_id,
    client_secret=config.client_secret,
)
blob_client = BlobStorageClient(
    config.blob_connection_string, config.blob_container_name
)


# --- HTTP Starter & Main Orchestrator ---
@myApp.route(route="start_audit_sync")
@myApp.durable_client_input(client_name="client")
async def http_starter(
    req: func.HttpRequest, client: df.DurableOrchestrationClient
) -> func.HttpResponse:
    # クエリパラメータから対象日付を取得 (例: ?date=2026-05-01)
    target_date_str = req.params.get("date")

    # クエリパラメータに日付がない場合は、リクエストボディから取得を試みる
    if not target_date_str:
        try:
            req_body = req.get_json()
            target_date_str = req_body.get("date")
        except ValueError:
            # JSON形式ではない、またはボディが空の場合は何もしない
            pass

    if not target_date_str:
        return func.HttpResponse(
            "Please provide a target date in 'YYYY-MM-DD' format via query parameter or JSON body.",
            status_code=400,
        )

    instance_id = await client.start_new("main_orchestrator", None, target_date_str)
    return client.create_check_status_response(req, instance_id)


@myApp.orchestration_trigger(context_name="context")
def main_orchestrator(context: df.DurableOrchestrationContext):
    target_date_str = context.get_input()
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    results = []

    for i in range(24):
        start_time = target_date + timedelta(hours=i)
        time_window = {
            "start": start_time.isoformat() + "Z",
            "end": (start_time + timedelta(hours=1)).isoformat() + "Z",
        }

        # forループの中で1つずつyieldして完了を待つ (直列実行)
        result = yield context.call_sub_orchestrator(
            "job_lifecycle_sub_orchestrator", time_window
        )
        results.append(result)

        if i < 23:  # 最後のジョブの後は待つ必要がないためスキップ
            delay_minutes = 3
            next_start_time = context.current_utc_datetime + timedelta(
                minutes=delay_minutes
            )
            yield context.create_timer(next_start_time)

    return f"Completed. {len(results)} jobs processed sequentially."


# --- Sub Orchestrator ---
@myApp.orchestration_trigger(context_name="context")
def job_lifecycle_sub_orchestrator(context: df.DurableOrchestrationContext):
    time_window = context.get_input()

    # --- リトライポリシーの定義 ---
    # 初回は5秒後、最大3回まで再試行する設定
    retry_options = df.RetryOptions(
        first_retry_interval_in_milliseconds=5000, max_number_of_attempts=3
    )

    # 検索ジョブの実行
    job_id = yield context.call_activity_with_retry(
        "start_search_job_activity", retry_options, time_window
    )

    # 検索ジョブの完了待機
    expiry_time = context.current_utc_datetime + timedelta(
        hours=config.polling_timeout_hours
    )
    while context.current_utc_datetime < expiry_time:
        job_status = yield context.call_activity_with_retry(
            "check_job_status_activity", retry_options, job_id
        )
        status_lower = str(job_status).lower()

        if status_lower == "succeeded":
            break
        elif status_lower in ["failed", "cancelled", "unknownfuturevalue"]:
            return f"Job {job_id} failed."

        next_check = context.current_utc_datetime + timedelta(
            seconds=config.polling_interval_seconds
        )
        yield context.create_timer(next_check)

    # ログの取得とBlobへの追記保存
    fetch_and_save_params = {
        "job_id": job_id,
        "blob_name": f"audit_logs_{time_window['start'].replace(':', '')}.jsonl",
    }
    result_msg = yield context.call_activity_with_retry(
        "fetch_and_save_logs_activity", retry_options, fetch_and_save_params
    )

    return f"Success for window {time_window['start']}. {result_msg}"


# --- Activity Functions ---
@myApp.activity_trigger(input_name="timeWindow")
def start_search_job_activity(timeWindow: dict) -> str:
    return graph_client.start_search_job(timeWindow["start"], timeWindow["end"])


@myApp.activity_trigger(input_name="jobId")
def check_job_status_activity(jobId: str) -> str:
    return graph_client.get_job_status(jobId)


@myApp.activity_trigger(input_name="params")
def fetch_and_save_logs_activity(params: dict) -> str:
    job_id = params["job_id"]
    blob_name = params["blob_name"]

    total_records = 0
    buffer = []
    BUFFER_LIMIT = 1000

    # Graph APIからジェネレータで1ページ(複数レコード)ずつ取得
    for page_records in graph_client.fetch_logs_pages(job_id):
        if page_records:
            buffer.extend(page_records)
            total_records += len(page_records)

            # バッファが1000件以上になったら、1000件ずつ切り出して書き込み
            while len(buffer) >= BUFFER_LIMIT:
                chunk_to_write = buffer[:BUFFER_LIMIT]
                blob_client.append_jsonl(blob_name, chunk_to_write)
                # 書き込んだ分をバッファから削除
                buffer = buffer[BUFFER_LIMIT:]

    # ループ終了後、バッファに残っている端数のレコードを書き込み
    if buffer:
        blob_client.append_jsonl(blob_name, buffer)

    return f"Saved total {total_records} records to {blob_name} (in chunks of {BUFFER_LIMIT})"
