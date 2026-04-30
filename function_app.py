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
    target_date_str = req.params.get("date")

    if not target_date_str:
        return func.HttpResponse(
            "Please pass a 'date' parameter (YYYY-MM-DD)", status_code=400
        )

    instance_id = await client.start_new("main_orchestrator", None, target_date_str)
    return client.create_check_status_response(req, instance_id)


@myApp.orchestration_trigger(context_name="context")
def main_orchestrator(context: df.DurableOrchestrationContext):
    target_date_str = context.get_input()
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    tasks = []

    for i in range(24):
        start_time = target_date + timedelta(hours=i)
        time_window = {
            "start": start_time.isoformat() + "Z",
            "end": (start_time + timedelta(hours=1)).isoformat() + "Z",
        }
        tasks.append(
            context.call_sub_orchestrator("job_lifecycle_sub_orchestrator", time_window)
        )

    results = yield context.task_all(tasks)
    return f"Completed. {len(results)} jobs processed."


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

    # ログ取得
    log_data = yield context.call_activity_with_retry(
        "fetch_logs_activity", retry_options, job_id
    )

    # Blob保存
    blob_params = {
        "blob_name": f"audit_logs_{time_window['start'].replace(':', '')}.json",
        "log_data": log_data,
    }
    yield context.call_activity("save_to_blob_activity", blob_params)

    return f"Success for window {time_window['start']}"


# --- Activity Functions ---
@myApp.activity_trigger(input_name="timeWindow")
def start_search_job_activity(timeWindow: dict) -> str:
    return graph_client.start_search_job(timeWindow["start"], timeWindow["end"])


@myApp.activity_trigger(input_name="jobId")
def check_job_status_activity(jobId: str) -> str:
    return graph_client.get_job_status(jobId)


@myApp.activity_trigger(input_name="jobId")
def fetch_logs_activity(jobId: str) -> dict:
    return graph_client.fetch_logs(jobId)


@myApp.activity_trigger(input_name="blobParams")
def save_to_blob_activity(blobParams: dict) -> str:
    blob_client.save_json(blobParams["blob_name"], blobParams["log_data"])
    return blobParams["blob_name"]
