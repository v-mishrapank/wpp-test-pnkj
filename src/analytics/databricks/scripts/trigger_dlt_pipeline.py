# trigger_dlt_pipeline.py
# Upload this to DBFS: dbfs:/FileStore/scripts/trigger_dlt_pipeline.py
# Called by ADF to trigger DLT pipeline after ACA Jobs complete

import sys
import requests
import time


def trigger_pipeline(pipeline_id: str, full_refresh: bool = False):
    """
    Trigger a DLT pipeline and wait for completion.
    """
    # Get workspace context
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # Get API credentials from Spark context
    ctx = spark.sparkContext._jvm.com.databricks.backend.daemon.driver.DriverDaemon.singleton()
    workspace_url = ctx.getNotebookContext().apiUrl().get()
    token = ctx.getNotebookContext().apiToken().get()

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Trigger pipeline update
    print(f"Triggering DLT pipeline: {pipeline_id}")
    response = requests.post(
        f"{workspace_url}/api/2.0/pipelines/{pipeline_id}/updates", headers=headers, json={"full_refresh": full_refresh}
    )

    if response.status_code != 200:
        raise Exception(f"Failed to trigger pipeline: {response.text}")

    update_id = response.json().get("update_id")
    print(f"Pipeline update started: {update_id}")

    # Poll for completion
    max_wait_minutes = 120
    poll_interval_seconds = 30
    elapsed_seconds = 0

    while elapsed_seconds < max_wait_minutes * 60:
        time.sleep(poll_interval_seconds)
        elapsed_seconds += poll_interval_seconds

        # Check status
        response = requests.get(f"{workspace_url}/api/2.0/pipelines/{pipeline_id}/updates/{update_id}", headers=headers)

        if response.status_code != 200:
            print(f"Warning: Failed to get update status: {response.text}")
            continue

        state = response.json().get("update", {}).get("state")
        print(f"Pipeline state: {state} ({elapsed_seconds}s elapsed)")

        if state == "COMPLETED":
            print("Pipeline completed successfully!")
            return True
        elif state in ["FAILED", "CANCELED"]:
            raise Exception(f"Pipeline {state}")

    raise Exception(f"Pipeline timed out after {max_wait_minutes} minutes")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: trigger_dlt_pipeline.py <pipeline_id> [full_refresh]")
        sys.exit(1)

    pipeline_id = sys.argv[1]
    full_refresh = len(sys.argv) > 2 and sys.argv[2].lower() == "true"

    trigger_pipeline(pipeline_id, full_refresh)
