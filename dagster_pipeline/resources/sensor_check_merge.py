import os
import pandas as pd
from typing import Union, List
from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
    define_asset_job,
    DefaultSensorStatus,
    RunStatusSensorContext,
    run_status_sensor,
    DagsterRunStatus,
    get_dagster_logger,
    AssetMaterialization
)
from lakefs_spec import LakeFSFileSystem
import time

# Import the asset so Dagster recognizes it in the asset graph
from dagster_pipeline.assets.predict_percent import predict_percent

# Define the jobs

test_job = define_asset_job(
    name="test_job",
    selection=["load_data", "prepare_data", "predict_percent"]
)
retrain_job = define_asset_job(
    name="retrain_job",
    selection=["load_data", "prepare_data", "split_data", "preprocess", "train_XGBC", "predict_asset_act"]
)
deploy_job = define_asset_job(
    name="deploy_job",
    selection=["evaluate_and_deploy_model", "serve_model"]
)

# Helper: store state in instance storage (or use context.cursor for simple state)

@sensor(
    job=test_job,
    minimum_interval_seconds=30,
    description="Monitors lakeFS 'check_merge_data' for new CSVs and triggers a job chain.",
    required_resource_keys={"lakefs"},
    default_status=DefaultSensorStatus.RUNNING
)
def check_merge_sensor(context: SensorEvaluationContext) -> Union[SkipReason, List[RunRequest]]:
    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = "main"
    folder = "new_data/check_merge_data"
    files = fs.ls(f"{repo}/{branch}/{folder}/")
    csv_files = [f["name"] if isinstance(f, dict) else f for f in files if (f["name"] if isinstance(f, dict) else f).endswith(".csv")]
    # Use cursor to track processed files
    last_seen = set(context.cursor.split(",")) if context.cursor else set()
    csv_files_sorted = sorted(set(csv_files))
    new_files = [f for f in csv_files_sorted if f not in last_seen]
    if not new_files:
        return SkipReason("No new CSV files detected in lakeFS.")
    # Only process one new file at a time (FIFO)
    csv_path = new_files[0]
    # Remove any repo/branch prefix from csv_path if present
    prefix = f"{repo}/{branch}/"
    if csv_path.startswith(prefix):
        csv_path = csv_path[len(prefix):]
    lakefs_uri = f"lakefs://{repo}/{branch}/{csv_path}"
    # Start the first job (test_job) with the new file
    run_request = RunRequest(
        run_key=f"checkmerge_{csv_path}_{int(time.time())}",
        run_config={
            "ops": {
                "load_data": {
                    "config": {
                        "lakefs_uri": lakefs_uri
                    }
                }
            }
        },
        tags={
            "check_merge_file": csv_path,
            "job_chain_stage": "test_job"
        }
    )
    # Update cursor to mark this file as seen
    all_seen = sorted(set(csv_files_sorted) | last_seen | {csv_path})
    context.update_cursor(",".join(all_seen))
    return [run_request]

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[test_job],
    default_status=DefaultSensorStatus.RUNNING
)
def after_test_job_sensor(context: RunStatusSensorContext):
    run = context.dagster_run
    if run.tags.get("job_chain_stage") != "test_job":
        return None
    csv_path = run.tags.get("check_merge_file")
    lakefs_uri = run.run_config["ops"]["load_data"]["config"]["lakefs_uri"]
    # Extract percent from this run (predict_percent asset)
    percent = None
    for event in context.instance.get_event_records(run.run_id):
        if not hasattr(event, "event_type"):
            continue
        if event.event_type == "ASSET_MATERIALIZATION":
            mat = event.dagster_event.asset_materialization
            if mat and mat.asset_key and mat.asset_key.to_string() == "predict_percent":
                percent = mat.metadata.get("percent")
                if percent is not None:
                    percent = float(percent.value if hasattr(percent, "value") else percent)
                    break
    return RunRequest(
        run_key=f"retrain_{csv_path}_{int(time.time())}",
        run_config={
            "ops": {
                "load_data": {"config": {"lakefs_uri": lakefs_uri}}
            }
        },
        tags={
            "check_merge_file": csv_path,
            "job_chain_stage": "retrain_job",
            "test_percent": str(percent) if percent is not None else ""
        },
        job_name="retrain_job"
    )

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[retrain_job],
    default_status=DefaultSensorStatus.RUNNING
)
def after_retrain_job_sensor(context: RunStatusSensorContext):
    run = context.dagster_run
    if run.tags.get("job_chain_stage") != "retrain_job":
        return None
    csv_path = run.tags.get("check_merge_file")
    lakefs_uri = run.run_config["ops"]["load_data"]["config"]["lakefs_uri"]
    # Extract percent from this run (predict_asset_act asset)
    retrain_percent = None
    for event in context.instance.get_event_records(run.run_id):
        if not hasattr(event, "event_type"):
            continue
        if event.event_type == "ASSET_MATERIALIZATION":
            mat = event.dagster_event.asset_materialization
            if mat and mat.asset_key and mat.asset_key.to_string() == "predict_asset_act":
                retrain_percent = mat.metadata.get("percent")
                if retrain_percent is not None:
                    retrain_percent = float(retrain_percent.value if hasattr(retrain_percent, "value") else retrain_percent)
                    break
    # Get test_percent from tags
    test_percent = run.tags.get("test_percent")
    test_percent = float(test_percent) if test_percent not in (None, "") else None
    logger = get_dagster_logger()
    logger.info(f"Comparing retrain_percent={retrain_percent} to test_percent={test_percent}")
    if retrain_percent is not None and (test_percent is None or retrain_percent > test_percent):
        # Continue to deploy_job if retrain is better
        return RunRequest(
            run_key=f"deploy_{csv_path}_{int(time.time())}",
            run_config={},
            tags={
                "check_merge_file": csv_path,
                "job_chain_stage": "deploy_job",
                "merge": "0",
                "retrain_percent": str(retrain_percent),
                "test_percent": str(test_percent) if test_percent is not None else ""
            },
            job_name="deploy_job"
        )
    return None
