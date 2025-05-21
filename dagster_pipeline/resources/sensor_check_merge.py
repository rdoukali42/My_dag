import os
import time
from typing import List, Optional, Union
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
    AssetKey,
    DagsterInstance,
    EventLogEntry,
    MetadataValue
)
from lakefs_spec import LakeFSFileSystem

# Import your assets
from dagster_pipeline.assets.predict_percent import predict_percent

# Define jobs
test_job = define_asset_job(
    name="test_job",
    selection=["load_data", "prepare_data", "predict_percent"]
)

retrain2_job = define_asset_job(
    name="retrain2_job",
    selection=["load_data", "prepare_data", "split_data", "preprocess", "train_XGBC", "predict_asset_act"]
)

deploy_job = define_asset_job(
    name="deploy_job",
    selection=["evaluate_and_deploy_model", "serve_model"]
)

def get_latest_percent(context: RunStatusSensorContext, asset_key_str: str) -> Optional[float]:
    """Get latest percentage value from asset materializations with error handling."""
    try:
        asset_key = AssetKey(asset_key_str)
        instance: DagsterInstance = context.instance
        
        # Remove has_asset check for 1.7 compatibility
        materialization = instance.get_latest_materialization_event(asset_key)
        if not materialization:
            context.log.warning(f"No materializations found for {asset_key_str}")
            return None

        entry: EventLogEntry = materialization.event_log_entry
        if not entry.dagster_event or not entry.dagster_event.asset_materialization:
            return None

        metadata = entry.dagster_event.asset_materialization.metadata
        percent_value = metadata.get("percent")
        if not percent_value or not isinstance(percent_value, MetadataValue.float):
            return None

        return percent_value.value

    except Exception as e:
        context.log.error(f"Error getting {asset_key_str} value: {str(e)}")
        return None

@sensor(
    job=test_job,
    minimum_interval_seconds=30,
    description="Monitors lakeFS for new CSVs and triggers testing",
    required_resource_keys={"lakefs"},
    default_status=DefaultSensorStatus.RUNNING
)
def check_merge_sensor(context: SensorEvaluationContext) -> Union[SkipReason, List[RunRequest]]:
    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = "main"
    folder = "new_data/check_merge_data"
    
    try:
        files = fs.ls(f"{repo}/{branch}/{folder}/")
    except Exception as e:
        context.log.error(f"LakeFS list failed: {str(e)}")
        return SkipReason("Failed to list lakeFS files")

    csv_files = [
        f["name"] if isinstance(f, dict) else f 
        for f in files 
        if (f["name"] if isinstance(f, dict) else f).endswith(".csv")
    ]
    
    last_seen = set(context.cursor.split(",")) if context.cursor else set()
    csv_files_sorted = sorted(set(csv_files))
    new_files = [f for f in csv_files_sorted if f not in last_seen]
    
    if not new_files:
        return SkipReason("No new CSV files detected")
    
    csv_path = new_files[0]
    prefix = f"{repo}/{branch}/"
    if csv_path.startswith(prefix):
        csv_path = csv_path[len(prefix):]
    
    lakefs_uri = f"lakefs://{repo}/{branch}/{csv_path}"
    
    all_seen = sorted(set(csv_files_sorted) | last_seen | {csv_path})
    context.update_cursor(",".join(all_seen))
    
    return [RunRequest(
        run_key=f"checkmerge_{csv_path}_{int(time.time())}",
        run_config={
            "ops": {"load_data": {"config": {"lakefs_uri": lakefs_uri}}}
        },
        tags={
            "check_merge_file": csv_path,
            "job_chain_stage": "test_job"
        }
    )]

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[test_job],
    request_job=retrain2_job,
    default_status=DefaultSensorStatus.RUNNING,
)
def after_test_job_sensor(context: RunStatusSensorContext):
    run = context.dagster_run
    if run.job_name == retrain2_job.name:
        return SkipReason("Don't retrain on retrain_job success.")
    if run.tags.get("job_chain_stage") != "test_job":
        return None

    csv_path = run.tags.get("check_merge_file")
    lakefs_uri = run.run_config["ops"]["load_data"]["config"]["lakefs_uri"]
    
    percent = get_latest_percent(context, "predict_percent")
    if percent is None:
        return SkipReason("No valid percent found in test_job run")

    return RunRequest(
        run_key=f"retrain_{csv_path}_{int(time.time())}",
        run_config={
            "ops": {"load_data": {"config": {"lakefs_uri": lakefs_uri}}}
        },
        tags={
            "check_merge_file": csv_path,
            "job_chain_stage": "retrain2_job",
            "test_percent": str(percent),
        },
    )

@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[retrain2_job],
    request_job=deploy_job,
    default_status=DefaultSensorStatus.RUNNING,
)
def after_retrain_job_sensor(context: RunStatusSensorContext):
    run = context.dagster_run
    if run.job_name == deploy_job.name:
        return SkipReason("Don't deploy on deploy_job success.")
    if run.tags.get("job_chain_stage") != "retrain2_job":
        return None

    csv_path = run.tags.get("check_merge_file")
    test_percent = run.tags.get("test_percent")
    test_percent = float(test_percent) if test_percent not in (None, "") else None
    
    retrain_percent = get_latest_percent(context, "predict_asset_act")
    logger = get_dagster_logger()
    logger.info(f"Test accuracy: {test_percent}, Retrain accuracy: {retrain_percent}")

    if retrain_percent is not None and (test_percent is None or retrain_percent > test_percent):
        return RunRequest(
            run_key=f"deploy_{csv_path}_{int(time.time())}",
            run_config={},
            tags={
                "check_merge_file": csv_path,
                "job_chain_stage": "deploy_job",
                "merge": "0",
                "retrain_percent": str(retrain_percent),
                "test_percent": str(test_percent) if test_percent is not None else "",
            },
        )
    return SkipReason("Retrain percent not better than test percent or not found.")