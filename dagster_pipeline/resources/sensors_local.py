from typing import Union, List

import os
import time
import json
from datetime import datetime
from dagster import AssetKey


from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
    AssetSelection,
    define_asset_job
)

# Define Jobs
check_model_job = define_asset_job(
    name="check_model_job", 
    selection=["load_data", "prepare_data", "check_model_if_exist"]
)

redeploy_job = define_asset_job(
    name="redeploy_job",
    selection=["evaluate_and_deploy_model", "serve_model"]
)

training_job = define_asset_job(
    name="training_job", 
    selection=[
        "load_data", "prepare_data", "split_data", "split_data_train", "split_data_test", "preprocess", 
        "train_XGBC", "evaluate_and_deploy_model", "serve_model", "archive_run_data"
    ]
)


@sensor(
    job=check_model_job,    
    minimum_interval_seconds=20,
    description="Smart sensor that detects new data and triggers appropriate pipeline based on model existence",
    required_resource_keys={"lakefs"} 
)
def smart_pipeline_sensor(context: SensorEvaluationContext) -> Union[SkipReason, List[RunRequest]]:
    """
    Unified sensor that:
    1. Detects new CSV files in lakeFS
    2. For each new file, runs model check to determine if model exists
    3. Triggers appropriate job: redeploy_job if model exists, training_job if not
    """
    # Get lakeFS resource and environment variables
    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = os.getenv("LAKEFS_DEFAULT_BRANCH")
    new_data = os.getenv("LAKEFS_NEW_DATA", "new_data/")

    # List all CSV files in the new_data directory
    try:
        files = fs.ls(f"{repo}/{branch}/{new_data}")
    except Exception as e:
        return SkipReason(f"Error accessing lakeFS: {e}")

    # Extract CSV files from the listing
    def strip_prefix(path):
        prefix = f"{repo}/{branch}/"
        return path[len(prefix):] if path.startswith(prefix) else path
    
    csv_files = [
        strip_prefix(f["name"] if isinstance(f, dict) else f) 
        for f in files 
        if (f["name"] if isinstance(f, dict) else f).endswith(".csv")
    ]

    # Use cursor to track processed files
    last_seen = set(context.cursor.split(",")) if context.cursor else set()
    csv_files_sorted = sorted(set(csv_files))
    new_files = [f for f in csv_files_sorted if f not in last_seen]

    if not new_files:
        return SkipReason("No new CSV files detected in lakeFS.")

    context.log.info(f"Found {len(new_files)} new files: {new_files}")

    run_requests = []
    
    # For each new file, create a run request for the check_model_job first
    # This will determine which pipeline to run next
    for csv_path in new_files:
        lakefs_uri = f"lakefs://{repo}/{branch}/{csv_path}"
        
        # First, run the model check job to determine pipeline path
        check_request = RunRequest(
            run_key=f"check_model_{csv_path}_{int(time.time())}",
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
                "data_path": csv_path,
                "merge": "0",
                "pipeline_stage": "check_model"
            }
        )
        run_requests.append(check_request)

    # Update cursor to include all seen files
    all_seen = sorted(set(csv_files_sorted) | last_seen)
    context.update_cursor(",".join(all_seen))
    
    return run_requests


@sensor(
    job=redeploy_job,
    minimum_interval_seconds=30,
    description="Triggers redeploy job when model exists for the data"
)
def redeploy_decision_sensor(context: SensorEvaluationContext) -> Union[SkipReason, RunRequest]:
    """
    Sensor that monitors check_model_if_exist asset materializations and triggers
    redeploy_job when model exists
    """
    asset_key = AssetKey("check_model_if_exist")
    latest_materialization = context.instance.get_latest_materialization_event(asset_key)
    
    if not latest_materialization:
        return SkipReason("No check_model_if_exist materialization found")
    
    # Use cursor to track processed materializations
    last_processed_timestamp = float(context.cursor) if context.cursor else 0
    current_timestamp = latest_materialization.timestamp
    
    if current_timestamp <= last_processed_timestamp:
        return SkipReason("Already processed this materialization")
    
    # Get metadata from the materialization
    metadata = latest_materialization.asset_materialization.metadata
    
    # Extract boolean value from MetadataValue
    exact_match_metadata = metadata.get("exact_match_found")
    model_exists = exact_match_metadata.value if exact_match_metadata and hasattr(exact_match_metadata, 'value') else False
    
    # Extract data path from MetadataValue
    data_path_metadata = metadata.get("data_path")
    data_path = data_path_metadata.value if data_path_metadata and hasattr(data_path_metadata, 'value') else "unknown"
    
    context.log.info(f"Redeploy sensor: Model check result for {data_path}: model_exists={model_exists}")
    
    # Only trigger if model exists
    if model_exists:
        context.log.info("Model exists - triggering redeploy pipeline")
        # Update cursor to current timestamp
        context.update_cursor(str(current_timestamp))
        
        return RunRequest(
            run_key=f"redeploy_{data_path}_{int(current_timestamp)}",
            tags={
                "data_path": data_path,
                "pipeline_type": "redeploy",
                "model_exists": "true"
            }
        )
    else:
        # Don't update cursor - let training sensor handle this
        return SkipReason(f"Model does not exist for {data_path} - training sensor will handle")


@sensor(
    job=training_job,
    minimum_interval_seconds=30,
    description="Triggers training job when no model exists for the data"
)
def training_decision_sensor(context: SensorEvaluationContext) -> Union[SkipReason, RunRequest]:
    """
    Sensor that monitors check_model_if_exist asset materializations and triggers
    training_job when no model exists
    """
    asset_key = AssetKey("check_model_if_exist")
    latest_materialization = context.instance.get_latest_materialization_event(asset_key)
    
    if not latest_materialization:
        return SkipReason("No check_model_if_exist materialization found")
    
    # Use cursor to track processed materializations
    last_processed_timestamp = float(context.cursor) if context.cursor else 0
    current_timestamp = latest_materialization.timestamp
    
    if current_timestamp <= last_processed_timestamp:
        return SkipReason("Already processed this materialization")
    
    # Get metadata from the materialization
    metadata = latest_materialization.asset_materialization.metadata
    
    # Extract boolean value from MetadataValue
    exact_match_metadata = metadata.get("exact_match_found")
    model_exists = exact_match_metadata.value if exact_match_metadata and hasattr(exact_match_metadata, 'value') else False
    
    # Extract data path from MetadataValue
    data_path_metadata = metadata.get("data_path")
    data_path = data_path_metadata.value if data_path_metadata and hasattr(data_path_metadata, 'value') else "unknown"
    
    context.log.info(f"Training sensor: Model check result for {data_path}: model_exists={model_exists}")
    
    # Only trigger if no model exists
    if not model_exists:
        context.log.info("No model exists - triggering training pipeline")
        # Update cursor to current timestamp
        context.update_cursor(str(current_timestamp))
        
        # For training job, we need to reconstruct the lakefs_uri from the data_path
        repo = os.getenv("LAKEFS_REPOSITORY")
        branch = os.getenv("LAKEFS_DEFAULT_BRANCH")
        lakefs_uri = f"lakefs://{repo}/{branch}/{data_path}"
        
        return RunRequest(
            run_key=f"training_{data_path}_{int(current_timestamp)}",
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
                "data_path": data_path,
                "pipeline_type": "training",
                "model_exists": "false",
                "merge": "0"
            }
        )
    else:
        # Don't update cursor - let redeploy sensor handle this
        return SkipReason(f"Model exists for {data_path} - redeploy sensor will handle")

