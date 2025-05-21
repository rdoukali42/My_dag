# import os
# from dagster import sensor, RunRequest, define_asset_job
# from dagster_pipeline.assets import load_data, split_data, preprocess, train_XGBC, evaluate_spotify_model

# # Define the retrain job using your asset graph
# retrain_job = define_asset_job(
#     name="retrain_job",
#     selection=[
#         "load_data",
#         "split_data",
#         "preprocess",
#         "train_XGBC",
#         "evaluate_spotify_model"
#     ]
# )

# # Sensor to watch for new data files in the resources folder
# @sensor(job=retrain_job)
# def new_data_sensor(context):
#     data_folder = os.path.join(os.path.dirname(__file__), "resources")
#     processed_files = set(context.instance.get_key_value_store().get("processed_files", set()))
#     for fname in os.listdir(data_folder):
#         if fname.endswith(".csv") and fname not in processed_files:
#             processed_files.add(fname)
#             context.instance.get_key_value_store().set("processed_files", processed_files)
#             yield RunRequest(
#                 run_key=fname,
#                 run_config={
#                     "ops": {
#                         "load_data": {"config": {"file_path": os.path.join(data_folder, fname)}}
#                     }
#                 }
#             )


from typing import Union, List

import os
import time
import json
from datetime import datetime

from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
    AssetSelection,
    define_asset_job
)


# Define the retrain job using your asset graph
retrain_job = define_asset_job(
    name="retrain_job",
    selection=[
        "load_data",
        "prepare_data",
        "split_data",
        "preprocess",
        "train_XGBC",
        "evaluate_and_deploy_model",
        "serve_model",

    ]
)


@sensor(
    job=retrain_job,
    minimum_interval_seconds=30,  # Check every minute
    description="Data Trigger",
)
def local_data_sensor(context: SensorEvaluationContext) -> Union[SkipReason, RunRequest]:
    # Directory to monitor for new CSV files
    data_dir = os.environ.get("SPOTIFY_DATA_DIR", "/Users/level3/TrackAI/mlops/dagster_pipeline/dagster_pipeline/data")
    
    # Get the state from the previous run
    cursor_state = {}
    if context.cursor:
        try:
            cursor_state = json.loads(context.cursor)
        except json.JSONDecodeError:
            context.log.error(f"Invalid cursor format: {context.cursor}")
            cursor_state = {}
    
    last_processed_files = cursor_state.get("processed_files", [])
    
    # Get current list of CSV files in the directory
    current_files = []
    try:
        for filename in os.listdir(data_dir):
            if filename.lower().endswith('.csv'):
                file_path = os.path.join(data_dir, filename)
                current_files.append(file_path)
    except FileNotFoundError:
        context.log.error(f"Data directory {data_dir} not found!")
        return SkipReason(f"Data directory {data_dir} not found!")
    
    # Find new files that haven't been processed yet
    new_files = [f for f in current_files if f not in last_processed_files]
    
    if not new_files:
        # No new files, skip this run
        return SkipReason("No new CSV files detected")
    
    # Log the new files that were found
    context.log.info(f"Found {len(new_files)} new CSV files: {new_files}")
    
    # Update the cursor with the new files and current time
    new_cursor_state = {
        "last_check_time": time.time(),
        "processed_files": current_files
    }
    context.update_cursor(json.dumps(new_cursor_state))


    run_requests = []
    for new_file in new_files:
        run_requests.append(
            RunRequest(
                run_key=f"new_data_{os.path.basename(new_file)}_{int(time.time())}",
                run_config={
                    "ops": {
                        "load_data": {
                            "config": {
                                "file_path": new_file
                            }
                        }
                    }
                },
                tags={
                    "source_file": new_file,
                    "processing_time": datetime.now().isoformat()
                }
            )
        )
    
    context.update_cursor(json.dumps(new_cursor_state))
    return run_requests
