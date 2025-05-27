from typing import Union, List
import time
import json
from datetime import datetime
from lakefs_spec import LakeFSFileSystem
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
        "split_data_train",
        "split_data_test",
        "preprocess",
        "train_XGBC",
        "evaluate_and_deploy_model",
        "serve_model"
    ]
)

@sensor(
    job=retrain_job,
    minimum_interval_seconds=15,
    description="Monitors lakeFS for new CSV data commits",
    required_resource_keys={"lakefs"}
)
def new_data_sensor(context: SensorEvaluationContext) -> Union[SkipReason, List[RunRequest]]:
    import os
    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = os.getenv("LAKEFS_DEFAULT_BRANCH")
    data_path = os.getenv("LAKEFS_CSV_DATA")
    new_data = os.getenv("LAKEFS_NEW_DATA", "new_data/")
    data_prefix = os.path.dirname(data_path)

    # List all CSV files in the data_prefix
    files = fs.ls(f"{repo}/{branch}/{new_data}")
    # Fix: strip any accidental repo/branch prefix from csv_files
    def strip_prefix(path):
        prefix = f"{repo}/{branch}/"
        return path[len(prefix):] if path.startswith(prefix) else path
    csv_files = [strip_prefix(f["name"] if isinstance(f, dict) else f) for f in files if (f["name"] if isinstance(f, dict) else f).endswith(".csv")]

    # Use the sensor cursor to track which files have been processed
    last_seen = set(context.cursor.split(",")) if context.cursor else set()
    # Always sort and deduplicate the file list for consistency
    csv_files_sorted = sorted(set(csv_files))
    new_files = [f for f in csv_files_sorted if f not in last_seen]

    context.log.info(f"Sensor detected files: {csv_files_sorted}")
    context.log.info(f"Already seen files: {sorted(last_seen)}")
    context.log.info(f"New files to process: {new_files}")

    if not new_files:
        return SkipReason("No new CSV files detected in lakeFS.")

    run_requests = []
    for csv_path in new_files:
        lakefs_uri = f"lakefs://{repo}/{branch}/{csv_path}"
        run_requests.append(
            RunRequest(
                run_key=f"lakefs_{csv_path}",
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
                    "merge": "0"
                }
            )
        )

    # Update the cursor to include all seen files (including new ones)
    all_seen = sorted(set(csv_files_sorted) | last_seen)
    context.update_cursor(",".join(all_seen))
    return run_requests