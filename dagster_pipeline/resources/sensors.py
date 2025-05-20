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
        "process_data",
        "split_data",
        "preprocess",
        "train_XGBC",
        "evaluate_and_deploy_model",
        "serve_model",
        "evaluate_spotify_model"
    ]
)

@sensor(
    job=retrain_job,
    minimum_interval_seconds=15,
    description="Monitors lakeFS for new CSV data commits",
    required_resource_keys={"lakefs"}
)
def new_data_sensor(context: SensorEvaluationContext) -> Union[SkipReason, List[RunRequest]]:
    # Get lakeFS resource configuration
    import os
    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = os.getenv("LAKEFS_DEFAULT_BRANCH")
    data_path = os.getenv("LAKEFS_CSV_DATA")
    new_data = os.getenv("LAKEFS_NEW_DATA", "new_data/")
    data_prefix = os.path.dirname(data_path)

    # List all CSV files in the data_prefix
    files = fs.ls(f"{repo}/{branch}/{new_data}")
    # Each entry in files is likely a dict/object, not a string path
    csv_files = [f["name"] if isinstance(f, dict) else f for f in files if (f["name"] if isinstance(f, dict) else f).endswith(".csv")]

    # Use the sensor cursor to track which files have been processed
    last_seen = set(context.cursor.split(",")) if context.cursor else set()
    new_files = set(csv_files) - last_seen

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
                    "data_path": csv_path
                }
            )
        )

    # Update the cursor to include all seen files
    context.update_cursor(",".join(csv_files))
    return run_requests