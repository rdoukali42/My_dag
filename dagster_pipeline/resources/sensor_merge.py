import os
import pandas as pd
from typing import Union, List
from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
    define_asset_job
)
from lakefs_spec import LakeFSFileSystem
import time

# Use the same retrain_job as in your main pipeline
from dagster_pipeline.resources.sensors import retrain_job
@sensor(
    job=retrain_job,
    minimum_interval_seconds=30,
    description="Monitors lakeFS 'merge_data' folder for new CSVs and merges them if columns match.",
    required_resource_keys={"lakefs"}
)
def merge_and_retrain_sensor(context: SensorEvaluationContext) -> Union[SkipReason, List[RunRequest]]:
    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = "main"
    merge_folder = "new_data/merge_data"
    files = fs.ls(f"{repo}/{branch}/{merge_folder}/")
    # Fix: strip any accidental repo/branch prefix from csv_files
    def strip_prefix(path):
        prefix = f"{repo}/{branch}/"
        return path[len(prefix):] if path.startswith(prefix) else path
    csv_files = [strip_prefix(f["name"] if isinstance(f, dict) else f) for f in files if (f["name"] if isinstance(f, dict) else f).endswith(".csv")]
    if not csv_files:
        return SkipReason(f"No CSV files found in lakeFS folder: {repo}/{branch}/{merge_folder}/")

    # Check columns
    columns_set = None
    dfs = []
    for file_path in csv_files:
        lakefs_uri = f"lakefs://{repo}/{branch}/{file_path}"
        try:
            with fs.open(lakefs_uri) as f:
                df = pd.read_csv(f, nrows=10)  # Read a sample for columns
            if columns_set is None:
                columns_set = set(df.columns)
            elif set(df.columns) != columns_set:
                context.log.warning(f"File {file_path} has different columns. Skipping merge.")
                return SkipReason(f"File {file_path} has different columns. All files must have the same columns to merge.")
        except Exception as e:
            context.log.warning(f"Failed to read {lakefs_uri}: {e}")
            return SkipReason(f"Failed to read {lakefs_uri}: {e}")

    # Only merge if there is more than one file
    if len(csv_files) < 2:
        context.log.info("Only one CSV file found, skipping merge.")
        return SkipReason("Only one CSV file found, skipping merge.")

    # If all columns match, merge all files
    merged_df = pd.concat([
        pd.read_csv(fs.open(f"lakefs://{repo}/{branch}/{file_path}")) for file_path in csv_files
    ], ignore_index=True)

    # Save merged file to a temp location in lakeFS (e.g., merge_data/merged.csv)
    merged_path = f"{merge_folder}/merged.csv"
    merged_uri = f"lakefs://{repo}/{branch}/{merged_path}"
    with fs.open(merged_uri, "w") as f:
        merged_df.to_csv(f, index=False)
    context.log.info(f"Merged CSVs saved to {merged_uri}")

    # Remove old files after merge (except the merged file)
    for file_path in csv_files:
        if file_path != merged_path:
            try:
                fs.rm(f"{repo}/{branch}/{file_path}")
                context.log.info(f"Removed old file: {file_path}")
            except Exception as e:
                context.log.warning(f"Failed to remove {file_path}: {e}")

    # Trigger retrain job with merged file
    run_request = RunRequest(
        run_key=f"merge_{int(time.time())}",
        run_config={
            "ops": {
                "load_data": {
                    "config": {
                        "lakefs_uri": merged_uri
                    }
                }
            }
        },
        tags={
            "merged_data": merged_path,
            "merge": "1"
        }
    )
    return [run_request]
