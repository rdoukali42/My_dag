import os
import pandas as pd
from dagster import asset, AssetExecutionContext

@asset(required_resource_keys={"lakefs"})
def merge_files_if_tagged(context: AssetExecutionContext):
    merge_tag = context.run.tags.get("merge")
    if str(merge_tag) != "1":
        context.log.info("Merge tag is not set to 1. Skipping merge.")
        return None

    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = os.getenv("LAKEFS_DEFAULT_BRANCH")
    # Get all file paths from the run config or tags
    merged_folder = context.run.tags.get("merged_data")
    if not merged_folder:
        context.log.error("No 'merged_data' tag found. Cannot determine folder for merged file.")
        return None
    folder = os.path.dirname(merged_folder)
    # List all CSV files in the folder
    files = fs.ls(f"{repo}/{branch}/{folder}/")
    csv_files = [f["name"] if isinstance(f, dict) else f for f in files if (f["name"] if isinstance(f, dict) else f).endswith(".csv")]
    if not csv_files:
        context.log.error(f"No CSV files found in lakeFS folder: {repo}/{branch}/{folder}/")
        return None
    # Merge all CSV files
    merged_df = pd.concat([
        pd.read_csv(fs.open(f"lakefs://{repo}/{branch}/{file_path}")) for file_path in csv_files
    ], ignore_index=True)
    # Save merged file
    merged_path = f"{folder}/merged.csv"
    merged_uri = f"lakefs://{repo}/{branch}/{merged_path}"
    with fs.open(merged_uri, "w") as f:
        merged_df.to_csv(f, index=False)
    context.log.info(f"Merged CSVs saved to {merged_uri}")
    # Remove old files (except the merged file)
    for file_path in csv_files:
        if file_path != merged_path:
            try:
                fs.rm(f"{repo}/{branch}/{file_path}")
                context.log.info(f"Removed old file: {file_path}")
            except Exception as e:
                context.log.warning(f"Failed to remove {file_path}: {e}")
    # Set the path to the merged file in the run config (for downstream assets)
    context.add_output_metadata({"lakefs_uri": merged_uri})
    return merged_df
