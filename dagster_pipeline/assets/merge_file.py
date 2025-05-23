import os
import pandas as pd
from dagster import asset, AssetExecutionContext
from dagster_pipeline.utils.utils import list_lakefs_csv_files, merge_csv_files, save_merged_csv, remove_old_files

@asset(required_resource_keys={"lakefs"})
def merge_files_if_tagged(context: AssetExecutionContext):
    merge_tag = context.run.tags.get("merge")
    if str(merge_tag) != "1":
        context.log.info("Merge tag is not set to 1. Skipping merge.")
        return None

    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = os.getenv("LAKEFS_DEFAULT_BRANCH")
    merged_folder = context.run.tags.get("merged_data")
    if not merged_folder:
        context.log.error("No 'merged_data' tag found. Cannot determine folder for merged file.")
        return None
    folder = os.path.dirname(merged_folder)
    # List all CSV files in the folder
    csv_files = list_lakefs_csv_files(fs, repo, branch, folder)
    if not csv_files:
        context.log.error(f"No CSV files found in lakeFS folder: {repo}/{branch}/{folder}/")
        return None
    # Merge all CSV files
    merged_df = merge_csv_files(fs, repo, branch, csv_files)
    merged_path, merged_uri = save_merged_csv(fs, repo, branch, folder, merged_df)
    context.log.info(f"Merged CSVs saved to {merged_uri}")
    remove_old_files(fs, repo, branch, csv_files, merged_path, context)
    # Set the path to the merged file in the run config (for downstream assets)
    context.add_output_metadata({"lakefs_uri": merged_uri})
    return merged_df
