import os
import pandas as pd
from dagster import asset
from dotenv import load_dotenv
from dagster_pipeline.utils.lakefs_utils import get_lakefs_csv_path
from dagster_pipeline.utils.utils import load_csv_with_encoding_fallback



@asset(required_resource_keys={"lakefs"},
       description="Asset that loads data from lakeFS",
       group_name="Load_data")
def load_data(context):
    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = os.getenv("LAKEFS_DEFAULT_BRANCH")
    lakefs_uri = context.op_config.get("lakefs_uri") if hasattr(context, "op_config") and context.op_config else None
    context.log.info(f"Using lakeFS URI: {lakefs_uri}")
    if not lakefs_uri:
        lakefs_uri = get_lakefs_csv_path(fs, repo, branch)
    df = load_csv_with_encoding_fallback(fs, lakefs_uri, context)
    context.log.info(f"Loaded data from lakeFS: {lakefs_uri}")
    context.log.info(f"Data shape: {df.shape}")
    return df
