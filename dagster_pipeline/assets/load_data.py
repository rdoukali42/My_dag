import os
import pandas as pd
from dagster import asset
from dotenv import load_dotenv

load_dotenv()

@asset(required_resource_keys={"lakefs"})
def load_data(context):
    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = os.getenv("LAKEFS_DEFAULT_BRANCH")
    lakefs_uri = context.op_config.get("lakefs_uri") if hasattr(context, "op_config") and context.op_config else None
    if lakefs_uri:
        with fs.open(lakefs_uri) as f:
            df = pd.read_csv(f)
        context.log.info(f"Loaded data from lakeFS: {lakefs_uri}")
    else:
        folder = os.getenv("LAKEFS_CSV_DATA", "new_data/").rstrip("/")
        files = fs.ls(f"{repo}/{branch}/{folder}/")
        csv_files = [f["name"] if isinstance(f, dict) else f for f in files if (f["name"] if isinstance(f, dict) else f).endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in lakeFS folder: {repo}/{branch}/{folder}/")
        def strip_prefix(path):
            prefix = f"{repo}/{branch}/"
            return path[len(prefix):] if path.startswith(prefix) else path
        data_path = strip_prefix(csv_files[0])
        lakefs_uri = f"lakefs://{repo}/{branch}/{data_path}"
        with fs.open(lakefs_uri) as f:
            df = pd.read_csv(f)
        context.log.info(f"Loaded data from lakeFS: {lakefs_uri}")
    return df




# @asset(
#     config_schema={
#         "file_path": Field(
#             str,
#             description="Path to CSV file to process",
#             default_value="/Users/level3/TrackAI/BikeEnv/bikes_rent/src/spotify_data.csv",
#         )
#     }
# )
# def load_data(context):
#     file_path = context.op_config["file_path"]
#     # file_path = "/Users/level3/TrackAI/BikeEnv/bikes_rent/src/spotify_data.csv"
#     df = pd.read_csv(file_path)
#     df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
#     df = df.drop(["track_id"], axis=1)
#     df = df[df["year"] != 2023]
#     df["popularity"] = (df["popularity"] >= 50).astype(int)
#     return df
