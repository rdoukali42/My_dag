import dagster as dg
from dagster import IOManager, io_manager
from lakefs_spec import LakeFSFileSystem


def get_lakefs_csv_path(fs, repo, branch, folder="new_data/"):
    folder = folder.rstrip("/")
    files = fs.ls(f"{repo}/{branch}/{folder}/")
    csv_files = [
        f["name"] if isinstance(f, dict) else f
        for f in files
        if (f["name"] if isinstance(f, dict) else f).endswith(".csv")
    ]
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in lakeFS folder: {repo}/{branch}/{folder}/")
    prefix = f"{repo}/{branch}/"
    data_path = csv_files[0][len(prefix):] if csv_files[0].startswith(prefix) else csv_files[0]
    return f"lakefs://{repo}/{branch}/{data_path}"
