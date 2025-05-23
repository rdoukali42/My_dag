import os
import pandas as pd




def list_lakefs_csv_files(fs, repo, branch, folder):
    files = fs.ls(f"{repo}/{branch}/{folder}/")
    return [f["name"] if isinstance(f, dict) else f for f in files if (f["name"] if isinstance(f, dict) else f).endswith(".csv")]


def merge_csv_files(fs, repo, branch, csv_files):
    import pandas as pd
    return pd.concat([
        pd.read_csv(fs.open(f"lakefs://{repo}/{branch}/{file_path}")) for file_path in csv_files
    ], ignore_index=True)


def save_merged_csv(fs, repo, branch, folder, merged_df):
    merged_path = f"{folder}/merged.csv"
    merged_uri = f"lakefs://{repo}/{branch}/{merged_path}"
    with fs.open(merged_uri, "w") as f:
        merged_df.to_csv(f, index=False)
    return merged_path, merged_uri


def remove_old_files(fs, repo, branch, csv_files, merged_path, context=None):
    for file_path in csv_files:
        if file_path != merged_path:
            try:
                fs.rm(f"{repo}/{branch}/{file_path}")
                if context:
                    context.log.info(f"Removed old file: {file_path}")
            except Exception as e:
                if context:
                    context.log.warning(f"Failed to remove {file_path}: {e}")


def load_csv_with_encoding_fallback(fs, lakefs_uri, context):
    # List of encodings to try in order
    encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings_to_try:
        try:
            context.log.info(f"Trying to load CSV with encoding: {encoding}")
            with fs.open(lakefs_uri, encoding=encoding) as f:
                return pd.read_csv(f)
        except UnicodeDecodeError as e:
            context.log.warning(f"{encoding} decode failed: {e}")
            continue
        except Exception as e:
            context.log.warning(f"Failed to load CSV with {encoding}: {e}")
            continue
    
    # Last resort: try with errors='ignore' to skip problematic bytes
    try:
        context.log.warning(f"Loading CSV with UTF-8 and ignoring decode errors")
        with fs.open(lakefs_uri, encoding='utf-8', errors='ignore') as f:
            return pd.read_csv(f)
    except Exception as e:
        context.log.error(f"Failed to load CSV with error handling: {e}")
        raise