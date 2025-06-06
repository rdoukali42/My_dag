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


def get_run_path(fs, repo, branch):
    last_run_path = f"{repo}/{branch}/Last_run/"
    if fs.exists(last_run_path):
        return last_run_path
    else:
        raise FileNotFoundError(f"Last run path does not exist: {last_run_path}")
    
def get_archive_path(fs, repo, branch):
    last_archive_path = f"{repo}/{branch}/archives/"
    if fs.exists(last_archive_path):
        return last_archive_path
    else:
        raise FileNotFoundError(f"Last archives path does not exist: {last_archive_path}") 


def check_history_if_model_exist(context, prepare_data):
    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = os.getenv("LAKEFS_PROCESS_BRANCH")
    archive_folder = f"lakefs://{repo}/{branch}/archives/"
    
    context.log.info(f"Checking for existing models in: {archive_folder}")
    context.log.info(f"Current prepared data shape: {prepare_data.shape}")
    
    try:
        # List all subdirectories in archives folder
        archive_contents = fs.ls(archive_folder, detail=True)
        context.log.info(f"Found {len(archive_contents)} items in archive folder")
        
        # Filter for directories only - need to check the 'type' field correctly
        archive_runs = []
        for item in archive_contents:
            item_path = item['name'] if isinstance(item, dict) else str(item)
            # Check if it's a directory by trying to list its contents or checking the path
            if item_path.endswith('/') or item.get('type') == 'directory':
                archive_runs.append(item_path)
                
        context.log.info(f"Found {len(archive_runs)} archive run directories")
        
        if not archive_runs:
            context.log.info("No archived runs found")
            return None
            
        # Check each archived run for matching data and models
        for run_folder in archive_runs:
            try:
                # Ensure the path ends with /
                if not run_folder.endswith('/'):
                    run_folder += '/'
                    
                context.log.info(f"Checking run folder: {run_folder}")
                
                # List files in this run folder
                run_files = fs.ls(run_folder, detail=True)
                context.log.info(f"Found {len(run_files)} files in {run_folder}")
                
                # Look for processed data file (parquet or csv) and model file
                data_file = None
                model_file = None
                
                for file_info in run_files:
                    file_path = file_info['name'] if isinstance(file_info, dict) else str(file_info)
                    file_name = Path(file_path).name
                    
                    # Look for data files (parquet preferred, csv as fallback)
                    if file_name.lower().endswith('.parquet') and 'prepare_data' in file_name:
                        data_file = file_path
                    elif file_name.lower().endswith('.csv') and 'prepare_data' in file_name and not data_file:
                        data_file = file_path
                    
                    # Look for model files
                    if file_name.lower().endswith('.pkl') and ('model' in file_name.lower() or 'train' in file_name.lower()):
                        model_file = file_path
                
                if data_file and model_file:
                    context.log.info(f"Found data file: {data_file}")
                    context.log.info(f"Found model file: {model_file}")
                    
                    # Compare current data with archived data
                    try:
                        if data_file.lower().endswith('.parquet'):
                            archived_data = pd.read_parquet(fs.open(data_file, 'rb'))
                        else:
                            archived_data = pd.read_csv(fs.open(data_file, 'r'))
                        
                        context.log.info(f"Archived data shape: {archived_data.shape}")
                        
                        # Compare data - check if they are identical
                        if prepare_data.shape == archived_data.shape:
                            # Sort both dataframes to ensure consistent comparison
                            current_sorted = prepare_data.sort_values(by=list(prepare_data.columns)).reset_index(drop=True)
                            archived_sorted = archived_data.sort_values(by=list(archived_data.columns)).reset_index(drop=True)
                            
                            if current_sorted.equals(archived_sorted):
                                context.log.info(f"✅ Found matching data! Reusing model from: {run_folder}")
                                
                                # Extract run ID from folder name
                                folder_name = Path(run_folder.rstrip('/')).name

                                context.add_output_metadata({
                                    "model_file": model_file,
                                    "run_id": folder_name.split('_')[0],  # Extract short run ID
                                    "run_folder": run_folder,
                                    "message": f"Reusing existing model from {folder_name}"
                                })
                                
                                return {
                                    "model_exists": True,
                                    "model_file": model_file,
                                    "run_id": folder_name.split('_')[0],  # Extract short run ID
                                    "run_folder": run_folder,
                                    "message": f"Reusing existing model from {folder_name}"
                                }
                            else:
                                context.log.info(f"Data shapes match but content differs for {run_folder}")
                        else:
                            context.log.info(f"Data shapes differ: current {prepare_data.shape} vs archived {archived_data.shape}")
                            
                    except Exception as e:
                        context.log.warning(f"Could not compare data in {run_folder}: {e}")
                        continue
                else:
                    context.log.info(f"Missing data or model file in {run_folder} (data: {bool(data_file)}, model: {bool(model_file)})")
                    
            except Exception as e:
                context.log.warning(f"Error processing run folder {run_folder}: {e}")
                continue
        
        # No matching model found
        context.log.info("🔍 No matching models found in archive")
        return {
            "model_exists": False,
            "message": "No matching model found, will train new model"
        }
            
    except Exception as e:
        context.log.error(f"Error checking for existing models: {e}")
        # Don't raise the exception, just return no match found
        return {
            "model_exists": False,
            "message": f"Error during model check: {str(e)}"
        }  