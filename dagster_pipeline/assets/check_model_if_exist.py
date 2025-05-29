from dagster import asset, MetadataValue
import os
import pandas as pd
import json
from pathlib import Path

"""Compare the uploaded-prepared data with all the prepared data from history ('archives/' folder) to check if it matches, 
then return the old run process id and the model file to deploy it, if not ignore it and continue the next assets"""
@asset(
    description="Check if the model already exists in LakeFS History",
    required_resource_keys={"lakefs"},
    group_name="Model_data_check"
)
def check_model_if_exist(context, prepare_data):
    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = os.getenv("LAKEFS_PROCESS_BRANCH")
    archive_folder = f"lakefs://{repo}/{branch}/archives/"
    
    # Extract data path from run tags if available
    data_path = context.run.tags.get("data_path", "unknown")
    
    context.log.info(f"Checking for existing models in: {archive_folder}")
    context.log.info(f"Current prepared data shape: {prepare_data.shape}")
    context.log.info(f"Processing data from: {data_path}")
    
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
            
            context.add_output_metadata({
                "exact_match_found": MetadataValue.bool(False),
                "data_path": MetadataValue.text(data_path),
                "message": MetadataValue.text("No archived runs found"),
                "data_previously_used": MetadataValue.bool(False),
                "previous_run_id": MetadataValue.text("none")
            })
            
            return {
                "model_exists": False,
                "message": "No archived runs found",
                "previous_usage": {
                    "data_previously_used": False,
                    "previous_run_id": "none",
                    "exact_match_found": False
                }
            }
            
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
                                    "exact_match_found": MetadataValue.bool(True),
                                    "data_path": MetadataValue.text(data_path),
                                    "model_file": MetadataValue.text(model_file),
                                    "run_id": MetadataValue.text(folder_name.split('_')[0]),
                                    "run_folder": MetadataValue.text(run_folder),
                                    "message": MetadataValue.text(f"Reusing existing model from {folder_name}"),
                                    "data_previously_used": MetadataValue.bool(True),
                                    "previous_run_id": MetadataValue.text(folder_name.split('_')[0])
                                })
                                
                                return {
                                    "model_exists": True,
                                    "model_file": model_file,
                                    "run_id": folder_name.split('_')[0],  # Extract short run ID
                                    "run_folder": run_folder,
                                    "message": f"Reusing existing model from {folder_name}",
                                    "previous_usage": {
                                        "data_previously_used": True,
                                        "previous_run_id": folder_name.split('_')[0],
                                        "exact_match_found": True
                                    }
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
        
        context.add_output_metadata({
            "exact_match_found": MetadataValue.bool(False),
            "data_path": MetadataValue.text(data_path),
            "message": MetadataValue.text("No matching model found, will train new model"),
            "data_previously_used": MetadataValue.bool(False),
            "previous_run_id": MetadataValue.text("none")
        })
        
        return {
            "model_exists": False,
            "message": "No matching model found, will train new model",
            "previous_usage": {
                "data_previously_used": False,
                "previous_run_id": "none",
                "exact_match_found": False
            }
        }
            
    except Exception as e:
        context.log.error(f"Error checking for existing models: {e}")
        
        context.add_output_metadata({
            "exact_match_found": MetadataValue.bool(False),
            "data_path": MetadataValue.text(data_path),
            "message": MetadataValue.text(f"Error during model check: {str(e)}"),
            "data_previously_used": MetadataValue.bool(False),
            "previous_run_id": MetadataValue.text("error")
        })
        
        # Don't raise the exception, just return no match found
        return {
            "model_exists": False,
            "message": f"Error during model check: {str(e)}",
            "previous_usage": {
                "data_previously_used": False,
                "previous_run_id": "error",
                "exact_match_found": False
            }
        }