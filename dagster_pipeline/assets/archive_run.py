import os
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from dagster import asset
from dagster_pipeline.utils.utils import get_run_path

@asset(
    deps=["serve_model"],
    description="Archive all run data by converting to compressed formats",
    required_resource_keys={"lakefs"}
)
def archive_run_data(context) -> dict:
    fs = context.resources.lakefs
    repo = os.getenv("LAKEFS_REPOSITORY")
    branch = os.getenv("LAKEFS_PROCESS_BRANCH")
    
    # Get the run path from LakeFS
    last_run_path = get_run_path(fs, repo, branch)
    context.log.info(f"Last run data path: {last_run_path}")
    
    # Create run-specific archive folder
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id_short = context.run_id[:8]
    archive_folder = f"lakefs://{repo}/{branch}/archives/{run_id_short}_{timestamp}/"
    
    context.log.info(f"📦 Converting files to compressed formats: {archive_folder}")
    
    files_processed = 0
    total_original_size = 0
    total_compressed_size = 0
    
    try:
        # List files from LakeFS
        files = fs.ls(last_run_path, detail=True)
        context.log.info(f"Found {len(files)} files in last run path")
        
        for file_info in files:
            if file_info.get('type') != 'directory':
                file_path = file_info['name']
                file_name = Path(file_path).name
                original_size = file_info.get('size', 0)
                total_original_size += original_size
                context.log.info(f"Processing file: {file_name} ({original_size/1024:.1f}KB) at {file_path}")
                
                # Convert CSV to Parquet
                if file_name.lower().endswith('.csv'):
                    try:
                        # Read CSV from LakeFS
                        with fs.open(file_path, 'r') as f:
                            df = pd.read_csv(f)
                        
                        # Save as compressed Parquet
                        parquet_name = file_name.replace('.csv', '.parquet')
                        parquet_path = f"{archive_folder}{parquet_name}"
                        
                        with fs.open(parquet_path, 'wb') as f:
                            df.to_parquet(f, index=False, compression='snappy')
                        
                        # Get compressed size
                        compressed_size = fs.info(parquet_path)['size']
                        total_compressed_size += compressed_size
                        
                        context.log.info(f"✅ CSV→Parquet: {file_name} ({original_size/1024:.1f}KB → {compressed_size/1024:.1f}KB)")
                        
                    except Exception as e:
                        # Copy original if conversion fails
                        original_path = f"{archive_folder}{file_name}"
                        with fs.open(file_path, 'rb') as src, fs.open(original_path, 'wb') as dst:
                            dst.write(src.read())
                        total_compressed_size += original_size
                        context.log.warning(f"⚠️ CSV conversion failed, copied original: {file_name}")
                
                # Compress JSON files
                elif file_name.lower().endswith('.json'):
                    try:
                        # Read JSON from LakeFS
                        with fs.open(file_path, 'r') as f:
                            data = json.load(f)
                        
                        # Save as compressed JSON
                        compressed_path = f"{archive_folder}{file_name}.gz"
                        import gzip
                        
                        with fs.open(compressed_path, 'wb') as f:
                            with gzip.open(f, 'wt') as gz_f:
                                json.dump(data, gz_f, separators=(',', ':'))
                        
                        compressed_size = fs.info(compressed_path)['size']
                        total_compressed_size += compressed_size
                        
                        context.log.info(f"✅ JSON→GZIP: {file_name} ({original_size/1024:.1f}KB → {compressed_size/1024:.1f}KB)")
                        
                    except Exception as e:
                        # Copy original if compression fails
                        original_path = f"{archive_folder}{file_name}"
                        with fs.open(file_path, 'rb') as src, fs.open(original_path, 'wb') as dst:
                            dst.write(src.read())
                        total_compressed_size += original_size
                        context.log.warning(f"⚠️ JSON compression failed, copied original: {file_name}")
                
                # Copy other files as-is
                else:
                    original_path = f"{archive_folder}{file_name}"
                    with fs.open(file_path, 'rb') as src, fs.open(original_path, 'wb') as dst:
                        dst.write(src.read())
                    total_compressed_size += original_size
                    context.log.info(f"📁 Copied: {file_name}")
                
                files_processed += 1
                
    except Exception as e:
        context.log.warning(f"Error processing files: {e}")
        return {"status": "error", "message": str(e)}
    
    # Calculate compression ratio
    compression_ratio = (1 - total_compressed_size / total_original_size) * 100 if total_original_size > 0 else 0
    
    # Create manifest
    manifest = {
        "run_id": context.run_id,
        "timestamp": datetime.now().isoformat(),
        "files_processed": files_processed,
        "original_size_mb": round(total_original_size / 1024 / 1024, 2),
        "compressed_size_mb": round(total_compressed_size / 1024 / 1024, 2),
        "compression_ratio_percent": round(compression_ratio, 1),
        "archive_folder": archive_folder,
        "conversions": {
            "csv_to_parquet": "snappy compression",
            "json_to_gzip": "gzip compression",
            "other_files": "copied as-is"
        }
    }
    
    # Save manifest to LakeFS
    manifest_path = f"{archive_folder}manifest.json"
    with fs.open(manifest_path, 'w') as f:
        json.dump(manifest, f, indent=2)
    
    context.log.info(f"🎉 Conversion complete! {files_processed} files processed")
    context.log.info(f"💾 Size reduction: {total_original_size/1024/1024:.1f}MB → {total_compressed_size/1024/1024:.1f}MB ({compression_ratio:.1f}%)")
    
    return manifest