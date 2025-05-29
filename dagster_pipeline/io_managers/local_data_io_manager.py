import dagster as dg
import pandas as pd
from lakefs_spec import LakeFSFileSystem
from dagster import io_manager
from typing import Any
import shutil
import json
import pickle
import os
import re
import io
from io import StringIO
from datetime import datetime
from dotenv import load_dotenv
from lakefs_client.exceptions import NotFoundException
from lakefs_client.models import BranchCreation

# Optional import for encoding detection
try:
    import chardet
    HAS_CHARDET = True
except ImportError:
    HAS_CHARDET = False
    print("[WARNING] chardet not available. Install with: pip install chardet")

# Load environment variables from .env file
load_dotenv()


        
class MyIOManager(dg.ConfigurableIOManager):
    # path_prefix: list[str] = []
    # lakefs_host: str = os.environ.get("LAKEFS_HOST")
    # lakefs_username: str = os.environ.get("LAKEFS_USERNAME")
    # lakefs_password: str = os.environ.get("LAKEFS_PASSWORD")
    # lakefs_repo: str = os.environ.get("LAKEFS_REPOSITORY")
    # lakefs_branch: str = os.environ.get("LAKEFS_DEFAULT_BRANCH")
    lakefs_endpoint: str
    lakefs_access_key: str
    lakefs_secret_key: str
    client: Any  # LakeFSClient instance
    repository: str
    branch: str = "Processed_data"
    path_prefix: list[str] = []



    def _get_path(self, context) -> str:
        asset_path = '/'.join(self.path_prefix + list(context.asset_key.path))
        folder_act = "Last_run"
        # Return a LakeFS URI
        return f"lakefs://{self.repository}/{self.branch}/{folder_act}/{asset_path}"
    
    def _folder_path(self, context) -> str:
        asset_path = '/'.join(self.path_prefix + list(context.asset_key.path))
        folder_act = "Last_run"
        return f"{folder_act}/{asset_path}"
    def _get_fs(self):
        return LakeFSFileSystem(
            host=self.lakefs_endpoint,
            username=self.lakefs_access_key,
            password=self.lakefs_secret_key,
        )


    def handle_output(self, context: dg.OutputContext, obj: Any):
        path = self._get_path(context)
        folder_path = self._folder_path(context)
        fs = self._get_fs()
        context.log.info(f"Preparing to write data to lakeFS: {path}")
        try:
            fs.ls(f"lakefs://{self.repository}/{self.branch}/")
        except Exception as e:
            raise ValueError(f"LakeFS connection failed: {str(e)}")
        print(f"OBJECT TO WRITE (type={type(obj)}): {obj}")

        # DataFrame
        if isinstance(obj, pd.DataFrame):
            path = path + '.csv'
            folder_path = folder_path + '.csv'
            # with fs.open(path, 'w', encoding='utf-8') as f:
            #     obj.to_csv(f, index=False)
            csv_content = obj.to_csv(index=False)
            csv_bytes = csv_content.encode('utf-8')
            csv_file = io.BytesIO(csv_bytes)
            self.client.objects.upload_object(
                repository=self.repository,
                branch=self.branch,
                path=folder_path,
                content=csv_file
            )
        # Tuple/list of DataFrames (e.g., split_data)
        elif isinstance(obj, (tuple, list)) and all(isinstance(x, pd.DataFrame) for x in obj):
            # path = path + '.pkl'
            with fs.open(path + '.pkl', 'wb') as f:
                pickle.dump(obj, f)
        # sklearn Pipeline or other model objects
        elif hasattr(obj, 'fit') and hasattr(obj, 'predict'):
            # path = path + '.pkl'
            with fs.open(path + '.pkl', 'wb') as f:
                pickle.dump(obj, f)
        # dict (metrics, etc.)
        elif isinstance(obj, dict):
            # path = path + '.json'
            with fs.open(path + '.json', 'w', encoding='utf-8') as f:
                json.dump(obj, f)
        # float/int (metrics, percent, etc.)
        elif isinstance(obj, (float, int)):
            # path = path + '.txt'
            with fs.open(path + '.txt', 'w', encoding='utf-8') as f:
                f.write(str(obj))
        # str (messages, model uri, etc.)
        elif isinstance(obj, str):
            # path = path + '.txt'
            with fs.open(path + '.txt', 'w', encoding='utf-8') as f:
                f.write(obj)
        elif obj is None:
            # path = path + '.none'
            with fs.open(path + '.none', 'w', encoding='utf-8') as f:
                f.write('null')
        # fallback: pickle
        else:
            # path = path + '.pkl'
            with fs.open(path + '.pkl', 'wb') as f:
                pickle.dump(obj, f)

        context.log.info(f"Data written to lakeFS: {path}")
        context.add_output_metadata({"lakefs_path": path, "object_type": str(type(obj))})

    def load_input(self, context) -> Any:
        path = self._get_path(context)
        folder_path = self._folder_path(context)
        fs = self._get_fs()
        print(f"[DEBUG] Checking for file: {path}")
        
        try:
            lakefs_available = True
        except Exception as e:
            # print(f"[DEBUG] LakeFS not accessible: {e}")
            lakefs_available = False
            
            
        # Try DataFrame
        if fs.exists(path + '.csv'):
            path = path + '.csv'
            folder_path = folder_path + '.csv'
            print(f"[DEBUG] fs.exists(path): {fs.exists(path)}")
            print(f"[DEBUG] Listing dir: lakefs://{self.repository}/{self.branch}/")
            print(f"[DEBUG] fs.ls: {fs.ls(f'lakefs://{self.repository}/{self.branch}/')}")

            try:
                response = self.client.objects.get_object(
                    repository=self.repository,
                    ref=self.branch,
                    path=folder_path
                )

                csv_content = response.read().decode('utf-8')
                context.log.info(f"✅ Downloaded {len(csv_content)} characters")

                
                df = pd.read_csv(StringIO(csv_content))
                context.log.info(f"✅ Loaded DataFrame: {df.shape}")

                return df
        
            except Exception as e:
                context.log.error(f"❌ Download failed from {self.repository}/{self.branch}/{path}: {e}")
                raise
            
            try:
                print(f"[WARNING] Loading CSV with UTF-8 and ignoring decode errors")
                with fs.open(path, 'r', encoding='utf-8', errors='ignore') as f:
                    return pd.read_csv(f)
            except Exception as e:
                print(f"[DEBUG] Failed to load CSV with error handling: {e}")
        # Try pickle
        if fs.exists(path + '.pkl'):
            with fs.open(path + '.pkl', 'rb') as f:
                return pickle.load(f)
        # Try JSON
        if fs.exists(path + '.json'):
            try:
                with fs.open(path + '.json', 'r', encoding='utf-8') as f:
                    return json.load(f)
            except UnicodeDecodeError:
                # Fallback to latin-1 for JSON files with encoding issues
                with fs.open(path + '.json', 'r', encoding='latin-1') as f:
                    return json.load(f)
        # Try float/int/str
        if fs.exists(path + '.txt'):
            try:
                with fs.open(path + '.txt', 'r', encoding='utf-8') as f:
                    val = f.read()
                    try:
                        return float(val) if '.' in val else int(val)
                    except Exception:
                        return val
            except UnicodeDecodeError:
                # Fallback to latin-1 for text files with encoding issues
                with fs.open(path + '.txt', 'r', encoding='latin-1') as f:
                    val = f.read()
                    try:
                        return float(val) if '.' in val else int(val)
                    except Exception:
                        return val
        # Try None
        if fs.exists(path + '.none'):
            return None
            

@io_manager(
    required_resource_keys={"lakefs", "lakefs_client"}
)
def my_io_manager_from_env(context, required_resource_keys={"lakefs"}) -> MyIOManager:
    return MyIOManager(
        client=context.resources.lakefs_client,
        lakefs_endpoint=os.environ["LAKEFS_HOST"],
        lakefs_access_key=os.environ["LAKEFS_USERNAME"],
        lakefs_secret_key=os.environ["LAKEFS_PASSWORD"],
        repository=os.environ["LAKEFS_REPOSITORY"],
        branch=os.environ.get("LAKEFS_PROCESS_BRANCH"),
        path_prefix=[],
    )
