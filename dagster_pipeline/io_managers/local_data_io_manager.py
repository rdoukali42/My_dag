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
from datetime import datetime
from dotenv import load_dotenv

# Optional import for encoding detection
try:
    import chardet
    HAS_CHARDET = True
except ImportError:
    HAS_CHARDET = False
    print("[WARNING] chardet not available. Install with: pip install chardet")

# Load environment variables from .env file
load_dotenv()
class LakeFSClient:
    def __init__(
        self,
        lakefs_endpoint: str,
        lakefs_access_key: str,
        lakefs_secret_key: str,
        repository: str,
        branch: str = "main",
        path_prefix: str = "",
    ):
        self.lakefs_endpoint = lakefs_endpoint
        self.lakefs_access_key = lakefs_access_key
        self.lakefs_secret_key = lakefs_secret_key
        self.repository = repository
        self.branch = branch
        self.path_prefix = path_prefix

        
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
    repository: str
    branch: str = "main"
    path_prefix: list[str] = []

    @property
    def client(self) -> LakeFSClient:
        return LakeFSClient(self.lakefs_endpoint, self.lakefs_access_key, self.lakefs_secret_key, self.repository, self.branch, self.path_prefix)

    def _get_path(self, context) -> str:
        asset_path = '/'.join(self.path_prefix + list(context.asset_key.path))
        # Return a LakeFS URI
        return f"lakefs://{self.repository}/{self.branch}/{asset_path}"

    def _get_fs(self):
        return LakeFSFileSystem(
            host=self.lakefs_endpoint,
            username=self.lakefs_access_key,
            password=self.lakefs_secret_key,
        )

    def _get_local_fallback_path(self, context) -> str:
        """Get local fallback path for when LakeFS is not accessible."""
        asset_path = '/'.join(self.path_prefix + list(context.asset_key.path))
        # Use local dagster-cloud directory as fallback
        return f"dagster-cloud/{self.branch}/{asset_path}"

    def _load_from_local(self, local_path: str, context) -> Any:
        """Load data from local file system as fallback."""
        import os
        
        # Try DataFrame
        if os.path.exists(local_path + '.csv'):
            file_path = local_path + '.csv'
            try:
                # Use the same encoding fallback logic
                encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
                
                for encoding in encodings_to_try:
                    try:
                        print(f"[DEBUG] Trying to load local CSV with encoding: {encoding}")
                        return pd.read_csv(file_path, encoding=encoding)
                    except UnicodeDecodeError as e:
                        print(f"[DEBUG] {encoding} decode failed: {e}")
                        continue
                    except Exception as e:
                        print(f"[DEBUG] Failed to load local CSV with {encoding}: {e}")
                        continue
                
                # Last resort: try with errors='ignore'
                try:
                    print(f"[WARNING] Loading local CSV with UTF-8 and ignoring decode errors")
                    return pd.read_csv(file_path, encoding='utf-8', errors='ignore')
                except Exception as e:
                    print(f"[DEBUG] Failed to load local CSV with error handling: {e}")
            except Exception as e:
                print(f"[DEBUG] Failed to load local CSV: {e}")
        
        # Try pickle
        if os.path.exists(local_path + '.pkl'):
            with open(local_path + '.pkl', 'rb') as f:
                return pickle.load(f)
        
        # Try JSON
        if os.path.exists(local_path + '.json'):
            try:
                with open(local_path + '.json', 'r', encoding='utf-8') as f:
                    return json.load(f)
            except UnicodeDecodeError:
                with open(local_path + '.json', 'r', encoding='latin-1') as f:
                    return json.load(f)
        
        # Try float/int/str
        if os.path.exists(local_path + '.txt'):
            try:
                with open(local_path + '.txt', 'r', encoding='utf-8') as f:
                    val = f.read()
                    try:
                        return float(val) if '.' in val else int(val)
                    except Exception:
                        return val
            except UnicodeDecodeError:
                with open(local_path + '.txt', 'r', encoding='latin-1') as f:
                    val = f.read()
                    try:
                        return float(val) if '.' in val else int(val)
                    except Exception:
                        return val
        
        # Try None
        if os.path.exists(local_path + '.none'):
            return None
            
        raise FileNotFoundError(f"No persisted object found at {local_path} (tried .csv, .json, .pkl, .txt, .none)")

    def _detect_encoding(self, fs, path: str) -> str:
        """Detect file encoding by reading a sample of the file."""
        if not HAS_CHARDET:
            print(f"[DEBUG] chardet not available, defaulting to utf-8")
            return 'utf-8'
            
        try:
            # Read first 10KB to detect encoding
            with fs.open(path, 'rb') as f:
                raw_data = f.read(10240)  # Read 10KB
            
            if raw_data:
                detected = chardet.detect(raw_data)
                encoding = detected.get('encoding', 'utf-8')
                confidence = detected.get('confidence', 0)
                
                print(f"[DEBUG] Detected encoding: {encoding} (confidence: {confidence:.2f})")
                
                # If confidence is low, fallback to common encodings
                if confidence < 0.7:
                    print(f"[DEBUG] Low confidence, trying common encodings")
                    return 'utf-8'  # Will fallback in the calling method
                
                return encoding
            else:
                return 'utf-8'
        except Exception as e:
            print(f"[DEBUG] Encoding detection failed: {e}")
            return 'utf-8'

    def handle_output(self, context: dg.OutputContext, obj: Any):
        path = self._get_path(context)
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
            with fs.open(path, 'w', encoding='utf-8') as f:
                obj.to_csv(f, index=False)
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
        # tuple/list of primitives (e.g., split_data returns)
        # elif isinstance(obj, (tuple, list)):
        #     path = path + '.pkl'
        #     with fs.open(path + '.pkl', 'wb') as f:
        #         pickle.dump(obj, f)
        # None
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
        fs = self._get_fs()
        print(f"[DEBUG] Checking for file: {path}")
        
        # First try LakeFS
        try:
            print(f"[DEBUG] fs.exists(path): {fs.exists(path)}")
            print(f"[DEBUG] Listing dir: lakefs://{self.repository}/{self.branch}/")
            print(f"[DEBUG] fs.ls: {fs.ls(f'lakefs://{self.repository}/{self.branch}/')}")
            lakefs_available = True
        except Exception as e:
            print(f"[DEBUG] LakeFS not accessible: {e}")
            lakefs_available = False
            
        # If LakeFS is not available, try local fallback
        if not lakefs_available:
            local_path = self._get_local_fallback_path(context)
            print(f"[DEBUG] Trying local fallback: {local_path}")
            return self._load_from_local(local_path, context)
            
        # LakeFS is available, proceed with LakeFS loading
        # Try DataFrame
        if fs.exists(path + '.csv'):
            path = path + '.csv'
            
            # First, try to detect the encoding
            detected_encoding = self._detect_encoding(fs, path)
            
            # List of encodings to try in order
            encodings_to_try = [detected_encoding, 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            # Remove duplicates while preserving order
            encodings_to_try = list(dict.fromkeys(encodings_to_try))
            
            for encoding in encodings_to_try:
                try:
                    print(f"[DEBUG] Trying to load CSV with encoding: {encoding}")
                    with fs.open(path, 'r', encoding=encoding) as f:
                        return pd.read_csv(f)
                except UnicodeDecodeError as e:
                    print(f"[DEBUG] {encoding} decode failed: {e}")
                    continue
                except Exception as e:
                    print(f"[DEBUG] Failed to load CSV with {encoding}: {e}")
                    continue
            
            # Last resort: try with errors='ignore' to skip problematic bytes
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
            
        # If nothing found in LakeFS, try local fallback as last resort
        print(f"[DEBUG] Nothing found in LakeFS, trying local fallback")
        local_path = self._get_local_fallback_path(context)
        try:
            return self._load_from_local(local_path, context)
        except FileNotFoundError:
            raise FileNotFoundError(f"No persisted object found at {path} or {local_path} (tried .csv, .json, .pkl, .txt, .none)")

# @io_manager
# def my_io_manager(init_context):
#     # No config needed, all from env
#     print("LAKEFS HOST:", os.environ.get("LAKEFS_HOST"))
#     print("LAKEFS USERNAME:", os.environ.get("LAKEFS_USERNAME"))
#     print("LAKEFS PASSWORD:", os.environ.get("LAKEFS_PASSWORD"))
#     print("LAKEFS REPO:", os.environ.get("LAKEFS_REPOSITORY"))
#     print("LAKEFS BRANCH:", os.environ.get("LAKEFS_DEFAULT_BRANCH"))
#     return MyIOManager(path_prefix=[])

@io_manager
def my_io_manager_from_env(context, required_resource_keys={"lakefs"}) -> MyIOManager:
    return MyIOManager(
        lakefs_endpoint=os.environ["LAKEFS_HOST"],
        lakefs_access_key=os.environ["LAKEFS_USERNAME"],
        lakefs_secret_key=os.environ["LAKEFS_PASSWORD"],
        repository=os.environ["LAKEFS_REPOSITORY"],
        branch=os.environ.get("LAKEFS_DEFAULT_BRANCH"),
        path_prefix=[],
    )
