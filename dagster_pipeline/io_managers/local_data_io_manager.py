from dagster import IOManager, io_manager, Field, String
import pandas as pd
import os
import numpy as np
from pathlib import Path
from typing import Any
import joblib

class LocalDataIOManager(IOManager):
    def __init__(self, base_dir: str = "data", raw_data_path: str = None):
        self.base_dir = base_dir
        self.raw_data_path = raw_data_path

    def handle_output(self, context, obj: Any):
        # Handle DataFrame outputs
        if isinstance(obj, pd.DataFrame):
            self._save_dataframe(context, obj)
        # Handle sklearn objects and other ML objects
        else:
            # hasattr(obj, '__module__') and ('sklearn' in obj.__module__ or 'xgboost' in obj.__module__):
            self._save_ml_object(context, obj)
        # Try converting to DataFrame for other types
        # else:
        #     try:
        #         df = self._try_convert_to_dataframe(obj)
        #         self._save_dataframe(context, df)
        #         context.log.info("Converted output to DataFrame")
        #     except ValueError as e:
        #         raise ValueError(
        #             f"LocalDataIOManager received incompatible type {type(obj).__name__}. "
        #             "Supported types: pandas.DataFrame, sklearn objects, lists, dictionaries, numpy arrays."
        #         ) from e

    def _save_dataframe(self, context, df):
        """Save a DataFrame as CSV"""
        output_path = self._get_csv_path(context)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        try:
            df.to_csv(output_path, index=False)
            context.log.info(f"Successfully saved DataFrame to {output_path}")
        except Exception as e:
            raise IOError(f"Failed to write CSV to {output_path}") from e

    def _save_ml_object(self, context, obj):
        """Save a machine learning object using joblib"""
        output_path = self._get_model_path(context)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        try:
            joblib.dump(obj, output_path)
            context.log.info(f"Successfully saved ML object to {output_path}")
        except Exception as e:
            raise IOError(f"Failed to write ML object to {output_path}") from e

    def load_input(self, context) -> Any:
        # Special handling for raw data load
        if context.asset_key.path[-1] == "load_data":
            if not os.path.exists(self.raw_data_path):
                raise FileNotFoundError(
                    f"Raw data file not found at {self.raw_data_path}. "
                    "Check workspace.yaml configuration."
                )
            return pd.read_csv(self.raw_data_path)
        
        # Check for ML model files first
        model_path = self._get_model_path(context)
        if os.path.exists(model_path):
            try:
                return joblib.load(model_path)
            except Exception as e:
                raise IOError(f"Failed to load model from {model_path}") from e
            
        # Then check for CSV files
        csv_path = self._get_csv_path(context)
        if os.path.exists(csv_path):
            try:
                df = pd.read_csv(csv_path)
                context.log.info(f"Successfully loaded DataFrame from {csv_path}")
                return df
            except Exception as e:
                raise IOError(f"Failed to read CSV from {csv_path}") from e
        
        available_files = "\n".join(os.listdir(self.base_dir))
        raise FileNotFoundError(
            f"Neither model file {model_path} nor CSV file {csv_path} found. "
            f"Available files:\n{available_files}"
        )

    def _get_csv_path(self, context) -> str:
        asset_key_path = "/".join(context.asset_key.path)
        return os.path.join(self.base_dir, f"{asset_key_path}.csv")
    
    def _get_model_path(self, context) -> str:
        asset_key_path = "/".join(context.asset_key.path)
        return os.path.join(self.base_dir, f"{asset_key_path}.joblib")

    def _try_convert_to_dataframe(self, obj: Any) -> pd.DataFrame:
        """Handle tuple types from workspace.yaml configuration"""
        if isinstance(obj, tuple):
            if all(isinstance(item, (pd.Series, np.ndarray)) for item in obj):
                return pd.DataFrame({f"col_{i}": item for i, item in enumerate(obj)})
            elif len(obj) == 2 and isinstance(obj[1], (pd.Series, np.ndarray)):
                return pd.DataFrame({
                    **{f"feature_{i}": val for i, val in enumerate(obj[0])},
                    "label": obj[1]
                })
            else:
                try:
                    return pd.DataFrame(list(obj))
                except ValueError:
                    return pd.DataFrame([obj])
        
        if isinstance(obj, list):
            return pd.DataFrame(obj)
        if hasattr(obj, "to_dict"):
            return pd.DataFrame(obj.to_dict())
        if isinstance(obj, dict):
            return pd.DataFrame([obj])
        
        raise ValueError(
            f"Cannot convert type {type(obj).__name__} to DataFrame. "
            "Supported types: DataFrame, list, dict, tuple of arrays, numpy array. "
            "Return a DataFrame directly for full control."
        )

@io_manager(
    config_schema={
        "base_dir": Field(
            String,
            default_value="data",
            description="Base directory for processed data files",
        ),
        "raw_data_path": Field(
            String,
            description="Absolute path to raw input CSV file",
            is_required=True
        )
    }
)
def local_data_io_manager(context):
    return LocalDataIOManager(
        base_dir=context.resource_config["base_dir"],
        raw_data_path=context.resource_config["raw_data_path"]
    )