from dagster import IOManager, io_manager
import pandas as pd
import os

class LocalCSVIOManager(IOManager):
    def handle_output(self, context, obj):
        # Save DataFrame to CSV using asset key as filename
        if isinstance(obj, pd.DataFrame):
            output_path = self._get_path(context)
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            obj.to_csv(output_path, index=False)
            context.log.info(f"Saved DataFrame to {output_path}")
        else:
            raise ValueError("LocalCSVIOManager only supports pandas DataFrames.")

    def load_input(self, context):
        # Load DataFrame from CSV using upstream output asset key
        input_path = self._get_path(context)
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"No file found at {input_path}")
        df = pd.read_csv(input_path)
        context.log.info(f"Loaded DataFrame from {input_path}")
        return df

    def _get_path(self, context):
        # Use asset key to generate a unique file path
        asset_key = context.asset_key.path[-1]
        base_dir = os.path.join(os.getcwd(), "data_io")
        return os.path.join(base_dir, f"{asset_key}.csv")

@io_manager
def local_csv_io_manager(_):
    return LocalCSVIOManager()
