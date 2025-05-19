import os
from dagster import sensor, RunRequest, define_asset_job
from dagster_pipeline.assets import load_data, split_data, preprocess, train_XGBC, evaluate_spotify_model

# Define the retrain job using your asset graph
retrain_job = define_asset_job(
    name="retrain_job",
    selection=[
        "load_data",
        "split_data",
        "preprocess",
        "train_XGBC",
        "evaluate_spotify_model"
    ]
)

# Sensor to watch for new data files in the resources folder
@sensor(job=retrain_job)
def new_data_sensor(context):
    data_folder = os.path.join(os.path.dirname(__file__), "resources")
    processed_files = set(context.instance.get_key_value_store().get("processed_files", set()))
    for fname in os.listdir(data_folder):
        if fname.endswith(".csv") and fname not in processed_files:
            processed_files.add(fname)
            context.instance.get_key_value_store().set("processed_files", processed_files)
            yield RunRequest(
                run_key=fname,
                run_config={
                    "ops": {
                        "load_data": {"config": {"file_path": os.path.join(data_folder, fname)}}
                    }
                }
            )
