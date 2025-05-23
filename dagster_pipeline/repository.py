from dagster import Definitions

# Import assets
from dagster_pipeline.assets.load_data import load_data
from dagster_pipeline.assets.process_data import process_data
from dagster_pipeline.assets.preprocess import split_data, preprocess
from dagster_pipeline.assets.train import train_XGBC
from dagster_pipeline.assets.evaluate import evaluate_spotify_model
from dagster_pipeline.assets.compare_results import compare_and_update_model
from dagster_pipeline.assets.mlflow_model_deploy import evaluate_and_deploy_model, serve_model
from dagster_pipeline.assets.getProdMetrics import get_production_model_metrics
from dagster_pipeline.assets import load_data, split_data, preprocess, train_XGBC, evaluate_spotify_model
from dagster_pipeline.assets.prepare_data import prepare_data
from dagster_pipeline.assets.predict_percent import predict_percent
from dagster_pipeline.assets.predict_asset_act import predict_asset_act

# Import resources
from dagster_pipeline.resources import mlflow_resource
from dagster_pipeline.resources.lakefs import lakefs_resource
from dagster_pipeline.io_managers.local_data_io_manager import my_io_manager_from_env
# Import the actual sensors
from dagster_pipeline.resources.sensors import new_data_sensor
from dagster_pipeline.resources.sensor_merge import merge_and_retrain_sensor
# from dagster_pipeline.resources.sensor_check_merge import check_merge_sensor, after_test_job_sensor, after_retrain_job_sensor

lakefs_config = {
    "host": "http://localhost:8000/",
    "username": "AKIAIOSFOLQUICKSTART",
    "password": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "repository": "dagster-cloud",
    "default_branch": "main"
}

defs = Definitions(
    assets=[
        load_data,
        prepare_data,
        split_data,
        preprocess,
        train_XGBC,
        evaluate_and_deploy_model,
        serve_model,
        get_production_model_metrics,
        predict_percent,
        predict_asset_act,
    ],
    resources={
        "mlflow": mlflow_resource,
        "lakefs": lakefs_resource.configured(lakefs_config),
        "io_manager": my_io_manager_from_env.configured({
            "lakefs_endpoint": "http://localhost:8000",
                "lakefs_access_key": "AKIAIOSFOLQUICKSTART",
                "lakefs_secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", 
                "repository": "dagster-cloud",
                "branch": "main",
                "path_prefix": []
        })
    },
    sensors=[new_data_sensor, merge_and_retrain_sensor]
)