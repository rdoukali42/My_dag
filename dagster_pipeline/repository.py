from dagster import Definitions

# Import assets
from dagster_pipeline.assets.load_data import load_data
from dagster_pipeline.assets.process_data import process_data
from dagster_pipeline.assets.preprocess import split_data, preprocess
from dagster_pipeline.assets.train import train_XGBC
from dagster_pipeline.assets.evaluate import evaluate_spotify_model
# from dagster_pipeline.assets.Mlflow_deploy import mlflow_deploy_model
from dagster_pipeline.assets.compare_results import compare_and_update_model
from dagster_pipeline.assets.mlflow_model_deploy import evaluate_and_deploy_model, serve_model
from dagster_pipeline.assets.getProdMetrics import get_production_model_metrics
from dagster_pipeline.assets import load_data, split_data, preprocess, train_XGBC, evaluate_spotify_model

# Import resources
from dagster_pipeline.resources import mlflow_resource
from dagster_pipeline.resources.lakefs import lakefs_resource


# Import the actual sensor object, not the module
from dagster_pipeline.resources.sensors import new_data_sensor


defs = Definitions(
    assets=[load_data, split_data, preprocess, train_XGBC, evaluate_spotify_model, evaluate_and_deploy_model, serve_model, get_production_model_metrics, process_data],
    resources={"mlflow": mlflow_resource,
               "lakefs": lakefs_resource.configured({
            "host": "http://127.0.0.1:8000/",
            "username": {"AKIAIOSFOLQUICKSTART"},
            "password": {"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
            "repository": "dagster-cloud",
            "default_branch": "main"
        })},
     # resources={
    #     "io_manager": local_data_io_manager.configured({
    #         "base_dir": "data",
    #         "raw_data_path": "/Users/level3/TrackAI/BikeEnv/bikes_rent/src/spotify_data.csv"
    #     })
    #     # "mlflow": mlflow_resource.configured({
    #     #     "tracking_uri": "http://localhost:5000",
    #     #     "experiment_name": "spotify_analysis"
    #     # })
    # },
    sensors=[new_data_sensor]
)