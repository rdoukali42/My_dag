from dagster import Definitions
import os

# Import assets
from dagster_pipeline.assets.load_data import load_data
from dagster_pipeline.assets.preprocess import split_data, split_data_train, split_data_test, preprocess
from dagster_pipeline.assets.train import train_XGBC
from dagster_pipeline.assets.mlflow_model_deploy import evaluate_and_deploy_model, serve_model
from dagster_pipeline.assets.getProdMetrics import get_production_model_metrics
from dagster_pipeline.assets.prepare_data import prepare_data
from dagster_pipeline.assets.predict_percent import predict_percent
from dagster_pipeline.assets.predict_asset_act import predict_asset_act

# Import jobs instead of assets for LakeFS operations
from dagster_pipeline.jobs.lakefs_jobs import setup_lakefs_job, cleanup_lakefs_job

# Import resources
from dagster_pipeline.resources import mlflow_resource
from dagster_pipeline.resources.lakefs import lakefs_resource
from dagster_pipeline.resources.lakefs_client import lakefs_client_resource
from dagster_pipeline.io_managers.local_data_io_manager import my_io_manager_from_env

# Import the actual sensors
from dagster_pipeline.resources.sensors import new_data_sensor
from dagster_pipeline.resources.sensor_merge import merge_and_retrain_sensor
# from dagster_pipeline.resources.sensor_check_merge import check_merge_sensor, after_test_job_sensor, after_retrain_job_sensor

# lakefs_config = {
#     "host": "http://localhost:8000/",
#     "username": "AKIAJR5HULP424UQEVVQ",
#     "password": "RmC0y4XMFfC4wEXj181jxxFKt03Px1MzCsGWSUQkY",
#     "repository": "dagster-cloud",
#     "default_branch": "main"
# }

defs = Definitions(
    assets=[
        load_data,
        prepare_data,
        split_data,
        split_data_train,
        split_data_test,
        preprocess,
        train_XGBC,
        evaluate_and_deploy_model,
        serve_model,
        get_production_model_metrics,
        predict_percent,
        predict_asset_act,
        # setup_lakefs_repository,
        # cleanup_lakefs_repository
    ],
    jobs = [
        setup_lakefs_job,
        cleanup_lakefs_job
    ],
    resources={
        "mlflow": mlflow_resource,
        # "lakefs": lakefs_resource.configured(lakefs_config),
        "lakefs": lakefs_resource.configured({
            "host": os.getenv("LAKEFS_HOST"),
            "username": os.getenv("LAKEFS_USERNAME"),
            "password": os.getenv("LAKEFS_PASSWORD"),
            "repository": os.getenv("LAKEFS_REPOSITORY"),
            "default_branch": os.getenv("LAKEFS_DEFAULT_BRANCH")}),
        "lakefs_client": lakefs_client_resource,
        "io_manager": my_io_manager_from_env.configured({
            "lakefs_uri": os.getenv("LAKEFS_URI"),
            "lakefs_endpoint": os.getenv("LAKEFS_HOST"),
            "lakefs_access_key": os.getenv("LAKEFS_USERNAME"),
            "lakefs_secret_key": os.getenv("LAKEFS_PASSWORD"),
            "repository": os.getenv("LAKEFS_REPOSITORY"),
            "path_prefix":  []
    })
    },
    sensors=[new_data_sensor, merge_and_retrain_sensor]
)