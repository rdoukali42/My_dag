from dagster import ResourceDefinition
import mlflow
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class MLflowResource:
    def __init__(self, tracking_uri=None, experiment_name=None):
        self.tracking_uri = tracking_uri
        self.experiment_name = experiment_name
        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        if experiment_name:
            mlflow.set_experiment(experiment_name)
        mlflow.sklearn.autolog()

    def start_run(self, run_name=None):
        return mlflow.start_run(run_name=run_name)

    def log_param(self, key, value):
        mlflow.log_param(key, value)

    def log_metric(self, key, value):
        mlflow.log_metric(key, value)

    def log_artifact(self, local_path):
        mlflow.log_artifact(local_path)

    def end_run(self):
        mlflow.end_run()

mlflow_resource = ResourceDefinition(
    resource_fn=lambda init_context: MLflowResource(
        tracking_uri=os.getenv("MLFLOW_TRACKING_URI"),
        experiment_name=os.getenv("MLFLOW_EXPERIMENT_NAME"),
    ),
    config_schema={},  # No config needed, since we use env vars
)
