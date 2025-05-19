from dagster import Definitions
from dagster_pipeline import assets  # ✅ Absolute import
from dagster_pipeline.resources import local_csv_io_manager
from .resources.mlflow_resource import mlflow_resource

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "mlflow": mlflow_resource,
        "local_csv_io_manager": local_csv_io_manager,
    },
)