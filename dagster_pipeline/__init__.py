from dagster import Definitions, load_assets_from_modules
from .assets import evaluate, load_data, preprocess, split_data, train_XGBC
from .resources.mlflow_resource import mlflow_resource
# from .io_managers.local_data_io_manager import local_data_io_manager

from .repository import defs

__all__ = ["defs"]

# all_assets = load_assets_from_modules([assets])

# defs = Definitions(
#     assets=all_assets,
#     resources={
#         "mlflow": mlflow_resource,
#         "local_data_io_manager": local_data_io_manager,
#     },
# )

# __init__.py (optional)
# from .assets import load_data
# from .resources import local_data_io_manager
