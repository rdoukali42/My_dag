# from dagster import Definitions, load_assets_from_modules
# from . import assets
# from .resources.mlflow_resource import mlflow_resource
# # from dagster_pipeline.io_managers.local_csv_io_manager import local_csv_io_manager
# from .io_manager.local_csv_io_manager import local_csv_io_manager

# all_assets = load_assets_from_modules([assets])

# defs = Definitions(
#     assets=all_assets,
#     resources={
#         "mlflow": mlflow_resource,
#         "local_csv_io_manager": local_csv_io_manager,
#     },
# )

# __init__.py (optional)
from .assets import load_data
from .resources import local_csv_io_manager
