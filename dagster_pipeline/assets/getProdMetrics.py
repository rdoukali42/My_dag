from dagster import asset, AssetExecutionContext, MetadataValue
from mlflow.tracking import MlflowClient
import os

@asset(required_resource_keys={"mlflow"}, description="Fetch and log production model metrics from MLflow.")
def get_production_model_metrics(context: AssetExecutionContext) -> dict:
    model_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "Dagster_Mlflow")
    client = MlflowClient()
    prod_versions = client.get_latest_versions(model_name, stages=["Production"])
    if not prod_versions:
        context.log.warning("No model in Production stage.")
        context.add_output_metadata({"status": MetadataValue.text("No production model found")})
        return {}
    prod_version = prod_versions[0]
    run_id = prod_version.run_id
    run = client.get_run(run_id)
    metrics = run.data.metrics
    # Log all metrics to Dagster metadata
    context.add_output_metadata({k: MetadataValue.float(v) for k, v in metrics.items() if isinstance(v, float)})
    context.add_output_metadata({"run_id": MetadataValue.text(run_id), "model_version": MetadataValue.text(prod_version.version)})
    return metrics
