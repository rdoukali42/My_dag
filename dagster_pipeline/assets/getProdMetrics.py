from dagster import asset, AssetExecutionContext, MetadataValue
from mlflow.tracking import MlflowClient
from dagster_pipeline.utils.mlflow_utils import MODEL_NAME

@asset(required_resource_keys={"mlflow"},
       description="Fetch and log production model metrics from MLflow.",
       group_name="Model_Metrics")
def get_production_model_metrics(context: AssetExecutionContext) -> dict:
    model_name = MODEL_NAME
    client = MlflowClient()
    try:
        prod_versions = client.get_latest_versions(model_name, stages=["Production"])
    except Exception as e:
        context.log.error(f"Error fetching production model versions: {e}")
        context.add_output_metadata({"status": MetadataValue.text("Error fetching production model versions")})
        return {}
    prod_version = prod_versions[0]
    run_id = prod_version.run_id
    run = client.get_run(run_id)
    metrics = run.data.metrics
    context.add_output_metadata({k: MetadataValue.float(v) for k, v in metrics.items() if isinstance(v, float)})
    context.add_output_metadata({"run_id": MetadataValue.text(run_id), "model_version": MetadataValue.text(prod_version.version)})
    return metrics
