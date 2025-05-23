from dagster import get_dagster_logger
from dagster import AssetMaterialization
from dagster import AssetIn
import os
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
import mlflow.pyfunc
from mlflow.models.signature import infer_signature
from mlflow.exceptions import MlflowException
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from typing import Dict, Any, List, Tuple, Optional
import logging
from mlflow.tracking import MlflowClient

from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue
)

from dagster_pipeline.utils.mlflow_utils import (
    log_metrics,
    get_best_production_model,
    compare_and_promote_model,

    # Remove local constants, use centralized config from mlflow_utils
    MODEL_NAME, MLFLOW_TRACKING_URI, MLFLOW_EXPERIMENT_NAME, METRIC_FOR_COMPARISON
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@asset(
    required_resource_keys={"mlflow"},
    ins={"production_metrics": AssetIn("get_production_model_metrics")},
    description="Evaluate the model, log to MLflow, and promote to production if improved"
)
def evaluate_and_deploy_model(context: AssetExecutionContext, train_XGBC, split_data, production_metrics) -> Dict[str, float]:
    context.log.info("Evaluating model and handling deployment...")

    # Unpack split_data
    _, X_test, _, y_test = split_data
    # Ensure y_test and y_pred are both 1D arrays of int (0 or 1)
    y_test = pd.Series(y_test).astype(int).values
    # Make predictions with the model
    y_pred = train_XGBC.predict(X_test)
    y_pred = pd.Series(y_pred).astype(int).values
    y_prob = train_XGBC.predict_proba(X_test)[:, 1] if hasattr(train_XGBC, "predict_proba") else None
    if y_prob is not None:
        y_prob = pd.Series(y_prob).astype(float).values
    # Calculate metrics (matching evaluate_spotify_model)
    metrics = log_metrics(y_test, y_pred, y_prob)

    # Generate model signature for schema validation during serving
    signature = infer_signature(X_test, y_pred)

    # Get current best pr_auc from production_metrics asset
    current_auc = production_metrics.get("pr_auc", 0.0)
    new_pr_auc = metrics.get("pr_auc", 0.0)
    context.log.info(f"Current production PR AUC: {current_auc}, New PR AUC: {new_pr_auc}")

    promoted = False
    run_id = None
    # Only log/register to MLflow if new model is better
    if new_pr_auc > current_auc:
        with context.resources.mlflow.start_run(run_name="evaluate_and_deploy_model") as run:
            run_id = run.info.run_id
            # Log model
            mlflow.sklearn.log_model(
                sk_model=train_XGBC,
                artifact_path="model",
                signature=signature,
                registered_model_name=MODEL_NAME
            )
            # Log metrics
            for metric_name, metric_value in metrics.items():
                context.resources.mlflow.log_metric(metric_name, metric_value)
                context.log.info(f"{metric_name}: {metric_value}")
            # Log parameters
            try:
                params = train_XGBC.get_params()
                context.resources.mlflow.log_param("model_params", str(params))
            except Exception:
                context.log.warning("Could not log model parameters")
            # Log feature importance if available
            try:
                if hasattr(train_XGBC, 'feature_importances_'):
                    feature_names = X_test.columns
                    importances = train_XGBC.feature_importances_
                    importance_df = pd.DataFrame({
                        'feature': feature_names,
                        'importance': importances
                    }).sort_values('importance', ascending=False)
                    importance_path = "feature_importance.csv"
                    importance_df.to_csv(importance_path, index=False)
                    context.resources.mlflow.log_artifact(importance_path)
                    os.remove(importance_path)
            except Exception as e:
                context.log.warning(f"Could not log feature importance: {e}")
            # Register and promote new model
            promoted = compare_and_promote_model(context, run_id, metrics)
    else:
        context.log.info("Model not logged to MLflow because it did not outperform the current production model.")

    # Add promotion status to context metadata
    context.add_output_metadata({
        "model_run_id": MetadataValue.text(run_id) if run_id else MetadataValue.text("not_logged"),
        "promoted_to_production": MetadataValue.bool(promoted),
        **{k: MetadataValue.float(v) for k, v in metrics.items() if isinstance(v, float)}
    })

    return metrics


@asset(
    deps=["evaluate_and_deploy_model"],
    required_resource_keys={"mlflow"},
    description="Serve the latest production model"
)
def serve_model(context: AssetExecutionContext) -> str:
    model_uri, metrics = get_best_production_model(context)
    pr_auc = metrics.get("pr_auc") if metrics else None
    if pr_auc is None:
        message = "No production model available to serve (no pr_auc in .env)"
        context.log.warning(message)
        return message
    context.log.info(f"Serving model with PR AUC: {pr_auc}")
    context.add_output_metadata({
        "serving_pr_auc": MetadataValue.float(pr_auc),
        "serving_status": MetadataValue.text("active"),
    })
    message = f"Model with PR AUC {pr_auc} is now serving"
    return message