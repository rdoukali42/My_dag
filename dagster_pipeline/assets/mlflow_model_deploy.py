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

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants (now from .env)
MODEL_NAME = "spotify_popularity_predictor"  # os.environ.get("MLFLOW_MODEL_NAME", "spotify_popularity_predictor")
MLFLOW_TRACKING_URI = "http://localhost:5010"  # os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5010")
MLFLOW_EXPERIMENT_NAME = "Dagster_Mlflow"  # os.environ.get("MLFLOW_EXPERIMENT_NAME", "Dagster_Mlflow")
METRIC_FOR_COMPARISON = "pr_auc"  # os.environ.get("MLFLOW_METRIC_FOR_COMPARISON", "rmse")  # e.g. "pr_auc" or "rmse"


def log_metrics(y_true, y_pred, y_prob) -> Dict[str, float]:
    """Calculate and return model performance metrics as in evaluate_spotify_model."""
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, precision_recall_curve, auc
    accuracy = accuracy_score(y_true, y_pred)
    precision_val = precision_score(y_true, y_pred)
    recall_val = recall_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred)
    roc_auc = roc_auc_score(y_true, y_prob) if y_prob is not None else None
    precision_curve, recall_curve, _ = precision_recall_curve(y_true, y_prob)
    pr_auc = auc(recall_curve, precision_curve)
    return {
        "accuracy": accuracy,
        "precision": precision_val,
        "recall": recall_val,
        "f1_score": f1,
        "roc_auc": roc_auc,
        "pr_auc": pr_auc
    }


def get_best_production_model(context) -> Tuple[Optional[str], Optional[Dict[str, float]]]:

    # Use .env for ACTUAL_MODEL_PR_AUC as in your pipeline
    env_path = os.path.join(os.path.dirname(__file__), '../../.env')
    current_auc = 0.0
    with open(env_path, 'r') as f:
        for line in f:
            if line.strip().startswith("ACTUAL_MODEL_PR_AUC"):
                try:
                    current_auc = float(line.strip().split('=')[1])
                except Exception:
                    current_auc = 0.0
    # If you want to return a model_uri, you can use MLflow model registry if needed
    return None, {"pr_auc": current_auc}


def compare_and_promote_model(context, run_id: str, metrics: Dict[str, float]) -> bool:
    client = MlflowClient()
    model_name = context.resources.mlflow.experiment_name
    metric_name = "pr_auc"
    new_pr_auc = metrics.get(metric_name)
    if new_pr_auc is None:
        context.log.error("No pr_auc found in metrics. Skipping promotion.")
        return False

    env_path = os.path.join(os.path.dirname(__file__), '../../.env')
    current_auc = 0.0
    with open(env_path, 'r') as f:
        for line in f:
            if line.strip().startswith("ACTUAL_MODEL_PR_AUC"):
                try:
                    current_auc = float(line.strip().split('=')[1])
                except Exception:
                    current_auc = 0.0
    context.log.info(f"Current best PR AUC: {current_auc}, New PR AUC: {new_pr_auc}")

    if new_pr_auc > current_auc:
        # Find the latest model version just registered (stage=None)
        latest_versions = client.get_latest_versions(MODEL_NAME, stages=["None"])
        if latest_versions:
            version = latest_versions[0].version
            client.transition_model_version_stage(
                name=MODEL_NAME,
                version=version,
                stage="Production",
                archive_existing_versions=True
            )
            context.log.info(f"Promoted model version {version} to Production.")
        else:
            context.log.warning("No model version found to promote.")
            return False
        # Update .env file
        with open(env_path, 'r') as f:
            lines = f.readlines()
        new_lines = []
        found = False
        for line in lines:
            if line.strip().startswith("ACTUAL_MODEL_PR_AUC"):
                new_lines.append(f"ACTUAL_MODEL_PR_AUC={new_pr_auc}\n")
                found = True
            else:
                new_lines.append(line)
        if not found:
            new_lines.append(f"ACTUAL_MODEL_PR_AUC={new_pr_auc}\n")
        with open(env_path, 'w') as f:
            f.writelines(new_lines)
        context.log.info(f"Updated .env with new PR AUC: {new_pr_auc}")
        return True
    else:
        context.log.info("No update to .env; new model did not outperform current best.")
        return False


@asset(
    required_resource_keys={"mlflow"},
    ins={"production_metrics": AssetIn("get_production_model_metrics")},
    description="Evaluate the model, log to MLflow, and promote to production if improved"
)
def evaluate_and_deploy_model(context: AssetExecutionContext, train_XGBC, split_data, production_metrics) -> Dict[str, float]:
    """
    Evaluate model performance, log to MLflow, and handle deployment logic.
    Compares against current production model and promotes if better.
    Only logs and registers the model to MLflow if it is the best (by pr_auc).
    """
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