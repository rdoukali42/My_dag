# filepath: /Users/level3/TrackAI/mlops/dagster_pipeline/dagster_pipeline/utils/mlflow_utils.py

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
from dotenv import load_dotenv

# Constants (centralized config)
env_path = os.path.join(os.path.dirname(__file__), '../../.env')
load_dotenv(env_path)

MODEL_NAME = os.getenv("MLFLOW_MODEL_NAME", "spotify_popularity_predictor")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5010")
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "Dagster_Mlflow")
METRIC_FOR_COMPARISON = os.getenv("METRIC_FOR_COMPARISON", "pr_auc")


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
    """Return the best production model's URI and metrics (currently just pr_auc from .env)."""
    env_path = os.path.join(os.path.dirname(__file__), '../../.env')
    current_auc = 0.0
    with open(env_path, 'r') as f:
        for line in f:
            if line.strip().startswith("ACTUAL_MODEL_PR_AUC"):
                try:
                    current_auc = float(line.strip().split('=')[1])
                except Exception:
                    current_auc = 0.0
    return None, {METRIC_FOR_COMPARISON: current_auc}


def compare_and_promote_model(context, run_id: str, metrics: Dict[str, float]) -> bool:
    """Promote model to production if new metric is better than current best in .env, and update .env."""
    client = MlflowClient()
    new_pr_auc = metrics.get(METRIC_FOR_COMPARISON)
    if new_pr_auc is None:
        context.log.error(f"No {METRIC_FOR_COMPARISON} found in metrics. Skipping promotion.")
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
    context.log.info(f"Current best {METRIC_FOR_COMPARISON}: {current_auc}, New {METRIC_FOR_COMPARISON}: {new_pr_auc}")

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
        context.log.info(f"Updated .env with new {METRIC_FOR_COMPARISON}: {new_pr_auc}")
        return True
    else:
        context.log.info(f"No update to .env; new model did not outperform current best {METRIC_FOR_COMPARISON}.")
        return False
