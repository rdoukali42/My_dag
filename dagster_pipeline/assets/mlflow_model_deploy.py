import os
import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
import mlflow.pyfunc
from mlflow.models.signature import infer_signature
from mlflow.exceptions import MlflowException
from mlflow.tracking import MlflowClient
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from typing import Dict, Any, List, Tuple, Optional
import logging

from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    AssetIn,
    MetadataValue
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
MODEL_NAME = "spotify_popularity_predictor"
MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_EXPERIMENT_NAME = "spotify_popularity_prediction"
METRIC_FOR_COMPARISON = "rmse"  # Metric to use for model comparison (lower is better)


def setup_mlflow():
    """Set up MLflow tracking server connection."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    
    # Create the experiment if it doesn't exist
    try:
        experiment = mlflow.get_experiment_by_name(MLFLOW_EXPERIMENT_NAME)
        if experiment is None:
            mlflow.create_experiment(MLFLOW_EXPERIMENT_NAME)
        mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    except Exception as e:
        logger.error(f"Error setting up MLflow: {e}")
        raise


def log_metrics(y_true, y_pred) -> Dict[str, float]:
    """Calculate and return model performance metrics."""
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    
    return {
        "rmse": rmse,
        "mae": mae,
        "r2": r2
    }


def get_best_production_model() -> Tuple[Optional[str], Optional[Dict[str, float]]]:
    """
    Get the current production model and its metrics.
    
    Returns:
        Tuple of (model_uri, metrics_dict) or (None, None) if no production model exists
    """
    client = MlflowClient()
    
    try:
        # Get production model version if it exists
        try:
            production_model = client.get_latest_versions(MODEL_NAME, stages=["Production"])
            if not production_model:
                logger.info("No production model found")
                return None, None
                
            model_version = production_model[0].version
            run_id = production_model[0].run_id
            
            # Get run that produced this model version
            run = client.get_run(run_id)
            
            # Extract metrics
            metrics = {
                "rmse": run.data.metrics.get("rmse", float('inf')),
                "mae": run.data.metrics.get("mae", float('inf')),
                "r2": run.data.metrics.get("r2", 0)
            }
            
            model_uri = f"models:/{MODEL_NAME}/{model_version}"
            return model_uri, metrics
            
        except MlflowException as e:
            if "RESOURCE_DOES_NOT_EXIST" in str(e):
                logger.info(f"Model {MODEL_NAME} not yet registered")
                return None, None
            else:
                raise
    
    except Exception as e:
        logger.error(f"Error getting production model: {e}")
        return None, None


def compare_and_promote_model(run_id: str, metrics: Dict[str, float]) -> bool:
    """
    Compare the new model against the production model and promote if better.
    
    Args:
        run_id: MLflow run ID of the new model
        metrics: Dictionary of metrics for the new model
        
    Returns:
        bool: True if the new model was promoted to production, False otherwise
    """
    client = MlflowClient()
    
    try:
        # Get current production model
        current_model_uri, current_metrics = get_best_production_model()
        
        # If no current production model, register and promote the new one
        if current_model_uri is None:
            logger.info("No production model found. Registering new model as production.")
            
            # Register model
            model_version = mlflow.register_model(
                model_uri=f"runs:/{run_id}/model",
                name=MODEL_NAME
            )
            
            # Transition to Production
            client.transition_model_version_stage(
                name=MODEL_NAME,
                version=model_version.version,
                stage="Production"
            )
            
            logger.info(f"Model {MODEL_NAME} version {model_version.version} promoted to Production")
            return True
        
        # Compare new model with current production model
        # Lower RMSE is better
        current_comparison_metric = current_metrics.get(METRIC_FOR_COMPARISON, float('inf'))
        new_comparison_metric = metrics.get(METRIC_FOR_COMPARISON, float('inf'))
        
        logger.info(f"Current production model {METRIC_FOR_COMPARISON}: {current_comparison_metric}")
        logger.info(f"New model {METRIC_FOR_COMPARISON}: {new_comparison_metric}")
        
        # For RMSE and MAE, lower is better
        if METRIC_FOR_COMPARISON in ["rmse", "mae"]:
            is_better = new_comparison_metric < current_comparison_metric
        # For R2, higher is better
        else:
            is_better = new_comparison_metric > current_comparison_metric
            
        if is_better:
            # Register new model
            model_version = mlflow.register_model(
                model_uri=f"runs:/{run_id}/model",
                name=MODEL_NAME
            )
            
            # Transition current Production model to Archived
            for version in client.get_latest_versions(MODEL_NAME, stages=["Production"]):
                client.transition_model_version_stage(
                    name=MODEL_NAME,
                    version=version.version,
                    stage="Archived"
                )
            
            # Transition new model to Production
            client.transition_model_version_stage(
                name=MODEL_NAME,
                version=model_version.version,
                stage="Production",
                archive_existing_versions=True  # Archive any existing versions in the Production stage
            )
            
            logger.info(f"Model {MODEL_NAME} version {model_version.version} promoted to Production")
            logger.info(f"Performance improvement: {current_comparison_metric - new_comparison_metric} {METRIC_FOR_COMPARISON}")
            return True
        else:
            logger.info(f"New model did not outperform current production model.")
            
            # Optional: Still register the model but leave in staging
            model_version = mlflow.register_model(
                model_uri=f"runs:/{run_id}/model",
                name=MODEL_NAME
            )
            
            logger.info(f"Model {MODEL_NAME} version {model_version.version} registered but not promoted")
            return False
    
    except Exception as e:
        logger.error(f"Error in model comparison and promotion: {e}")
        return False


@asset(
    deps=["preprocess", "train_XGBC"],
    description="Evaluate the model, log to MLflow, and promote to production if improved"
)
def evaluate_and_deploy_model(context: AssetExecutionContext, X_test, y_test, model) -> Dict[str, float]:
    """
    Evaluate model performance, log to MLflow, and handle deployment logic.
    Compares against current production model and promotes if better.
    """
    context.log.info("Evaluating model and handling deployment...")
    
    # Setup MLflow
    setup_mlflow()
    
    # Make predictions with the model
    y_pred = model.predict(X_test)
    
    # Calculate metrics
    metrics = log_metrics(y_test, y_pred)
    
    # Generate model signature for schema validation during serving
    signature = infer_signature(X_test, y_pred)
    
    # Start an MLflow run
    with mlflow.start_run() as run:
        run_id = run.info.run_id
        
        # Log model
        mlflow.sklearn.log_model(
            sk_model=model,
            artifact_path="model",
            signature=signature,
            registered_model_name=MODEL_NAME
        )
        
        # Log metrics
        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, metric_value)
            context.log.info(f"{metric_name}: {metric_value}")
        
        # Log parameters
        try:
            params = model.get_params()
            mlflow.log_params(params)
        except:
            context.log.warning("Could not log model parameters")
        
        # Log feature importance if available
        try:
            if hasattr(model, 'feature_importances_'):
                feature_names = X_test.columns
                importances = model.feature_importances_
                importance_df = pd.DataFrame({
                    'feature': feature_names,
                    'importance': importances
                }).sort_values('importance', ascending=False)
                
                # Log feature importance as a JSON artifact
                importance_path = "feature_importance.csv"
                importance_df.to_csv(importance_path, index=False)
                mlflow.log_artifact(importance_path)
                os.remove(importance_path)
        except Exception as e:
            context.log.warning(f"Could not log feature importance: {e}")
        
        # Compare with production model and promote if better
        promoted = compare_and_promote_model(run_id, metrics)
        
        # Add promotion status to context metadata
        context.add_output_metadata({
            "model_run_id": MetadataValue.text(run_id),
            "promoted_to_production": MetadataValue.bool(promoted),
            "rmse": MetadataValue.float(metrics["rmse"]),
            "mae": MetadataValue.float(metrics["mae"]),
            "r2": MetadataValue.float(metrics["r2"])
        })
    
    return metrics


@asset(
    deps=["evaluate_and_deploy_model"],
    description="Serve the latest production model"
)
def serve_model(context: AssetExecutionContext) -> str:
    """
    Asset to serve the latest production model.
    This would typically be a separate service, but represented as an asset here.
    
    In a real-world scenario, this might:
    - Deploy the model to a serving platform
    - Update API endpoints
    - Restart services
    """
    # Get current production model URI
    model_uri, _ = get_best_production_model()
    
    if model_uri is None:
        message = "No production model available to serve"
        context.log.warning(message)
        return message
    
    context.log.info(f"Serving model: {model_uri}")
    
    # In a real application, this would trigger deployment to a serving platform
    # For demonstration, we'll just load the model to verify it works
    try:
        model = mlflow.pyfunc.load_model(model_uri)
        context.log.info(f"Successfully loaded model {model_uri}")
        
        # Add serving info to context metadata
        context.add_output_metadata({
            "model_uri": MetadataValue.text(model_uri),
            "serving_status": MetadataValue.text("active"),
        })
        
        message = f"Model {model_uri} is now serving"
        return message
    
    except Exception as e:
        error_message = f"Error serving model {model_uri}: {e}"
        context.log.error(error_message)
        
        context.add_output_metadata({
            "model_uri": MetadataValue.text(model_uri),
            "serving_status": MetadataValue.text("error"),
            "error": MetadataValue.text(str(e))
        })
        
        return error_message