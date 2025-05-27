import os
import pandas as pd
from dagster import asset, AssetExecutionContext, AssetMaterialization, MetadataValue, AssetKey
import mlflow.pyfunc

@asset(required_resource_keys={"lakefs"},
       group_name="Model_Prediction",
       description="Predict the percentage of correct predictions using a trained model on new data.")
def predict_percent(context: AssetExecutionContext, prepare_data) -> float:
    # Use the loaded data (should be a DataFrame)
    df = prepare_data
    if not isinstance(df, pd.DataFrame):
        context.log.error("Input to predict_percent is not a DataFrame.")
        return True
    if "popularity" not in df.columns:
        context.log.error("No 'popularity' column found in input data. Cannot calculate percent correct.")
        return True
    X = df.drop(columns=["popularity"])
    y_true = df["popularity"]
    # Load latest production model from MLflow
    model_name = os.getenv("MLFLOW_MODEL_NAME") or os.getenv("MLFLOW_EXPERIMENT_NAME")
    if not model_name:
        context.log.error("MLFLOW_MODEL_NAME or MLFLOW_EXPERIMENT_NAME not set. Cannot load model.")
        return True
    try:
        model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}/Production")
        y_pred = model.predict(X)
        # If predictions are probabilities, convert to class labels
        import numpy as np
        if hasattr(y_pred, "shape") and len(y_pred.shape) > 1 and y_pred.shape[1] > 1:
            y_pred = np.argmax(y_pred, axis=1)
        percent = (y_pred == y_true).mean() * 100
        context.log.info(f"Percent correct predictions: {percent:.2f}%")
        context.log_event(AssetMaterialization(
            asset_key="predict_percent",
            description="Percent correct predictions on new data",
            metadata={"percent": percent}
        ))
        context.add_output_metadata({
        "Predictions Correct Percent": MetadataValue.float(percent),
    })
        return percent
    except Exception as e:
        context.log.error(f"Failed to predict or calculate percent: {e}")
        return True
