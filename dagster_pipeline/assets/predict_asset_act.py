import pandas as pd
from dagster import asset, AssetExecutionContext, AssetMaterialization, MetadataValue, AssetKey

@asset
def predict_asset_act(
    context: AssetExecutionContext,
    prepare_data: pd.DataFrame,
    train_XGBC
) -> float:
    df = prepare_data
    if not isinstance(df, pd.DataFrame):
        context.log.error("Input to predict_asset_act is not a DataFrame.")
        return None
    if "popularity" not in df.columns:
        context.log.error("No 'popularity' column found in input data.")
        return None

    X = df.drop(columns=["popularity"])
    y_true = df["popularity"]

    try:
        model = train_XGBC  # Should be a fitted model object
        y_pred = model.predict(X)
        percent = (y_pred == y_true).mean() * 100
        context.log.info(f"Percent correct predictions on actual data: {percent:.2f}%")
        context.log_event(AssetMaterialization(
            asset_key="predict_asset_act",
            description="Percent correct predictions on actual data using trained model",
            metadata={"percent": percent}
        ))
        context.add_output_metadata({
            "Prediction Percent": MetadataValue.float(percent),
        })
        return percent
    except Exception as e:
        context.log.error(f"Failed to predict with train_XGBC model: {e}")
        return None
