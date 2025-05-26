import pandas as pd
from dagster import asset, AssetExecutionContext

@asset(group_name="Data_Preparation",
       description="Prepare data by cleaning and transforming the loaded DataFrame.")
def prepare_data(context: AssetExecutionContext, load_data: pd.DataFrame) -> pd.DataFrame:
    df = load_data.copy()
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    if "track_id" in df.columns:
        df = df.drop(["track_id"], axis=1)
    if "year" in df.columns:
        df = df[df["year"] != 2023]
    if "popularity" in df.columns:
        df["popularity"] = (df["popularity"] >= 50).astype(int)
    context.log.info(f"Prepared data shape: {df.shape}")
    context.log.info(f"Prepared data shape: {df.head(1)}")
    return df
