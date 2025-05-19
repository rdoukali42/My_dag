import pandas as pd
from dagster import asset
from dagster import AssetMaterialization, Output
import io
import base64

@asset
def load_data():
    # file_path = context.op_config["file_path"]
    file_path = "/Users/level3/TrackAI/BikeEnv/bikes_rent/src/spotify_data.csv"
    df = pd.read_csv(file_path)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.drop(['track_id'], axis=1)
    df = df[df['year'] != 2023]
    df['popularity'] = (df['popularity'] >= 50).astype(int)
    return df