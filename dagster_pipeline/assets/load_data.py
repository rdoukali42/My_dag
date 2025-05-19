import pandas as pd
from dagster import asset, Field

##set asset to read the file from the io_manager path

@asset(
    config_schema={
        "file_path": Field(
            str,
            description="Path to CSV file to process",
            default_value="/Users/level3/TrackAI/BikeEnv/bikes_rent/src/spotify_data.csv"
        )
    }
)
def load_data(context):
    file_path = context.op_config["file_path"]
    # file_path = "/Users/level3/TrackAI/BikeEnv/bikes_rent/src/spotify_data.csv"
    df = pd.read_csv(file_path)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.drop(['track_id'], axis=1)
    df = df[df['year'] != 2023]
    df['popularity'] = (df['popularity'] >= 50).astype(int)
    return df