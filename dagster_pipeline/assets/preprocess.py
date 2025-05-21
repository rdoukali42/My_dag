import pandas as pd
from dagster import asset
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OrdinalEncoder
from dagster import AssetMaterialization, Output



@asset
def split_data(prepare_data):
    file = prepare_data
    dt = file.drop(['popularity'], axis=1)
    pr = file['popularity']
    dt_train, dt_test, pr_train, pr_test = train_test_split(dt, pr, test_size=0.2, random_state=42)
    return dt_train, dt_test, pr_train, pr_test

@asset
def preprocess(split_data):
    dt_train, dt_test, pr_train, pr_test = split_data
    numeric_features = ['danceability', 'energy', 'key', 'loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature', 'mode']
    categorical_features = ['artist_name', 'track_name', 'genre']
    ordinal_features = ['year']

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), numeric_features),
            ('cat', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1), categorical_features),
            ('ord', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1), ordinal_features)
        ]
    )
    return preprocessor
