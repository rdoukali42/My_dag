# import pandas as pd
# from dagster import asset
# from sklearn.model_selection import train_test_split
# from sklearn.compose import ColumnTransformer
# from sklearn.preprocessing import StandardScaler, OrdinalEncoder
# from sklearn.pipeline import Pipeline
# from xgboost import XGBClassifier
# import matplotlib.pyplot as plt
# import seaborn as sns
# from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix
# from dagster import AssetMaterialization, Output
# # from dagster_pipeline.io_managers.local_data_io_manager import local_data_io_manager
# import io
# import base64

# # @asset
# # def print_spotify_csv_head():
# #     file_path = "/Users/level3/TrackAI/BikeEnv/bikes_rent/src/spotify_data.csv"
# #     df = pd.read_csv(file_path)
# #     print(df.head())

# @asset
# def load_data():
#     # file_path = context.op_config["file_path"]
#     file_path = "/Users/level3/TrackAI/BikeEnv/bikes_rent/src/spotify_data.csv"
#     df = pd.read_csv(file_path)
#     df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
#     df = df.drop(['track_id'], axis=1)
#     df = df[df['year'] != 2023]
#     df['popularity'] = (df['popularity'] >= 50).astype(int)
#     return df

# @asset
# def split_data(load_data):
#     file = load_data
#     dt = file.drop(['popularity'], axis=1)
#     pr = file['popularity']
#     dt_train, dt_test, pr_train, pr_test = train_test_split(dt, pr, test_size=0.2, random_state=42)
#     return dt_train, dt_test, pr_train, pr_test

# @asset
# def preprocess(split_data):
#     dt_train, dt_test, pr_train, pr_test = split_data
#     numeric_features = ['danceability', 'energy', 'key', 'loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature', 'mode']
#     categorical_features = ['artist_name', 'track_name', 'genre']
#     ordinal_features = ['year']

#     preprocessor = ColumnTransformer(
#         transformers=[
#             ('num', StandardScaler(), numeric_features),
#             ('cat', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1), categorical_features),
#             ('ord', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1), ordinal_features)
#         ]
#     )
#     return preprocessor, dt_train, dt_test, pr_train, pr_test

# @asset
# def train_XGBC(split_data):
#     # preprocessor, dt_train, dt_test, pr_train, pr_test = preprocess
#     dt_train, dt_test, pr_train, pr_test = split_data
#     numeric_features = ['danceability', 'energy', 'key', 'loudness', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature', 'mode']
#     categorical_features = ['artist_name', 'track_name', 'genre']
#     ordinal_features = ['year']

#     preprocessor = ColumnTransformer(
#         transformers=[
#             ('num', StandardScaler(), numeric_features),
#             ('cat', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1), categorical_features),
#             ('ord', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1), ordinal_features)
#         ]
#     )
#     weight = ((pr_train == 0).sum() / (pr_train == 1).sum())
#     model = Pipeline([
#         ('preprocessor', preprocessor),
#         ('classifier', XGBClassifier(
#             n_estimators=200,
#             learning_rate=0.01,
#             max_depth=6,
#             subsample=0.8,
#             colsample_bytree=0.8,
#             use_label_encoder=False,
#             eval_metric='aucpr',
#             scale_pos_weight=weight
#         ))
#     ])
#     model.fit(dt_train, pr_train)
#     return model

# @asset
# def evaluate_spotify_model(context, train_XGBC, split_data):
#     model = train_XGBC
#     _, dt_test, _, pr_test = split_data
#     y_pred = model.predict(dt_test)
#     y_prob = model.predict_proba(dt_test)[:, 1] if hasattr(model, "predict_proba") else None

#     # Ensure pr_test and y_pred are both 1D arrays of int (0 or 1)
#     pr_test = pd.Series(pr_test).astype(int).values
#     y_pred = pd.Series(y_pred).astype(int).values
#     if y_prob is not None:
#         y_prob = pd.Series(y_prob).astype(float).values

#     # Metrics
#     accuracy = accuracy_score(pr_test, y_pred)
#     precision = precision_score(pr_test, y_pred)
#     recall = recall_score(pr_test, y_pred)
#     f1 = f1_score(pr_test, y_pred)
#     roc_auc = roc_auc_score(pr_test, y_prob) if y_prob is not None else None

#     # Confusion matrix plot
#     cm = confusion_matrix(pr_test, y_pred, normalize='true')
#     plt.figure(figsize=(6, 4))
#     sns.heatmap(cm, annot=True, fmt=".2f", cmap="Blues")
#     plt.xlabel("Predicted")
#     plt.ylabel("Actual")
#     plt.title("Confusion Matrix")
#     buf = io.BytesIO()
#     plt.savefig(buf, format='png')
#     plt.close()
#     buf.seek(0)
#     img_bytes = buf.read()
#     img_b64 = base64.b64encode(img_bytes).decode('utf-8')
#     context.log_event(AssetMaterialization(
#         asset_key="confusion_matrix_plot",
#         description="Confusion matrix plot as base64 PNG.",
#         metadata={"image/png;base64": img_b64}
#     ))

#     # Log metrics to Dagster
#     metrics = {
#         "accuracy": accuracy,
#         "precision": precision,
#         "recall": recall,
#         "f1_score": f1,
#         "roc_auc": roc_auc
#     }
#     for k, v in metrics.items():
#         context.log_event(AssetMaterialization(
#             asset_key=k,
#             description=f"{k} metric value",
#             metadata={"value": v}
#         ))

#     return Output(metrics, metadata=metrics)

