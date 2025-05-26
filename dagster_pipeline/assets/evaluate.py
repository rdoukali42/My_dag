# import pandas as pd
# from dagster import asset
# from sklearn.model_selection import train_test_split
# from sklearn.compose import ColumnTransformer
# from sklearn.preprocessing import StandardScaler, OrdinalEncoder
# from sklearn.pipeline import Pipeline
# from xgboost import XGBClassifier
# import matplotlib.pyplot as plt
# import seaborn as sns
# from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, confusion_matrix, precision_recall_curve, auc
# from dagster import AssetMaterialization, Output
# import io
# import base64


# @asset(required_resource_keys={"lakefs"})
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
#     precision_val = precision_score(pr_test, y_pred)
#     recall_val = recall_score(pr_test, y_pred)
#     f1 = f1_score(pr_test, y_pred)
#     roc_auc = roc_auc_score(pr_test, y_prob) if y_prob is not None else None
#     # Compute PR AUC correctly
#     precision_curve, recall_curve, _ = precision_recall_curve(pr_test, y_prob)
#     pr_auc = auc(recall_curve, precision_curve)


#     # Log metrics to Dagster
#     metrics = {
#         "accuracy": accuracy,
#         "precision": precision_val,
#         "recall": recall_val,
#         "f1_score": f1,
#         "roc_auc": roc_auc,
#         "pr_auc": pr_auc
#     }
#     for k, v in metrics.items():
#         context.log_event(AssetMaterialization(
#             asset_key=k,
#             description=f"{k} metric value",
#             metadata={"value": v}
#         ))

#     return Output(value = None, metadata=metrics)