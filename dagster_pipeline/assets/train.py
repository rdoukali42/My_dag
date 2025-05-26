import pandas as pd
from dagster import asset
import mlflow
from sklearn.pipeline import Pipeline
from xgboost import XGBClassifier
from dagster import AssetMaterialization, Output, AssetExecutionContext
import io
import base64

@asset(required_resource_keys={"mlflow"},
       group_name="Model_Training",
       description="Train an XGBoost classifier on the preprocessed data.")
def train_XGBC(context: AssetExecutionContext, preprocess, split_data_train) -> Pipeline:
    dt_train, pr_train = split_data_train
    preprocessor = preprocess
    weight = ((pr_train == 0).sum() / (pr_train == 1).sum())
    model = Pipeline([
        ('preprocessor', preprocessor),
        ('classifier', XGBClassifier(
            n_estimators=200,
            learning_rate=0.01,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            use_label_encoder=False,
            eval_metric='aucpr',
            scale_pos_weight=weight
        ))
    ])
    # Start MLflow run and autolog
    with context.resources.mlflow.start_run(run_name="train_XGBC"):
        mlflow.sklearn.autolog()
        model.fit(dt_train, pr_train)
        context.log.info("Model trained and logged to MLflow.")
    return model