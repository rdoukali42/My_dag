import pandas as pd
from dagster import asset
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OrdinalEncoder
from sklearn.pipeline import Pipeline
from xgboost import XGBClassifier
from dagster import AssetMaterialization, Output, AssetExecutionContext
import io
import base64

@asset
def train_XGBC(context: AssetExecutionContext, preprocess, split_data) -> Pipeline:
    dt_train, dt_test, pr_train, pr_test = split_data
    preprocessor = preprocess
    weight = ((pr_train == 0).sum() / (pr_train == 1).sum())
    context.log.info(f"Weight for class 1: {weight}")
    context.log.info(f"Shape of training data: {dt_train.shape}")
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
    model.fit(dt_train, pr_train)
    return model