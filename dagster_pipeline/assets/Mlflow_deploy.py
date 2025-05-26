# import mlflow
# from dagster import asset

# @asset(required_resource_keys={"mlflow"})
# def mlflow_deploy_model(context, train_XGBC):
#     with context.resources.mlflow.start_run(run_name="model_deploy"):
#         # mlflow.sklearn.log_model(train_XGBC, "model")
#         context.log.info("Model deployed to MLflow.")