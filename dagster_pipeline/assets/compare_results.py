# import os
# from dotenv import load_dotenv
# from dagster import asset, AssetMaterialization, Output
# from dagster_pipeline.assets.evaluate import evaluate_spotify_model

# @asset(required_resource_keys={"mlflow"})
# def compare_and_update_model(context, evaluate_spotify_model):
#     """
#     Compare new model's PR AUC to the current best in .env. If better, update .env and (optionally) re-register model in MLflow.
#     """
#     # Load .env
#     load_dotenv(override=True)
#     env_path = os.path.join(os.path.dirname(__file__), '../../.env')
#     pr_auc_new = evaluate_spotify_model.get("pr_auc")

#     if pr_auc_new is None:
#         context.log.error("No pr_auc found in evaluate_spotify_model output. Skipping model comparison and .env update.")
#         return Output(None, metadata={"updated": False, "reason": "No pr_auc in input"})

#     # Read current best PR AUC from .env
#     with open(env_path, 'r') as f:
#         lines = f.readlines()
#     current_auc = 0.0
#     for line in lines:
#         if line.strip().startswith("ACTUAL_MODEL_PR_AUC"):
#             try:
#                 current_auc = float(line.strip().split('=')[1])
#             except Exception:
#                 current_auc = 0.0
    
#     context.log.info(f"Current best PR AUC: {current_auc}, New PR AUC: {pr_auc_new}")
#     if pr_auc_new > current_auc:
#         # Update .env file
#         new_lines = []
#         found = False
#         for line in lines:
#             if line.strip().startswith("ACTUAL_MODEL_PR_AUC"):
#                 new_lines.append(f"ACTUAL_MODEL_PR_AUC={pr_auc_new}\n")
#                 found = True
#             else:
#                 new_lines.append(line)
#         if not found:
#             new_lines.append(f"ACTUAL_MODEL_PR_AUC={pr_auc_new}\n")
#         with open(env_path, 'w') as f:
#             f.writelines(new_lines)
#         context.log.info(f"Updated .env with new PR AUC: {pr_auc_new}")
#         # Optionally, trigger model registration in MLflow here
#         # context.resources.mlflow.log_metric("best_pr_auc", pr_auc_new)
#         context.log_event(AssetMaterialization(asset_key="compare_and_update_model", description="New best model found and .env updated", metadata={"pr_auc": pr_auc_new}))
#         return Output(pr_auc_new, metadata={"updated": True, "pr_auc": pr_auc_new})
#     else:
#         context.log.info("No update to .env; new model did not outperform current best.")
#         context.log_event(AssetMaterialization(asset_key="compare_and_update_model", description="Model not updated", metadata={"pr_auc": pr_auc_new}))
#         return Output(pr_auc_new, metadata={"updated": False, "pr_auc": pr_auc_new})
