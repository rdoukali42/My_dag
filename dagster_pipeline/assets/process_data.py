# # assets/process_data.py
# import os
# import pandas as pd
# from dagster import asset
# from dotenv import load_dotenv
# from datetime import datetime

# load_dotenv() 

# @asset(required_resource_keys={"lakefs"})
# def process_data(context, prepare_data: pd.DataFrame):
#     fs = context.resources.lakefs
#     repo = os.getenv("LAKEFS_REPOSITORY")
#     base_branch = os.getenv("LAKEFS_DEFAULT_BRANCH")
#     output_path = os.getenv("LAKEFS_PROCESSED_PATH", "data/processed/v1/processed_data.csv")

#     # Create new branch for this processing run
#     branch_name = f"processed/{datetime.now().strftime('%Y%m%d_%H%M%S')}"
#     if hasattr(fs, 'client') and hasattr(fs.client, 'branches'):
#         fs.client.branches.create_branch(
#             repository=repo,
#             branch_creation={
#                 "name": branch_name,
#                 "source": base_branch
#             }
#         )
#     else:
#         context.log.warning("lakeFS client does not support branch creation via this API. Skipping branch creation.")

#     # Save processed data
#     with fs.open(f"lakefs://{repo}/{branch_name}/{output_path}", "w") as f:
#         prepare_data.to_csv(f, index=False)

#     context.log.info(f"Saved processed data to {repo}/{branch_name}/{output_path}")
#     return branch_name