import os
from dagster import asset, AssetExecutionContext, AutoMaterializePolicy
from dotenv import load_dotenv
from dagster_pipeline.utils.lakefs_utils import setup_lakefs_branches_and_folders

# Load environment variables
load_dotenv()

@asset(
    required_resource_keys={"lakefs", "lakefs_client"},
    deps=["cleanup_lakefs_repository"],
    description="Set up LakeFS repository with required branches and folders",
    group_name="lakefs_Clean_Setup",
    auto_materialize_policy=AutoMaterializePolicy.eager()
)
def setup_lakefs_repository(context: AssetExecutionContext):
    # Get repository name from environment
    repository = os.getenv("LAKEFS_REPOSITORY")
    default_branch = os.getenv("LAKEFS_DEFAULT_BRANCH", "main")
    
    if not repository:
        raise ValueError("LAKEFS_REPOSITORY environment variable is required")
    
    context.log.info(f"Setting up LakeFS repository: {repository}")
    
    # Call the setup function with resources
    success = setup_lakefs_branches_and_folders(
        context=context,
        lakefs_resource=context.resources.lakefs,
        lakefs_client_resource=context.resources.lakefs_client,
        repository=repository,
        default_branch=default_branch
    )
    
    if success:
        context.log.info("✅ LakeFS repository setup completed successfully")
        return {
            "repository": repository,
            "branches_created": ["Processed_data", "Models_deployed"],
            "folders_created": ["data/", "new_data/", "new_merge_data/"],
            "status": "success"
        }
    else:
        raise RuntimeError("Failed to set up LakeFS repository")
