from dagster import job, op, OpExecutionContext
import os
from dagster_pipeline.utils.lakefs_utils import setup_lakefs_branches_and_folders, clean_lakefs_repository


@op(required_resource_keys={"lakefs", "lakefs_client"})
def setup_lakefs_op(context: OpExecutionContext):
    """Operation to set up LakeFS repository with required branches and folders"""
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
        raise Exception("❌ LakeFS repository setup failed")


@op(required_resource_keys={"lakefs", "lakefs_client"})
def cleanup_lakefs_op(context: OpExecutionContext):
    """Operation to clean up LakeFS repository (remove all branches except main and clear folders)"""
    repository = os.getenv("LAKEFS_REPOSITORY")
    default_branch = os.getenv("LAKEFS_DEFAULT_BRANCH", "main")
    
    if not repository:
        raise ValueError("LAKEFS_REPOSITORY environment variable is required")
    
    context.log.info(f"Cleaning up LakeFS repository: {repository}")
    
    # Call the cleanup function with resources
    success = clean_lakefs_repository(
        context=context,
        lakefs_resource=context.resources.lakefs,
        lakefs_client_resource=context.resources.lakefs_client,
        repository=repository,
        default_branch=default_branch
    )
    
    if success:
        context.log.info("✅ LakeFS repository cleanup completed successfully")
        return {
            "repository": repository,
            "status": "cleaned",
            "message": "All branches removed except main, all files removed"
        }
    else:
        raise Exception("❌ LakeFS repository cleanup failed")


@job
def setup_lakefs_job():
    """Job to set up LakeFS repository structure"""
    setup_lakefs_op()


@job
def cleanup_lakefs_job():
    """Job to clean up LakeFS repository"""
    cleanup_lakefs_op()
