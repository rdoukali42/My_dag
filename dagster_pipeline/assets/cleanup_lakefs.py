# import dagster as dg
# from dagster_pipeline.utils.lakefs_utils import clean_lakefs_repository


# @dg.asset(
#     description="Clean LakeFS repository by removing all branches except main and cleaning the main branch",
#     group_name="lakefs_Clean_Setup",
#     compute_kind="lakefs"
# )
# def cleanup_lakefs_repository(context: dg.AssetExecutionContext, lakefs: dg.ConfigurableResource, lakefs_client: dg.ConfigurableResource) -> bool:
#     # Get repository name from environment or config
#     repository = "trackAI"  # You can make this configurable if needed
    
#     context.log.info("🧹 Starting LakeFS repository cleanup...")
#     context.log.warning("⚠️  WARNING: This will delete all branches and files except the empty main branch!")
    
#     success = clean_lakefs_repository(
#         context=context,
#         lakefs_resource=lakefs,
#         lakefs_client_resource=lakefs_client,
#         repository=repository
#     )
    
#     if success:
#         context.log.info("✅ LakeFS repository cleanup completed successfully")
#         return True
#     else:
#         context.log.error("❌ LakeFS repository cleanup failed")
#         return False
