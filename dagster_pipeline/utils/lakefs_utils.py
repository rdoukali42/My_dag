import dagster as dg
from dagster import IOManager, io_manager
from lakefs_spec import LakeFSFileSystem
import requests
import lakefs_client
from lakefs_client.models import BranchCreation


def get_lakefs_csv_path(fs, repo, branch, folder="data/"):
    folder = folder.rstrip("/")
    files = fs.ls(f"{repo}/{branch}/{folder}/")
    csv_files = [
        f["name"] if isinstance(f, dict) else f
        for f in files
        if (f["name"] if isinstance(f, dict) else f).endswith(".csv")
    ]
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in lakeFS folder: {repo}/{branch}/{folder}/")
    prefix = f"{repo}/{branch}/"
    data_path = csv_files[0][len(prefix):] if csv_files[0].startswith(prefix) else csv_files[0]
    return f"lakefs://{repo}/{branch}/{data_path}"


def setup_lakefs_branches_and_folders(context, lakefs_resource, lakefs_client_resource, repository, default_branch="main"):
    context.log.info(f"🚀 Setting up LakeFS repository: {repository}")
    try:
        # Get resources
        fs = lakefs_resource
        client = lakefs_client_resource
        
        # 1. Check existing branches and create missing ones
        branches_to_create = ["Processed_data", "Models_deployed"]
        existing_branches = []
        created_branches = []
        
        # Get list of existing branches
        try:
            branches_response = client.branches.list_branches(repository=repository)
            existing_branch_names = [branch.id for branch in branches_response.results]
            context.log.info(f"📋 Existing branches: {existing_branch_names}")
        except Exception as e:
            context.log.warning(f"⚠️  Could not list existing branches: {e}")
            existing_branch_names = []
        
        for branch_name in branches_to_create:
            if branch_name in existing_branch_names:
                context.log.info(f"⚠️  Branch '{branch_name}' already exists, skipping creation")
                existing_branches.append(branch_name)
            else:
                try:
                    context.log.info(f"📋 Creating branch: {branch_name}")
                    
                    # Create branch using the client
                    branch_creation = BranchCreation(
                        name=branch_name,
                        source=default_branch
                    )
                    
                    client.branches.create_branch(
                        repository=repository,
                        branch_creation=branch_creation
                    )
                    context.log.info(f"✅ Branch '{branch_name}' created successfully")
                    created_branches.append(branch_name)
                    
                except lakefs_client.exceptions.ConflictException:
                    context.log.info(f"⚠️  Branch '{branch_name}' already exists (detected during creation)")
                    existing_branches.append(branch_name)
                except Exception as e:
                    context.log.error(f"❌ Failed to create branch '{branch_name}': {e}")
                    return False
        
        # 2. Check existing folders and create missing ones
        folders_to_create = ["data/", "new_data/", "new_merge_data/"]
        existing_folders = []
        created_folders = []
        changes_made = False
        
        # Check which folders already exist
        for folder in folders_to_create:
            try:
                placeholder_path = f"lakefs://{repository}/{default_branch}/{folder}.gitkeep"
                
                # Try to check if the folder (via .gitkeep file) exists
                try:
                    with fs.open(placeholder_path, 'r') as f:
                        f.read(1)  # Just try to read one character
                    context.log.info(f"⚠️  Folder '{folder}' already exists, skipping creation")
                    existing_folders.append(folder)
                except FileNotFoundError:
                    # Folder doesn't exist, create it
                    context.log.info(f"📁 Creating folder: {folder}")
                    
                    placeholder_content = "# This file ensures the folder exists in LakeFS\n"
                    
                    # Write placeholder file using the filesystem
                    with fs.open(placeholder_path, 'w', encoding='utf-8') as f:
                        f.write(placeholder_content)
                    
                    context.log.info(f"✅ Folder '{folder}' created successfully")
                    created_folders.append(folder)
                    changes_made = True
                    
            except Exception as e:
                context.log.error(f"❌ Failed to process folder '{folder}': {e}")
                return False
        
        # 3. Commit the changes only if there were changes made
        if changes_made:
            try:
                context.log.info("💾 Committing changes...")
                
                commit_creation = lakefs_client.models.CommitCreation(
                    message=f"Setup: Create folders {', '.join(created_folders)}",
                    metadata={
                        "setup_by": "dagster_lakefs_setup",
                        "folders_created": ",".join(created_folders),
                        "branches_created": ",".join(created_branches)
                    }
                )
                
                client.commits.commit(
                    repository=repository,
                    branch=default_branch,
                    commit_creation=commit_creation
                )
                
                context.log.info("✅ Changes committed successfully")
                    
            except Exception as e:
                context.log.error(f"❌ Failed to commit changes: {e}")
                return False
        else:
            context.log.info("ℹ️  No folder changes to commit")
        
        context.log.info(f"🎉 LakeFS repository '{repository}' setup completed successfully!")
        context.log.info("📋 Summary:")
        context.log.info(f"   Repository: {repository}")
        
        if created_branches:
            context.log.info(f"   Branches created: {', '.join(created_branches)}")
        if existing_branches:
            context.log.info(f"   Branches already existed: {', '.join(existing_branches)}")
            
        if created_folders:
            context.log.info(f"   Folders created in '{default_branch}': {', '.join(created_folders)}")
        if existing_folders:
            context.log.info(f"   Folders already existed in '{default_branch}': {', '.join(existing_folders)}")
        
        return True
        
    except Exception as e:
        context.log.error(f"❌ Setup failed: {e}")
        return False


def clean_lakefs_repository(context, lakefs_resource, lakefs_client_resource, repository, default_branch="main"):
    """
    Clean LakeFS repository by removing all branches except main and cleaning the main branch.
    This is useful for testing or resetting the repository to a clean state.
    """
    context.log.info(f"🧹 Cleaning LakeFS repository: {repository}")
    try:
        # Get resources
        fs = lakefs_resource
        client = lakefs_client_resource
        
        # 1. List and delete all branches except main
        try:
            branches_response = client.branches.list_branches(repository=repository)
            branches_to_delete = [
                branch.id for branch in branches_response.results 
                if branch.id != default_branch
            ]
            
            context.log.info(f"🔍 Found branches to delete: {branches_to_delete}")
            
            for branch_name in branches_to_delete:
                try:
                    client.branches.delete_branch(repository=repository, branch=branch_name)
                    context.log.info(f"🗑️  Deleted branch: {branch_name}")
                except Exception as e:
                    context.log.warning(f"⚠️  Could not delete branch '{branch_name}': {e}")
                    
        except Exception as e:
            context.log.warning(f"⚠️  Could not list/delete branches: {e}")
        
        # 2. Clean files from main branch (except keep .lakefs folder if it exists)
        try:
            # List all objects in main branch
            objects_response = client.objects.list_objects(
                repository=repository,
                ref=default_branch
            )
            
            objects_to_delete = [
                obj.path for obj in objects_response.results 
                if not obj.path.startswith('.lakefs/')
            ]
            
            context.log.info(f"🔍 Found {len(objects_to_delete)} objects to delete from {default_branch}")
            
            # Delete objects one by one
            for obj_path in objects_to_delete:
                try:
                    client.objects.delete_object(
                        repository=repository,
                        branch=default_branch,
                        path=obj_path
                    )
                    context.log.debug(f"🗑️  Deleted object: {obj_path}")
                except Exception as e:
                    context.log.warning(f"⚠️  Could not delete object '{obj_path}': {e}")
            
            # Commit the deletions if any objects were deleted
            if objects_to_delete:
                commit_creation = lakefs_client.models.CommitCreation(
                    message="Clean repository: Remove all files",
                    metadata={
                        "cleaned_by": "dagster_lakefs_cleanup",
                        "objects_deleted": str(len(objects_to_delete))
                    }
                )
                
                client.commits.commit(
                    repository=repository,
                    branch=default_branch,
                    commit_creation=commit_creation
                )
                
                context.log.info(f"✅ Cleaned {len(objects_to_delete)} objects and committed changes")
            else:
                context.log.info("ℹ️  No objects to clean from main branch")
                
        except Exception as e:
            context.log.warning(f"⚠️  Could not clean objects from main branch: {e}")
        
        context.log.info(f"🎉 Repository '{repository}' cleaned successfully!")
        return True
        
    except Exception as e:
        context.log.error(f"❌ Cleanup failed: {e}")
        return False


