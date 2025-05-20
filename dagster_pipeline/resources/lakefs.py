from lakefs_spec import LakeFSFileSystem
from dagster import resource
import os
from dotenv import load_dotenv

load_dotenv()

@resource
def lakefs_resource(_):
    return LakeFSFileSystem(
        host=os.getenv("LAKEFS_HOST"),
        username=os.getenv("LAKEFS_USERNAME"),
        password=os.getenv("LAKEFS_PASSWORD"),
        default_repo=os.getenv("LAKEFS_REPOSITORY"),
        default_branch=os.getenv("LAKEFS_DEFAULT_BRANCH", "main")
    )