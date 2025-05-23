from lakefs_spec import LakeFSFileSystem
from dagster import resource, Field


@resource(
    config_schema={
        "host": Field(str, description="lakeFS server URL"),
        "username": Field(str, description="lakeFS access key ID"),
        "password": Field(str, description="lakeFS secret access key"),
        "repository": Field(str, description="Default repository name"),
        "default_branch": Field(
            str,
            default_value="main",
            description="Default branch to use"
        )
    }
)
def lakefs_resource(context):
    cfg = context.resource_config
    return LakeFSFileSystem(
        host=cfg["host"],
        username=cfg["username"],
        password=cfg["password"],
        default_repo=cfg["repository"],
        default_branch=cfg["default_branch"]
    )