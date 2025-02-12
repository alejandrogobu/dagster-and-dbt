import os
import boto3
from dagster import EnvVar
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource
from dagster_aws.s3 import S3Resource
from ..project import dbt_project

database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE"),
)


if os.getenv("DAGSTER_ENVIRONMENT") == "prod":
    session = boto3.Session(
        aws_access_key_id=EnvVar("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=EnvVar("AWS_SECRET_ACCESS_KEY"),
        region_name=EnvVar("AWS_REGION"),
    )
    smart_open_config = {"client": session.client("s3")}
else:
    smart_open_config = {}

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)

r2_resource = S3Resource(
    endpoint_url=EnvVar("R2_ENDPOINT"),
    region_name=EnvVar("R2_REGION_NAME"),
    aws_access_key_id=EnvVar("R2_AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=EnvVar("R2_AWS_SECRET_ACCESS_KEY"),
)