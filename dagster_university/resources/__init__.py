import os

import boto3
from dagster import EnvVar
from dagster_duckdb import DuckDBResource
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource
from dagster_aws.s3 import S3Resource
from ..project import dbt_project

database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE"),
)

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)

r2_resource = S3Resource(
    endpoint_url=EnvVar("R2_ENDPOINT"),
    region_name=EnvVar("R2_REGION_NAME"),
    aws_access_key_id=EnvVar("R2_AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=EnvVar("R2_AWS_SECRET_ACCESS_KEY"),
)

dlt_resource = DagsterDltResource()