from dagster import AssetExecutionContext,AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator
from ..partitions import daily_partition
import json
from datetime import datetime, timedelta

INCREMENTAL_SELECTOR = "config.materialized:incremental"

from ..project import dbt_project

# class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
#     def get_asset_key(self, dbt_resource_props):
#         resource_type = dbt_resource_props["resource_type"]
#         name = dbt_resource_props["name"]
#         if resource_type == "source":
#             return AssetKey(f"taxi_{name}")
#         else:
#             return super().get_asset_key(dbt_resource_props)

# @dbt_assets(
#     manifest=dbt_project.manifest_path,
#     #dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
#     exclude=INCREMENTAL_SELECTOR,
# )
# def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
#     yield from dbt.cli(["build"], context=context).stream()

@dbt_assets(
    manifest=dbt_project.manifest_path,
    select=INCREMENTAL_SELECTOR,     # select only models with INCREMENTAL_SELECTOR
    partitions_def=daily_partition,   # partition those models using daily_partition
)
def incremental_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    partition_date = context.partition_key
    # Convert partition date to datetime object at midnight (00:00:00)
    start_datetime = datetime.strptime(partition_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0)
    
    # Calculate end of the day (23:59:59)
    end_datetime = start_datetime + timedelta(hours=23, minutes=59, seconds=59)
    
    # Format as ISO strings
    date_start_iso = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    date_end_iso = end_datetime.strftime("%Y-%m-%d %H:%M:%S")
    dbt_vars = {
        "min_date": date_start_iso,
        "max_date": date_end_iso
    }
    yield from dbt.cli(
        ["build", "--vars", json.dumps(dbt_vars)], context=context
    ).stream()
