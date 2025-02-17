from dagster import AssetSelection, define_asset_job
from dagster_dbt import build_dbt_asset_selection
from ..assets.dbt import incremental_dbt_models
from ..partitions import daily_partition


chicago_crimes_extract= AssetSelection.assets("dlt_chicago_crimes_source_get_crimes")
dbt_chicago_crimes = build_dbt_asset_selection([incremental_dbt_models], "stg_chicago_crimes").downstream()
chicago_crimes_update_job = define_asset_job(
    name="chicago_crimes_update_job",
    partitions_def=daily_partition,
    selection=chicago_crimes_extract | dbt_chicago_crimes
)