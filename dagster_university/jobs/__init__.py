from dagster import AssetSelection, define_asset_job
from dagster_dbt import build_dbt_asset_selection
from ..assets.dbt import dbt_analytics
from ..partitions import monthly_partition

dbt_trips_selection = build_dbt_asset_selection([dbt_analytics], "stg_trips").downstream()
trips= AssetSelection.assets("taxi_trips_file","taxi_trips")
trip_update_job = define_asset_job(
    name="trip_update_job",
    partitions_def=monthly_partition,
    selection=trips
)