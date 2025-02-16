from dagster import Definitions, load_assets_from_modules
from .assets import trips, dbt, chicago_crimes
from .jobs import trip_update_job
from .resources import database_resource, dbt_resource, r2_resource, dlt_resource
from .schedules import trip_update_schedule


trip_assets = load_assets_from_modules([trips])
dbt_analytics_assets = load_assets_from_modules([dbt])
chicago_crimes_assets = load_assets_from_modules([chicago_crimes])

all_jobs = [trip_update_job]
all_schedules = [trip_update_schedule]


defs = Definitions(
    assets=[*trip_assets, *dbt_analytics_assets, *chicago_crimes_assets],
    resources={
        "database": database_resource,
        "dbt": dbt_resource,
        "r2": r2_resource,
        "dlt": dlt_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules
)
