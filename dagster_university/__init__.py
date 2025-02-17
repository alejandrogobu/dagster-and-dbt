from dagster import Definitions, load_assets_from_modules
from .assets import dbt, chicago_crimes
from .jobs import chicago_crimes_update_job
from .resources import database_resource, dbt_resource, r2_resource, dlt_resource
from .schedules import chicago_update_schedule



dbt_analytics_assets = load_assets_from_modules([dbt])
chicago_crimes_assets = load_assets_from_modules([chicago_crimes])

all_jobs = [chicago_crimes_update_job]
all_schedules = [chicago_update_schedule]


defs = Definitions(
    assets=[*dbt_analytics_assets, *chicago_crimes_assets],
    resources={
        "database": database_resource,
        "dbt": dbt_resource,
        "r2": r2_resource,
        "dlt": dlt_resource,
    },
    jobs=all_jobs,
    schedules=all_schedules
)
