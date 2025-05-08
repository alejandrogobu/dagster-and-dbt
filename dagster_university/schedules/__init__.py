from dagster import ScheduleDefinition, build_schedule_from_partitioned_job
from ..jobs import chicago_crimes_update_job

chicago_update_schedule = build_schedule_from_partitioned_job(
    job=chicago_crimes_update_job,
    hour_of_day=22,
    minute_of_hour=00
)