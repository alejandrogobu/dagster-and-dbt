from dagster import ScheduleDefinition
from ..jobs import chicago_crimes_update_job

chicago_update_schedule = ScheduleDefinition(
    job=chicago_crimes_update_job,
    cron_schedule="1 0 * * *",  # Runs every day at 00:01
)