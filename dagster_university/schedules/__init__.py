from dagster import ScheduleDefinition
from ..jobs import trip_update_job

trip_update_schedule = ScheduleDefinition(
    job=trip_update_job,
    cron_schedule="0 0 5 * *",  # every 5th of the month at midnight
)