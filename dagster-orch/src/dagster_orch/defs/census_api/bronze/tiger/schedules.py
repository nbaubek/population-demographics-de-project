"""Bronze TIGER schedules."""

import dagster as dg

from dagster_orch.defs.census_api.bronze.tiger.jobs import bronze_tiger_job

bronze_tiger_schedule = dg.ScheduleDefinition(
    name="bronze_tiger_schedule",
    job=bronze_tiger_job,
    cron_schedule="0 1 1 1 *",  # January 1st at 1 AM UTC - yearly TIGER update
    description="Download TIGER geographic boundaries annually",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)