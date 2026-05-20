"""Silver TIGER schedules."""

import dagster as dg

from dagster_orch.defs.census_api.silver.tiger.jobs import silver_tiger_job

# Runs after bronze TIGER completes
silver_tiger_schedule = dg.ScheduleDefinition(
    name="silver_tiger_schedule",
    job=silver_tiger_job,
    cron_schedule="0 4 * * *",  # 4 AM UTC daily
    description="Transform bronze to silver TIGER Iceberg tables",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)