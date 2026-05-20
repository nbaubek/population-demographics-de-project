"""Silver ACS5 schedules."""

import dagster as dg

from dagster_orch.defs.census_api.silver.acs5.jobs import silver_acs5_job

# Runs after bronze completes - typically same day or next
silver_acs5_schedule = dg.ScheduleDefinition(
    name="silver_acs5_schedule",
    job=silver_acs5_job,
    cron_schedule="0 3 * * *",  # 3 AM UTC daily - will process latest partition
    description="Transform bronze to silver Iceberg tables",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)