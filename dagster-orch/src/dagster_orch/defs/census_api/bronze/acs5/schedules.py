"""Bronze ACS5 schedules."""

import dagster as dg

from dagster_orch.defs.census_api.bronze.acs5.jobs import bronze_acs5_job

# Schedule runs yearly after new ACS 5-year data is released (typically December)
# ACS 2024 data released Dec 2024, so schedule for January to be safe
bronze_acs5_schedule = dg.ScheduleDefinition(
    name="bronze_acs5_schedule",
    job=bronze_acs5_job,
    cron_schedule="0 0 15 12 *",  # December 15th at midnight UTC
    description="Ingest ACS 5-year Census data annually",
    default_status=dg.DefaultScheduleStatus.STOPPED,  # Enable manually after initial backfill
)