"""Gold schedules."""

import dagster as dg

from dagster_orch.defs.census_api.gold.jobs import gold_job

# Gold runs after all silver partitions are complete
# Typically run manually after backfill or after a new year's data is available
gold_schedule = dg.ScheduleDefinition(
    name="gold_schedule",
    job=gold_job,
    cron_schedule="0 5 * * 0",  # 5 AM UTC every Sunday
    description="Join silver tables into gold layer (weekly)",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)