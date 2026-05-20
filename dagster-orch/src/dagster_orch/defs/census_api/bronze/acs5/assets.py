"""Bronze ACS5 assets - raw Census API data partitioned by year.

Uses tenacity for retry with exponential backoff on API calls.
Data written to S3 with Hive-style partitioning (year=YYYY).
"""

import logging

import dagster as dg
import polars as pl
import requests
from datetime import date
from os import getenv
from requests.exceptions import RequestException
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from dagster_orch.defs.census_api.shared.constants import (
    ACS_VARIABLES,
    YEAR_PARTITIONS,
    TRACT_PARTITIONS,
)

logger = logging.getLogger(__name__)

BRONZE_BUCKET = "s3://population-demographics-iceberg/bronze"


@retry(
    stop=stop_after_attempt(7),
    wait=wait_exponential(multiplier=2, min=10, max=120),
    retry=retry_if_exception_type((RequestException, ValueError, TimeoutError, OSError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def _census_get_with_retry(url: str, params: dict) -> dict:
    """Make a request to the Census API with retry logic."""
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def _fetch_census_data(
    api_key: str,
    year: int,
    for_clause: str,
    state_filter: str | None = None,
) -> list[dict]:
    """Fetch Census ACS data for a geography level.

    Returns list of records with survey_year and ingest_date added.
    """
    api_url = f"https://api.census.gov/data/{year}/acs/acs5"
    var_codes = ",".join(ACS_VARIABLES.keys())

    params = {"get": f"NAME,{var_codes}", "for": for_clause, "key": api_key}
    if state_filter:
        params["in"] = f"state:{state_filter}"

    data = _census_get_with_retry(api_url, params)
    headers = data[0]
    rows = data[1:]

    today = str(date.today())
    records = []
    for row in rows:
        row_dict = dict(zip(headers, row))
        renamed = {ACS_VARIABLES.get(k, k): v for k, v in row_dict.items()}
        renamed["survey_year"] = year
        renamed["ingest_date"] = today
        records.append(renamed)

    return records


def _get_api_key() -> str:
    """Get Census API key from environment."""
    key = getenv("CENSUS_API_KEY")
    if not key:
        raise ValueError("CENSUS_API_KEY environment variable not set")
    return key


@dg.asset(
    name="bronze_acs5_states",
    partitions_def=YEAR_PARTITIONS,
    group_name="bronze_acs5",
)
def bronze_acs5_states(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Ingest ACS 5-year data for all US states."""
    year = int(context.partition_key)
    api_key = _get_api_key()

    records = _fetch_census_data(api_key, year, for_clause="state:*")

    df = pl.DataFrame(records)
    output_path = f"{BRONZE_BUCKET}/census_acs5/states/year={year}/states.parquet"
    df.write_parquet(output_path, compression="snappy")

    return dg.MaterializeResult(
        metadata={"row_count": len(df), "year": year, "output_path": output_path}
    )


@dg.asset(
    name="bronze_acs5_counties",
    partitions_def=YEAR_PARTITIONS,
    group_name="bronze_acs5",
)
def bronze_acs5_counties(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Ingest ACS 5-year data for all US counties."""
    year = int(context.partition_key)
    api_key = _get_api_key()

    # Census API requires in=state:* for county-level queries to ensure completeness
    records = _fetch_census_data(api_key, year, for_clause="county:*", state_filter="*")

    df = pl.DataFrame(records)
    output_path = f"{BRONZE_BUCKET}/census_acs5/counties/year={year}/counties.parquet"
    df.write_parquet(output_path, compression="snappy")

    return dg.MaterializeResult(
        metadata={"row_count": len(df), "year": year, "output_path": output_path}
    )


@dg.asset(
    name="bronze_acs5_tracts",
    partitions_def=TRACT_PARTITIONS,
    group_name="bronze_acs5",
)
def bronze_acs5_tracts(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Ingest ACS 5-year data for all US census tracts.

    Multi-partitioned by (year, state) with Hive-style S3 paths:
    bronze/census_acs5/tracts/year=YYYY/state=FF/tracts.parquet
    """
    keys = context.partition_key.keys_by_dimension
    year = int(keys["year"])
    state_fips = keys["state"]
    api_key = _get_api_key()

    # Tracts for specific state (state_fips is zero-padded like "06")
    records = _fetch_census_data(
        api_key, year, for_clause="tract:*", state_filter=state_fips
    )

    df = pl.DataFrame(records)
    # Multi-dimensional Hive partitioning: year=YYYY/state=FF
    output_path = f"{BRONZE_BUCKET}/census_acs5/tracts/year={year}/state={state_fips}/tracts.parquet"
    df.write_parquet(output_path, compression="snappy")

    return dg.MaterializeResult(
        metadata={"row_count": len(df), "year": year, "state": state_fips, "output_path": output_path}
    )