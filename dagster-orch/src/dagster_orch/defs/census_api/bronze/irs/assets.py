"""Bronze IRS assets - raw IRS migration outflow data.

Downloads IRS migration CSV directly and uploads to S3 as parquet without local disk persistence.
Partitioned by the second year of the migration period (e.g., 1112.csv → year=2012).

Bronze layer stores raw source data unchanged. Filtering of aggregated total rows (96/97/98/99/58/57/59)
happens at the silver layer.
"""

import io
import logging
import time as time_module
from datetime import date

import dagster as dg
import polars as pl
import requests
from requests.exceptions import RequestException
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

BRONZE_BUCKET = "s3://population-demographics-iceberg/bronze"

# IRS state migration data available from 2011-2012 through 2022-2023
# County migration data available from 2011-2012 through 2022-2023
# Use the second year as partition (e.g., 1112.csv → year=2012)
IRS_STATE_YEARS = list(range(2012, 2024))   # 2012, 2013, ..., 2023
IRS_COUNTY_YEARS = list(range(2012, 2024))  # 2012, 2013, ..., 2023

IRS_STATE_PARTITIONS = dg.StaticPartitionsDefinition([str(y) for y in IRS_STATE_YEARS])
IRS_COUNTY_PARTITIONS = dg.StaticPartitionsDefinition([str(y) for y in IRS_COUNTY_YEARS])


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=10, max=60),
    retry=retry_if_exception_type((RequestException, TimeoutError, OSError)),
    before_sleep=before_sleep_log(__name__, logging.WARNING),
)
def _fetch_url(url: str, timeout: int) -> requests.Response:
    """Make an HTTP request with retry logic."""
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response


def _irs_state_url(year: int) -> str:
    """Build IRS SOI state migration outflow CSV URL."""
    y1 = str(year - 1)[2:]
    y2 = str(year)[2:]
    return f"https://www.irs.gov/pub/irs-soi/stateoutflow{y1}{y2}.csv"


def _irs_county_url(year: int) -> str:
    """Build IRS SOI county migration outflow CSV URL."""
    y1 = str(year - 1)[2:]
    y2 = str(year)[2:]
    return f"https://www.irs.gov/pub/irs-soi/countyoutflow{y1}{y2}.csv"


@dg.asset(
    name="bronze_irs_state_outflows",
    partitions_def=IRS_STATE_PARTITIONS,
    group_name="bronze_irs",
)
def bronze_irs_state_outflows(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Download IRS state-to-state migration outflows and write to S3 as parquet.

    Raw source data stored unchanged — aggregated total rows (96/97/98/99/58/57/59 codes) are
    preserved as-is and filtered at the silver layer.

    Partition key is the second year of the migration period (e.g., 2012 for 2011-2012 data).

    CSV columns (as-is from IRS):
    - y1_statefips: origin state FIPS
    - y2_statefips: destination state FIPS (96=US+Foreign total, 97=US total, 98=Foreign, 99=PR)
    - y2_state: destination state abbreviation
    - y2_state_name: destination state name
    - n1: number of non-exempt returns
    - n2: number of exempt returns
    - AGI: adjusted gross income in thousands
    """
    year = int(context.partition_key)
    start_time = time_module.time()
    context.log.info(f"Starting bronze_irs_state_outflows for year={year}")

    url = _irs_state_url(year)
    context.log.info(f"Fetching {url}")

    response = _fetch_url(url, timeout=60)

    df = pl.read_csv(io.BytesIO(response.content), infer_schema_length=0)
    today = str(date.today())
    df = df.with_columns([
        pl.lit(year).alias("survey_year"),
        pl.lit(today).alias("ingest_date"),
    ])
    context.log.info(f"Downloaded {len(df)} rows from IRS")

    output_path = f"{BRONZE_BUCKET}/irs/migration/state_outflows/year={year}/state_outflows.parquet"
    df.write_parquet(output_path, compression="snappy")
    elapsed = time_module.time() - start_time
    context.log.info(f"Wrote {len(df)} rows to {output_path} in {elapsed:.1f}s")

    return dg.MaterializeResult(
        metadata={
            "row_count": len(df),
            "year": year,
            "s3_path": output_path,
            "duration_seconds": round(elapsed, 2),
        }
    )


@dg.asset(
    name="bronze_irs_county_outflows",
    partitions_def=IRS_COUNTY_PARTITIONS,
    group_name="bronze_irs",
)
def bronze_irs_county_outflows(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Download IRS county-to-county migration outflows and write to S3 as parquet.

    Raw source data stored unchanged — aggregated total rows (96/97/98/99/58/57/59 codes) are
    preserved as-is and filtered at the silver layer.

    Partition key is the second year of the migration period (e.g., 2012 for 2011-2012 data).

    CSV columns (as-is from IRS):
    - y1_statefips: origin state FIPS
    - y1_countyfips: origin county FIPS (3-digit within state)
    - y2_statefips: destination state FIPS
    - y2_countyfips: destination county FIPS
    - y2_state: destination state abbreviation
    - y2_countyname: destination county name
    - n1: number of non-exempt returns
    - n2: number of exempt returns
    - agi: adjusted gross income in thousands
    """
    year = int(context.partition_key)
    start_time = time_module.time()
    context.log.info(f"Starting bronze_irs_county_outflows for year={year}")

    url = _irs_county_url(year)
    context.log.info(f"Fetching {url}")

    response = _fetch_url(url, timeout=120)

    # County CSV may contain non-UTF-8 characters; decode with replacement before parsing
    decoded_content = response.content.decode("utf-8", errors="replace")
    df = pl.read_csv(io.StringIO(decoded_content), infer_schema_length=0)
    today = str(date.today())
    df = df.with_columns([
        pl.lit(year).alias("survey_year"),
        pl.lit(today).alias("ingest_date"),
    ])
    context.log.info(f"Downloaded {len(df)} rows from IRS")

    output_path = f"{BRONZE_BUCKET}/irs/migration/county_outflows/year={year}/county_outflows.parquet"
    df.write_parquet(output_path, compression="snappy")
    elapsed = time_module.time() - start_time
    context.log.info(f"Wrote {len(df)} rows to {output_path} in {elapsed:.1f}s")

    return dg.MaterializeResult(
        metadata={
            "row_count": len(df),
            "year": year,
            "s3_path": output_path,
            "duration_seconds": round(elapsed, 2),
        }
    )