"""Bronze TIGER assets - geographic boundary data partitioned by year."""

import logging

import dagster as dg
import pygris
import polars as pl
from datetime import date
import time as time_module
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from dagster_orch.defs.census_api.shared.constants import YEAR_PARTITIONS, TRACT_PARTITIONS

logger = logging.getLogger(__name__)


# S3 paths for TIGER data
TIGER_BUCKET = "s3://population-demographics-iceberg/bronze/tiger"


@retry(
    stop=stop_after_attempt(7),
    wait=wait_exponential(multiplier=2, min=10, max=120),
    retry=retry_if_exception_type((ValueError, ConnectionError, TimeoutError, OSError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def _fetch_tiger_states(year: int):
    """Fetch TIGER state boundaries via direct FTP (bypasses pygris HTTP→FTP fallback)."""
    import ftplib
    import io
    import zipfile
    import geopandas as gpd
    import tempfile
    import shutil

    ftp_host = "ftp2.census.gov"
    ftp_path = f"geo/tiger/TIGER{year}/STATE"
    filename = f"tl_{year}_us_state.zip"

    buffer = io.BytesIO()
    with ftplib.FTP(ftp_host) as ftp:
        ftp.login()
        ftp.cwd(ftp_path)
        ftp.retrbinary(f"RETR {filename}", buffer.write)
    buffer.seek(0)

    tmpdir = tempfile.mkdtemp()
    try:
        zf = zipfile.ZipFile(buffer)
        zf.extractall(tmpdir)
        import os
        shp_name = [n for n in os.listdir(tmpdir) if n.endswith(".shp")][0]
        return gpd.read_file(os.path.join(tmpdir, shp_name))
    finally:
        shutil.rmtree(tmpdir)


@retry(
    stop=stop_after_attempt(7),
    wait=wait_exponential(multiplier=2, min=10, max=120),
    retry=retry_if_exception_type((ValueError, ConnectionError, TimeoutError, OSError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def _fetch_tiger_counties(year: int):
    """Fetch TIGER county boundaries with retry."""
    return pygris.counties(year=year)


@retry(
    stop=stop_after_attempt(7),
    wait=wait_exponential(multiplier=2, min=10, max=120),
    retry=retry_if_exception_type((ValueError, ConnectionError, TimeoutError, OSError)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def _fetch_tiger_tracts(state: str, year: int):
    """Fetch TIGER tract boundaries for a US state (by FIPS code like '06')."""
    import pygris
    return pygris.tracts(state=state, year=year)


def _prepare_tiger_gdf(gdf, year: int) -> pl.DataFrame:
    """Prepare TIGER geodataframe for parquet: add metadata, convert geometry to WKT.

    Returns Polars DataFrame for efficient serialization.
    """
    # Convert geometry to WKT string first
    gdf["geometry_wkt"] = gdf["geometry"].to_wkt()
    gdf = gdf.drop(columns=["geometry"])

    # Add metadata columns
    gdf["survey_year"] = year
    gdf["ingest_date"] = str(date.today())

    # Convert each column to a plain list, handling PyArrow StringArray and NaN nulls
    data = {}
    for col in gdf.columns:
        arr = gdf[col].to_numpy()
        if arr.dtype == object:
            # Object dtype - may contain PyArrow scalars or mixed types
            # Convert each element explicitly to a native Python type
            converted = []
            for v in arr:
                if v is None:
                    converted.append(None)
                elif isinstance(v, float) and v != v:  # NaN check
                    converted.append(None)
                else:
                    converted.append(str(v))
            data[col] = converted
        else:
            data[col] = arr

    return pl.DataFrame(data)


@dg.asset(
    name="bronze_tiger_states",
    partitions_def=YEAR_PARTITIONS,
    group_name="bronze_tiger",
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def bronze_tiger_states(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Download TIGER state boundaries for the given year."""
    year = int(context.partition_key)
    start_time = time_module.time()
    context.log.info(f"Starting bronze_tiger_states for year={year}")

    gdf = _fetch_tiger_states(year=year)
    context.log.info(f"Fetched {len(gdf)} rows from pygris")
    df = _prepare_tiger_gdf(gdf, year)

    output_path = f"{TIGER_BUCKET}/states/year={year}/states.parquet"
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
    name="bronze_tiger_counties",
    partitions_def=YEAR_PARTITIONS,
    group_name="bronze_tiger",
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def bronze_tiger_counties(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Download TIGER county boundaries for the given year."""
    year = int(context.partition_key)
    start_time = time_module.time()
    context.log.info(f"Starting bronze_tiger_counties for year={year}")

    gdf = _fetch_tiger_counties(year=year)
    context.log.info(f"Fetched {len(gdf)} rows from pygris")
    df = _prepare_tiger_gdf(gdf, year)

    output_path = f"{TIGER_BUCKET}/counties/year={year}/counties.parquet"
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
    name="bronze_tiger_tracts",
    partitions_def=TRACT_PARTITIONS,
    group_name="bronze_tiger",
    auto_materialize_policy=dg.AutoMaterializePolicy.eager(),
)
def bronze_tiger_tracts(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Download TIGER census tract boundaries for all US states.

    Multi-partitioned by (year, state) with Hive-style S3 paths:
    bronze/tiger/tracts/year=YYYY/state=FF/tracts.parquet
    """
    keys = context.partition_key.keys_by_dimension
    year = int(keys["year"])
    state_fips = keys["state"]
    start_time = time_module.time()
    context.log.info(f"Starting bronze_tiger_tracts for year={year}, state={state_fips}")

    gdf = _fetch_tiger_tracts(state=state_fips, year=year)
    context.log.info(f"Fetched {len(gdf)} rows from pygris")
    df = _prepare_tiger_gdf(gdf, year)

    # Multi-dimensional Hive partitioning: year=YYYY/state=FF
    output_path = f"{TIGER_BUCKET}/tracts/year={year}/state={state_fips}/tracts.parquet"
    df.write_parquet(output_path, compression="snappy")
    elapsed = time_module.time() - start_time
    context.log.info(f"Wrote {len(df)} rows to {output_path} in {elapsed:.1f}s")

    return dg.MaterializeResult(
        metadata={
            "row_count": len(df),
            "year": year,
            "state": state_fips,
            "s3_path": output_path,
            "duration_seconds": round(elapsed, 2),
        }
    )