"""Bronze layer data quality inspection using DuckDB + Polars.

Reads parquet files directly from S3 to inspect schema, row counts,
nulls, and sample data across year and geography partitions.

Usage:
    python testing_script.py                              # tracts: CA (06), all years
    python testing_script.py --tiger-state 48            # tracts: TX
    python testing_script.py --year-start 2020 --year-end 2024  # limit years
    python testing_script.py --nulls-only                 # only null analysis
"""

import argparse
from datetime import date

import duckdb
import polars as pl


S3_BUCKET = "s3://population-demographics-iceberg"
DEFAULT_TRACT_STATE = "06"


def get_s3_creds() -> dict:
    """Get AWS credentials via boto3 default chain."""
    import boto3
    session = boto3.Session()
    creds = session.get_credentials()
    if not creds:
        return {}
    frozen = creds.get_frozen_credentials()
    return {
        "access_key_id": frozen.access_key,
        "secret_access_key": frozen.secret_key,
        "session_token": frozen.token or "",
        "region": session.region_name or "us-east-1",
    }


def read_bronze(path: str, creds: dict) -> pl.DataFrame | None:
    """Read bronze parquet using DuckDB with direct S3 access."""
    try:
        con = duckdb.connect()
        try:
            con.install_extension("httpfs")
            con.load_extension("httpfs")
        except Exception:
            pass
        con.execute(f"SET s3_access_key_id='{creds['access_key_id']}'")
        con.execute(f"SET s3_secret_access_key='{creds['secret_access_key']}'")
        if creds.get("session_token"):
            con.execute(f"SET s3_session_token='{creds['session_token']}'")
        con.execute(f"SET s3_region='{creds['region']}'")
        df = con.execute(f"SELECT * FROM read_parquet('{path}')").df()
        con.close()
        return pl.DataFrame(df)
    except Exception as e:
        return None


def print_header(title: str):
    print(f"\n{'=' * 90}")
    print(f"  {title}")
    print(f"{'=' * 90}")


def check_dataset(name: str, paths: list[str], creds: dict):
    """Check a dataset: print row counts per year and sample."""
    print(f"\n  -- {name} --")
    for path in paths:
        year = path.split("year=")[1].split("/")[0] if "year=" in path else "?"
        state = path.split("state=")[1].split("/")[0] if "state=" in path else ""
        label = f"{year}" + (f" ({state})" if state else "")
        df = read_bronze(path, creds)
        if df is None:
            print(f"    {label}: FAILED (path not found or access denied)")
            continue
        print(f"    {label}: {len(df)} rows x {len(df.columns)} cols")
        if len(df) > 0:
            sample = df.head(3).to_pandas().to_string(index=False).split("\n")
            for line in sample:
                print(f"      {line}")


def null_analysis(name: str, path: str, cols: list[str], creds: dict):
    """Null analysis on key columns."""
    df = read_bronze(path, creds)
    if df is None:
        print(f"    {name}: FAILED")
        return
    total = len(df)
    print(f"    {name} ({total} rows):")
    for c in cols:
        if c in df.columns:
            nulls = int(df[c].is_null().sum())
            null_pct = nulls / max(total, 1) * 100
            flag = "  ⚠ NULLS" if nulls > 0 else ""
            print(f"      {c:30s}: {nulls:6d} null ({null_pct:5.1f}%){flag}")


def main():
    parser = argparse.ArgumentParser(description="Inspect bronze layer parquet files via DuckDB")
    parser.add_argument(
        "--tiger-state",
        type=str,
        default=DEFAULT_TRACT_STATE,
        help=f"FIPS code for tract-level inspection (default: {DEFAULT_TRACT_STATE})",
    )
    parser.add_argument(
        "--year-start", type=int, default=2012, help="Start year (default: 2012)"
    )
    parser.add_argument(
        "--year-end", type=int, default=2024, help="End year (default: 2024)"
    )
    parser.add_argument(
        "--nulls-only", action="store_true", help="Only run null analysis"
    )
    args = parser.parse_args()

    creds = get_s3_creds()
    if not creds.get("access_key_id"):
        print("ERROR: No AWS credentials found. Set via aws configure or environment variables.")
        return

    years = list(range(args.year_start, args.year_end + 1))
    state = args.tiger_state

    print(f"\n{'=' * 90}")
    print(f"  BRONZE LAYER INSPECTION")
    print(f"  Date: {date.today()}")
    print(f"  Years: {args.year_start}-{args.year_end}")
    print(f"  Tract state: {state}")
    print(f"{'=' * 90}")

    # --- ACS BRONZE ---
    print_header("ACS BRONZE (states, counties, tracts)")
    check_dataset(
        "ACS States (all years)",
        [f"{S3_BUCKET}/bronze/census_acs5/states/year={y}/states.parquet" for y in years],
        creds,
    )
    check_dataset(
        "ACS Counties (all years)",
        [f"{S3_BUCKET}/bronze/census_acs5/counties/year={y}/counties.parquet" for y in years],
        creds,
    )
    check_dataset(
        f"ACS Tracts {state} (all years)",
        [f"{S3_BUCKET}/bronze/census_acs5/tracts/year={y}/state={state}/tracts.parquet" for y in years],
        creds,
    )

    # --- TIGER BRONZE ---
    print_header("TIGER BRONZE (states, counties, tracts)")
    check_dataset(
        "TIGER States (all years)",
        [f"{S3_BUCKET}/bronze/tiger/states/year={y}/states.parquet" for y in years],
        creds,
    )
    check_dataset(
        "TIGER Counties (all years)",
        [f"{S3_BUCKET}/bronze/tiger/counties/year={y}/counties.parquet" for y in years],
        creds,
    )
    check_dataset(
        f"TIGER Tracts {state} (all years)",
        [f"{S3_BUCKET}/bronze/tiger/tracts/year={y}/state={state}/tracts.parquet" for y in years],
        creds,
    )

    # --- NULL ANALYSIS ---
    print_header("NULL ANALYSIS (first year only)")
    sample_year = years[0]
    null_checks = [
        (
            "ACS States",
            f"{S3_BUCKET}/bronze/census_acs5/states/year={sample_year}/states.parquet",
            ["survey_year", "ingest_date", "NAME"],
        ),
        (
            "ACS Counties",
            f"{S3_BUCKET}/bronze/census_acs5/counties/year={sample_year}/counties.parquet",
            ["survey_year", "ingest_date", "NAME"],
        ),
        (
            f"ACS Tracts {state}",
            f"{S3_BUCKET}/bronze/census_acs5/tracts/year={sample_year}/state={state}/tracts.parquet",
            ["survey_year", "ingest_date", "NAME"],
        ),
        (
            "TIGER States",
            f"{S3_BUCKET}/bronze/tiger/states/year={sample_year}/states.parquet",
            ["survey_year", "ingest_date", "GEOID"],
        ),
        (
            "TIGER Counties",
            f"{S3_BUCKET}/bronze/tiger/counties/year={sample_year}/counties.parquet",
            ["survey_year", "ingest_date", "GEOID"],
        ),
        (
            f"TIGER Tracts {state}",
            f"{S3_BUCKET}/bronze/tiger/tracts/year={sample_year}/state={state}/tracts.parquet",
            ["survey_year", "ingest_date", "GEOID"],
        ),
    ]
    for name, path, cols in null_checks:
        null_analysis(name, path, cols, creds)

    print(f"\n{'=' * 90}")
    print("  DONE")
    print(f"{'=' * 90}\n")


if __name__ == "__main__":
    main()