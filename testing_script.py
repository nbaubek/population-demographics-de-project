"""Data quality and schema analysis for Census demographics pipeline.

Checks schemas, nulls, composite key uniqueness, and join readiness
across bronze, silver, and gold Iceberg tables.
"""

import os
from datetime import date

import boto3
import polars as pl


ATHENA_REGION = "us-east-1"
WORKGROUP = "population-demographics"
BRONZE_DB = "population_demographics"
SILVER_DB = "population_demographics_silver"
GOLD_DB = "population_demographics_gold"

S3_BUCKET = "s3://population-demographics-iceberg"


def get_athena_client():
    return boto3.client("athena", region_name=ATHENA_REGION)


def run_athena_query(query: str, db: str = None) -> list[dict]:
    """Execute Athena query and return results as list of dicts."""
    client = get_athena_client()
    context = {"Database": db} if db else {}
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext=context,
        ResultConfiguration={"OutputLocation": f"{S3_BUCKET}/athena-results/"},
        WorkGroup=WORKGROUP,
    )
    execution_id = response["QueryExecutionId"]

    import time
    for _ in range(120):
        state = client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        elif state == "FAILED":
            reason = client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]["Status"].get("AthenaError", {})
            raise RuntimeError(f"Query failed: {reason}")
        time.sleep(2)

    result = client.get_query_results(QueryExecutionId=execution_id)
    rows = result["ResultSet"]["Rows"]
    if not rows:
        return []

    headers = [col["VarCharValue"] for col in rows[0]["Data"]]
    data = []
    for row in rows[1:]:
        data.append({headers[i]: row["Data"][i].get("VarCharValue", "") for i in range(len(headers))})
    return data


def print_section(title):
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")


def analyze_table_schema(db: str, table: str) -> dict | None:
    """Get column schema for a table."""
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{db}' AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    try:
        rows = run_athena_query(query)
        return {r["column_name"]: r["data_type"] for r in rows}
    except Exception as e:
        return None


def table_row_count(db: str, table: str, where_clause: str = "") -> int:
    """Get total row count for a table with optional where clause."""
    query = f"SELECT COUNT(*) as cnt FROM {db}.{table}"
    if where_clause:
        query += f" WHERE {where_clause}"
    try:
        result = run_athena_query(query)
        return int(result[0]["cnt"]) if result else 0
    except Exception:
        return 0


def null_check(db: str, table: str, columns: list[str]) -> dict:
    """Check null counts for specified columns."""
    null_checks = [f"COUNT(*) FILTER (WHERE {col} IS NULL) as null_{col}" for col in columns]
    query = f"SELECT {', '.join(null_checks)} FROM {db}.{table}"
    try:
        result = run_athena_query(query)
        return {k.replace("null_", ""): int(v) for k, v in result[0].items() if v}
    except Exception as e:
        return {"error": str(e)}


def sample_values(db: str, table: str, col: str, limit: int = 5) -> list:
    """Get sample values from a column."""
    query = f'SELECT DISTINCT {col} FROM {db}.{table} LIMIT {limit}'
    try:
        result = run_athena_query(query)
        return [r[col] for r in result]
    except Exception:
        return []


def check_composite_key_duplicates(db: str, table: str, col1: str, col2: str, limit: int = 10) -> dict:
    """Check for duplicate composite keys."""
    query = f"""
    SELECT {col1}, {col2}, COUNT(*) as cnt
    FROM {db}.{table}
    GROUP BY {col1}, {col2}
    HAVING COUNT(*) > 1
    LIMIT {limit}
    """
    try:
        result = run_athena_query(query)
        return {"has_duplicates": len(result) > 0, "duplicates": result}
    except Exception as e:
        return {"has_duplicates": None, "error": str(e)}


def check_key_joinability(db1: str, t1: str, db2: str, t2: str, key_col: str, year: str) -> dict:
    """Check if keys match between two tables for a given year."""
    query = f"""
    SELECT COUNT(DISTINCT a.{key_col}) as acs_keys,
           COUNT(DISTINCT b.{key_col}) as tiger_keys,
           COUNT(DISTINCT CASE WHEN b.{key_col} IS NOT NULL THEN a.{key_col} END) as matched_keys
    FROM {db1}.{t1} a
    JOIN {db2}.{t2} b
        ON a.{key_col} = b.{key_col}
        AND a.survey_year = b.survey_year
    WHERE a.survey_year = '{year}'
    """
    try:
        result = run_athena_query(query)
        if result:
            r = result[0]
            return {
                "acs_keys": int(r["acs_keys"]),
                "tiger_keys": int(r["tiger_keys"]),
                "matched_keys": int(r["matched_keys"]),
                "match_rate": round(int(r["matched_keys"]) / max(int(r["acs_keys"]), 1) * 100, 1),
            }
    except Exception as e:
        return {"error": str(e)}
    return {}


def main():
    print(f"\n{'#' * 80}")
    print("# CENSUS DEMOGRAPHICS PIPELINE - DATA QUALITY REPORT")
    print(f"# Run date: {date.today()}")
    print(f"{'#' * 80}")

    # --- BRONZE SCHEMA CHECK ---
    print_section("BRONZE LAYER - SCHEMA")
    bronze_tables = [
        ("bronze_acs5_states", BRONZE_DB),
        ("bronze_acs5_counties", BRONZE_DB),
        ("bronze_acs5_tracts", BRONZE_DB),
        ("bronze_tiger_states", BRONZE_DB),
        ("bronze_tiger_counties", BRONZE_DB),
        ("bronze_tiger_tracts", BRONZE_DB),
    ]

    for table, db in bronze_tables:
        schema = analyze_table_schema(db, table)
        if schema is None:
            print(f"\n  [SKIP] {table} - does not exist")
            continue
        print(f"\n  {table}:")
        print(f"    Columns ({len(schema)}): {', '.join(schema.keys())}")
        missing_cols = []
        if "survey_year" not in schema:
            missing_cols.append("survey_year")
        if "ingest_date" not in schema:
            missing_cols.append("ingest_date")
        if missing_cols:
            print(f"    [WARN] Missing columns: {missing_cols}")

    # --- SILVER SCHEMA CHECK ---
    print_section("SILVER LAYER - SCHEMA")
    silver_tables = [
        ("silver_acs5_states", SILVER_DB),
        ("silver_acs5_counties", SILVER_DB),
        ("silver_acs5_tracts", SILVER_DB),
        ("silver_tiger_states", SILVER_DB),
        ("silver_tiger_counties", SILVER_DB),
        ("silver_tiger_tracts", SILVER_DB),
    ]

    for table, db in silver_tables:
        schema = analyze_table_schema(db, table)
        if schema is None:
            print(f"\n  [SKIP] {table} - does not exist")
            continue
        print(f"\n  {table}:")
        print(f"    Columns ({len(schema)}): {', '.join(schema.keys())}")

        # Check key columns
        for col in ["geography_id", "survey_year"]:
            if col in schema:
                print(f"    {col}: {schema[col]}")
            else:
                print(f"    [WARN] Missing: {col}")

        # Row count
        total = table_row_count(db, table)
        print(f"    Total rows: {total}")

        # Null check
        nulls = null_check(db, table, ["geography_id", "survey_year"])
        if nulls:
            for col, cnt in nulls.items():
                if cnt > 0:
                    print(f"    [WARN] Null {col}: {cnt}")

    # --- SILVER KEY JOINABILITY (sample year) ---
    print_section("SILVER LAYER - KEY JOINABILITY (2021)")

    for geography in ["states", "counties", "tracts"]:
        acs_table = f"silver_acs5_{geography}"
        tiger_table = f"silver_tiger_{geography}"
        result = check_key_joinability(SILVER_DB, acs_table, SILVER_DB, tiger_table, "geography_id", "2021")
        print(f"\n  {geography}:")
        if "error" in result:
            print(f"    [SKIP] {result['error']}")
        else:
            print(f"    ACS5 distinct keys: {result['acs_keys']}")
            print(f"    TIGER distinct keys: {result['tiger_keys']}")
            print(f"    Matched keys: {result['matched_keys']} ({result['match_rate']}%)")
            if result['match_rate'] < 100:
                print(f"    [WARN] Not all ACS keys matched TIGER!")

    # --- SILVER COMPOSITE KEY UNIQUENESS ---
    print_section("SILVER LAYER - COMPOSITE KEY UNIQUENESS")
    print("  Checking geography_id + survey_year uniqueness...")

    for table, db in silver_tables:
        if "acs5" in table:
            continue  # ACS can have multiple rows per geography if NAME changes
        result = check_composite_key_duplicates(db, table, "geography_id", "survey_year")
        print(f"\n  {table}:")
        if "error" in result:
            print(f"    [SKIP] {result['error']}")
        elif result["has_duplicates"] is None:
            print(f"    [SKIP] Could not check")
        elif result["has_duplicates"]:
            print(f"    [FAIL] {len(result['duplicates'])} duplicate keys found!")
            for d in result["duplicates"][:3]:
                print(f"      geography_id={d.get('geography_id')}, survey_year={d.get('survey_year')}, count={d.get('cnt')}")
        else:
            print(f"    [OK] No duplicates")

    # --- GOLD LAYER CHECK ---
    print_section("GOLD LAYER - SCHEMA & ROW COUNTS")
    for geography in ["states", "counties", "tracts"]:
        table = f"gold_{geography}"
        schema = analyze_table_schema(GOLD_DB, table)
        total = table_row_count(GOLD_DB, table)
        print(f"\n  {table}:")
        if schema is None:
            print(f"    [SKIP] Does not exist")
            continue
        print(f"    Columns ({len(schema)}): {', '.join(schema.keys())}")
        print(f"    Total rows: {total}")
        nulls = null_check(GOLD_DB, table, ["geography_id", "survey_year"])
        for col, cnt in nulls.items():
            if cnt > 0:
                print(f"    [WARN] Null {col}: {cnt}")

        # Check for duplicates
        result = check_composite_key_duplicates(GOLD_DB, table, "geography_id", "survey_year")
        if result.get("has_duplicates"):
            print(f"    [FAIL] Duplicate composite keys!")
        else:
            print(f"    [OK] Composite key unique")

    print(f"\n{'=' * 80}")
    print("ANALYSIS COMPLETE")
    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    main()