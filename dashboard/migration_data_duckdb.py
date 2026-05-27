import duckdb
import awswrangler as wr
import boto3
import os
import gc
from pathlib import Path

os.environ['AWS_PROFILE'] = 'my-dev-profile'
session = boto3.Session(region_name='us-east-1')
DB = "population_demographics_gold_marts"
WG = "population-demographics"

SCRIPT_DIR = Path("dashboard").resolve()
DB_PATH = SCRIPT_DIR / "data.duckdb"

con = duckdb.connect(str(DB_PATH))

print("Loading migration_states from Athena...")

df = wr.athena.read_sql_query(
    sql="""
        SELECT
            origin_geography_id,
            dest_geography_id,
            survey_year,
            households,
            individuals,
            agi,
            is_non_migrant,
            origin_state_name,
            origin_total_population,
            origin_median_income,
            origin_poverty_rate,
            origin_remote_work_rate,
            dest_state_name,
            dest_total_population,
            dest_median_income,
            dest_poverty_rate,
            dest_remote_work_rate,
            income_differential,
            agi_per_household,
            origin_lat,
            origin_lon,
            dest_lat,
            dest_lon
        FROM mart_migration_flows_states
        WHERE is_non_migrant = false
    """,
    database=DB,
    workgroup=WG,
    boto3_session=session
)

print(f"Loaded {len(df)} rows from Athena")

# Insert into DuckDB
rows = []
for _, row in df.iterrows():
    rows.append((
        row.get('origin_geography_id'),
        row.get('dest_geography_id'),
        int(row['survey_year']) if row['survey_year'] == row['survey_year'] else None,
        int(row['households']) if row['households'] == row['households'] else None,
        int(row['individuals']) if row['individuals'] == row['individuals'] else None,
        int(row['agi']) if row['agi'] == row['agi'] else None,
        bool(row['is_non_migrant']) if row['is_non_migrant'] == row['is_non_migrant'] else None,
        row.get('origin_state_name'),
        int(row['origin_total_population']) if row['origin_total_population'] == row['origin_total_population'] else None,
        int(row['origin_median_income']) if row['origin_median_income'] == row['origin_median_income'] else None,
        float(row['origin_poverty_rate']) if row['origin_poverty_rate'] == row['origin_poverty_rate'] else None,
        float(row['origin_remote_work_rate']) if row['origin_remote_work_rate'] == row['origin_remote_work_rate'] else None,
        row.get('dest_state_name'),
        int(row['dest_total_population']) if row['dest_total_population'] == row['dest_total_population'] else None,
        int(row['dest_median_income']) if row['dest_median_income'] == row['dest_median_income'] else None,
        float(row['dest_poverty_rate']) if row['dest_poverty_rate'] == row['dest_poverty_rate'] else None,
        float(row['dest_remote_work_rate']) if row['dest_remote_work_rate'] == row['dest_remote_work_rate'] else None,
        int(row['income_differential']) if row['income_differential'] == row['income_differential'] else None,
        float(row['agi_per_household']) if row['agi_per_household'] == row['agi_per_household'] else None,
        float(row['origin_lat']) if row['origin_lat'] == row['origin_lat'] else None,
        float(row['origin_lon']) if row['origin_lon'] == row['origin_lon'] else None,
        float(row['dest_lat']) if row['dest_lat'] == row['dest_lat'] else None,
        float(row['dest_lon']) if row['dest_lon'] == row['dest_lon'] else None,
    ))

con.executemany("INSERT INTO migration_states VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
con.execute("CHECKPOINT")

del df
del rows
gc.collect()

# Verify
count = con.execute("SELECT COUNT(*) FROM migration_states").fetchone()[0]
years = con.execute("SELECT COUNT(DISTINCT survey_year) FROM migration_states").fetchone()[0]
sample = con.execute("""
    SELECT origin_state_name, dest_state_name, survey_year, 
           households, origin_lat, origin_lon, dest_lat, dest_lon
    FROM migration_states
    WHERE survey_year = 2023
    ORDER BY households DESC
    LIMIT 5
""").fetchdf()

print(f"\n=== Summary ===")
print(f"migration_states: {count:,} rows, {years} years")
print(f"\nTop 5 migration flows 2023:")
print(sample.to_string())

con.close()