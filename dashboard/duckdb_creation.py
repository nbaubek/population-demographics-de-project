import duckdb
import json
import os
import glob
from pathlib import Path
import gc

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "static" / "data"
DB_PATH = SCRIPT_DIR / "data.duckdb"

con = duckdb.connect(DB_PATH)

# --- Drop existing tables ---
con.execute("DROP TABLE IF EXISTS states")
con.execute("DROP TABLE IF EXISTS counties")
con.execute("DROP TABLE IF EXISTS migration_states")

# --- Create tables ---
con.execute("""
    CREATE TABLE states (
        geography_id VARCHAR,
        geography_name VARCHAR,
        state_fips VARCHAR,
        survey_year INTEGER,
        total_population BIGINT,
        median_age DOUBLE,
        median_household_income BIGINT,
        median_home_value BIGINT,
        median_gross_rent BIGINT,
        pct_white_alone DOUBLE,
        pct_black_alone DOUBLE,
        pct_asian_alone DOUBLE,
        pct_hispanic_or_latino DOUBLE,
        poverty_rate DOUBLE,
        bachelors_plus_rate DOUBLE,
        high_school_rate DOUBLE,
        owner_occupancy_rate DOUBLE,
        renter_rate DOUBLE,
        drove_alone_rate DOUBLE,
        walked_to_work_rate DOUBLE,
        remote_work_rate DOUBLE,
        centroid_lat DOUBLE,
        centroid_lon DOUBLE,
        geometry_geojson VARCHAR,
        ALAND BIGINT,
        AWATER BIGINT
    )
""")

con.execute("""
    CREATE TABLE counties (
        geography_id VARCHAR,
        geography_name VARCHAR,
        state_fips VARCHAR,
        county_fips VARCHAR,
        survey_year INTEGER,
        total_population BIGINT,
        median_age DOUBLE,
        median_household_income BIGINT,
        median_home_value BIGINT,
        median_gross_rent BIGINT,
        pct_white_alone DOUBLE,
        pct_black_alone DOUBLE,
        pct_asian_alone DOUBLE,
        pct_hispanic_or_latino DOUBLE,
        poverty_rate DOUBLE,
        bachelors_plus_rate DOUBLE,
        high_school_rate DOUBLE,
        owner_occupancy_rate DOUBLE,
        renter_rate DOUBLE,
        drove_alone_rate DOUBLE,
        walked_to_work_rate DOUBLE,
        remote_work_rate DOUBLE,
        geometry_geojson VARCHAR,
        ALAND BIGINT,
        AWATER BIGINT
    )
""")

con.execute("""
    CREATE TABLE migration_states (
        origin_geography_id VARCHAR,
        dest_geography_id VARCHAR,
        survey_year INTEGER,
        households BIGINT,
        individuals BIGINT,
        agi BIGINT,
        is_non_migrant BOOLEAN,
        origin_state_name VARCHAR,
        origin_total_population BIGINT,
        origin_median_income BIGINT,
        origin_poverty_rate DOUBLE,
        origin_remote_work_rate DOUBLE,
        dest_state_name VARCHAR,
        dest_total_population BIGINT,
        dest_median_income BIGINT,
        dest_poverty_rate DOUBLE,
        dest_remote_work_rate DOUBLE,
        income_differential BIGINT,
        agi_per_household DOUBLE,
        origin_lat DOUBLE,
        origin_lon DOUBLE,
        dest_lat DOUBLE,
        dest_lon DOUBLE
    )
""")

print("Tables created ✓")

# --- Load states ---
print("\nLoading states...")
inserted_states = set()

for fpath in sorted(glob.glob(str(DATA_DIR / "states_*_median_household_income.json"))):
    year = int(os.path.basename(fpath).split('_')[1])
    with open(fpath) as f:
        data = json.load(f)

    rows = []
    for feature in data['features']:
        p = feature['properties']
        key = (p.get('geography_id'), p.get('survey_year'))
        if key in inserted_states:
            continue
        inserted_states.add(key)
        rows.append((
            p.get('geography_id'), p.get('geography_name'), p.get('state_fips'),
            p.get('survey_year'), p.get('total_population'), p.get('median_age'),
            p.get('median_household_income'), p.get('median_home_value'),
            p.get('median_gross_rent'), p.get('pct_white_alone'), p.get('pct_black_alone'),
            p.get('pct_asian_alone'), p.get('pct_hispanic_or_latino'),
            p.get('poverty_rate'), p.get('bachelors_plus_rate'), p.get('high_school_rate'),
            p.get('owner_occupancy_rate'), p.get('renter_rate'), p.get('drove_alone_rate'),
            p.get('walked_to_work_rate'), p.get('remote_work_rate'),
            p.get('centroid_lat'), p.get('centroid_lon'),
            json.dumps(feature['geometry']) if feature.get('geometry') else None,
            # States row - change last two values
            p.get('aland'), p.get('awater')  # was p.get('ALAND'), p.get('AWATER')

        ))

    if rows:
        con.executemany("INSERT INTO states VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        con.execute("CHECKPOINT")

    del data
    del rows
    gc.collect()

    print(f"  states {year} ✓")

# --- Load counties ---
print("\nLoading counties...")
inserted_counties = set()

for fpath in sorted(glob.glob(str(DATA_DIR / "counties_*_median_household_income.json"))):
    year = int(os.path.basename(fpath).split('_')[1])

    with open(fpath) as f:
        data = json.load(f)

    rows = []
    for feature in data['features']:
        p = feature['properties']
        key = (p.get('geography_id'), p.get('survey_year'))
        if key in inserted_counties:
            continue
        inserted_counties.add(key)
        rows.append((
            p.get('geography_id'), p.get('geography_name'), p.get('state_fips'),
            p.get('county_fips'), p.get('survey_year'), p.get('total_population'),
            p.get('median_age'), p.get('median_household_income'), p.get('median_home_value'),
            p.get('median_gross_rent'), p.get('pct_white_alone'), p.get('pct_black_alone'),
            p.get('pct_asian_alone'), p.get('pct_hispanic_or_latino'),
            p.get('poverty_rate'), p.get('bachelors_plus_rate'), p.get('high_school_rate'),
            p.get('owner_occupancy_rate'), p.get('renter_rate'), p.get('drove_alone_rate'),
            p.get('walked_to_work_rate'), p.get('remote_work_rate'),
            json.dumps(feature['geometry']) if feature.get('geometry') else None,
            # Counties row - same fix
            p.get('aland'), p.get('awater')
        ))

    if rows:
        con.executemany("INSERT INTO counties VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
        con.execute("CHECKPOINT")

    del data
    del rows
    gc.collect()

    print(f"  counties {year} ✓")

# --- Summary ---
print("\n=== Summary ===")
for table in ['states', 'counties']:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"{table}: {count:,} rows")

# --- File size ---
db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
print(f"\nDuckDB file size: {db_size:.1f} MB")

con.close()
print(f"DuckDB file: {DB_PATH}")