"""Shared constants for Census API pipeline."""

import dagster as dg

# Year partitions: ACS 5-year data available from 2012 onwards
# Note: Education table (B15003) with modern codes starts at 2012
YEAR_PARTITIONS = dg.StaticPartitionsDefinition([str(y) for y in range(2012, 2025)])

# All 50 US states + DC (FIPS codes as zero-padded strings)
STATE_FIPS_CODES = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55", "56",
]

STATE_PARTITIONS = dg.StaticPartitionsDefinition(STATE_FIPS_CODES)

# Multi-partition definition for tracts (year x state)
TRACT_PARTITIONS = dg.MultiPartitionsDefinition({
    "year": YEAR_PARTITIONS,
    "state": STATE_PARTITIONS,
})

# S3 bucket paths
BRONZE_BUCKET = "s3://population-demographics-iceberg/bronze"

# ACS Census API variables (stable since 2012)
ACS_VARIABLES = {
    "B01001_001E": "total_population",
    "B01002_001E": "median_age",
    "B03002_003E": "white_alone_not_hispanic",
    "B03002_004E": "black_alone_not_hispanic",
    "B03002_006E": "asian_alone_not_hispanic",
    "B03002_012E": "hispanic_or_latino",
    "B19013_001E": "median_household_income",
    "B17001_001E": "poverty_total",
    "B17001_002E": "below_poverty_level",
    "B15003_001E": "education_total_25y_plus",
    "B15003_022E": "bachelors_degree",
    "B15003_023E": "masters_degree",
    "B15003_024E": "professional_degree",
    "B15003_025E": "doctorate_degree",
    "B15003_017E": "high_school_diploma",
    "B15003_002E": "no_high_school_diploma",
    "B25077_001E": "median_home_value",
    "B25003_001E": "total_occupied",
    "B25003_002E": "owner_occupied",
    "B25003_003E": "renter_occupied",
    "B25064_001E": "median_gross_rent",
    "B07001_001E": "migration_total",
    "B08301_001E": "commute_total_workers_16_plus",
    "B08301_003E": "drove_alone_to_work",
    "B08301_019E": "walked_to_work",
    "B08301_021E": "worked_from_home",
}

# Geography configurations for ACS assets
GEO_CONFIG = {
    "states": {
        "api_for": "state:*",
        "s3_path": f"{BRONZE_BUCKET}/census_acs5/states",
        "asset_name": "bronze_acs5_states",
    },
    "counties": {
        "api_for": "county:*",
        "s3_path": f"{BRONZE_BUCKET}/census_acs5/counties",
        "asset_name": "bronze_acs5_counties",
    },
    "tracts": {
        "api_for": "tract:*",
        "state_filter": "06",  # California
        "s3_path": f"{BRONZE_BUCKET}/census_acs5/tracts",
        "asset_name": "bronze_acs5_tracts",
    },
}
