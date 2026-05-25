# Census ACS 5-Year Estimates — Notes

## Variable Stability

Most ACS 5-year estimate variables use **stable codes across all years** (2009–2024):

| Table | Codes | Notes |
|-------|-------|-------|
| B01001 — Sex by Age | B01001_001E, B01001_002E–B01001_025E | Full age-by-sex cross-tab |
| B01002 — Median Age | B01002_001E, B01002_002E, B01002_003E | Total + by race |
| B03002 — Race | B03002_003E through B03002_012E | Detailed race groups |
| B19013 — Median Household Income | B19013_001E | Stable |
| B17001 — Poverty | B17001_001E, B17001_002E | Stable |
| B08301 — Commute to Work | B08301_001E, B08301_003E, B08301_019E, B08301_021E | Stable |
| B25077 — Median Home Value | B25077_001E | Stable |
| B25003 — Occupancy | B25003_001E–B25003_003E | Stable |
| B25064 — Median Gross Rent | B25064_001E | Stable |

### Education Table — Unstable Codes

**B15003 (Education Attainment) uses different variable codes before and after 2012.**

- **2009–2011:** B15002 (gender-split table) — codes B15002_002E through B15002_027E (male) and B15002_028E through B15002_053E (female)
- **2012 onwards:** B15003 (modern aggregate table) — codes B15003_001E through B15003_025E, no gender split

The pipeline uses `YEAR_PARTITIONS` starting at 2012 to avoid this discontinuity.

## API Quirks

### Census API Key Required
All ACS API requests require a `key` parameter. Get a free key at:
https://api.census.gov/data/key_signup.html

Rate limits are generous for local development; the API key should be set in `dagster-orch/.env` as `CENSUS_API_KEY`.

### String Types in Parquet
The Census API returns all values as strings. Bronze parquet files store them as strings. Silver layer casts explicitly to BIGINT/DOUBLE during INSERT.

### Tract-Level Queries
Tracts require `in=state:*` to ensure completeness. The API returns tracts only for requested states. For the full US, this means 50 separate API calls per year (one per state).

The pipeline handles this via `MultiPartitionsDefinition` on `year × state`, with `TRACT_PARTITIONS` spanning all 50 states × 12 years.

### ACS 5-Year Estimates Are Rolling Windows

ACS 5-year estimates represent a rolling 5-year window. For example:
- 2024 ACS 5-year = 2019–2024
- 2023 ACS 5-year = 2018–2023

The `survey_year` in the pipeline refers to the **end year** of the estimate window, consistent with how the Census Bureau labels releases.

## Relevant Links

- [ACS 5-Year Estimates Overview](https://www.census.gov/programs-surveys/acs/data/data-tables/table-ids-explained.html)
- [ACS API Documentation](https://api.census.gov/data.html)
- [Variable Codes Reference](https://api.census.gov/data/2022/acs/acs5/variables.html)