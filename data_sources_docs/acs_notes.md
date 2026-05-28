# Census ACS 5-Year Estimates — Notes

## Variable Stability

Most ACS 5-year estimate variables use **stable codes across all years** (2012–2024):

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

The pipeline uses `YEAR_PARTITIONS` in Dagster starting at 2012 to avoid this discontinuity.

---

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

---

### Census ACS 5-Year Estimates (2009-2024)

All data comes from the [American Community Survey 5-Year Estimates](https://www.census.gov/programs-surveys/acs/data/data-tables/table-ids-explained.html). ACS is an ongoing survey providing demographic, social, economic, and housing characteristics. The 5-year estimates cover a rolling 5-year window (e.g., 2024 = 2019-2024).

### Census ACS Tables and Variables

#### B01001 — Sex by Age

*Universe: Total population*

| Column | Code | Description |
| --- | --- | --- |
| `total_population` | B01001_001E | Total population |

#### B01002 — Median Age by Sex

*Universe: Total population*

| Column | Code | Description |
| --- | --- | --- |
| `median_age` | B01002_001E | Median age |

#### B03002 — Hispanic or Latino Origin by Race

*Universe: Total population*

| Column | Code | Description |
| --- | --- | --- |
| `white_alone_not_hispanic` | B03002_003E | White alone, not Hispanic or Latino |
| `black_alone_not_hispanic` | B03002_004E | Black or African American alone, not Hispanic or Latino |
| `asian_alone_not_hispanic` | B03002_006E | Asian alone, not Hispanic or Latino |
| `hispanic_or_latino` | B03002_012E | Hispanic or Latino origin |

#### B07001 — Geographic Mobility in the Past Year by Age

*Universe: Population 1 year and over*

| Column | Code | Description |
| --- | --- | --- |
| `migration_total` | B07001_001E | Total population 1 year and over |

**Note:** This table provides population denominators for migration rate calculations. Actual *migration flow* data (origin/destination pairs) comes from IRS Statistics of Income, not ACS.

#### B08301 — Means of Transportation to Work

*Universe: Workers 16 years and over*

| Column | Code | Description |
| --- | --- | --- |
| `commute_total_workers_16_plus` | B08301_001E | Total workers 16 years and over |
| `drove_alone_to_work` | B08301_003E | Car, truck, or van — drove alone |
| `walked_to_work` | B08301_019E | Walked |
| `worked_from_home` | B08301_021E | Worked from home |

#### B15003 — Educational Attainment

*Universe: Population 25 years and over*

| Column | Code | Description |
| --- | --- | --- |
| `education_total_25y_plus` | B15003_001E | Total population 25 years and over |
| `no_schooling_completed` | B15003_002E | No schooling completed |
| `regular_high_school_diploma` | B15003_017E | Regular high school diploma |
| `bachelors_degree` | B15003_022E | Bachelor's degree |
| `masters_degree` | B15003_023E | Master's degree |
| `professional_degree` | B15003_024E | Professional school degree |
| `doctorate_degree` | B15003_025E | Doctorate degree |

**Availability:** B15003 with modern codes exists from **2012 onwards**. Earlier years (2009-2011) used B15002 with gender split (male/female sections) and different variable codes. Since we only need aggregate totals, we use B15003 from 2012+ and exclude pre-2012 education data.

#### B17001 — Poverty Status in the Past 12 Months by Sex by Age

*Universe: Population for whom poverty status is determined*

| Column | Code | Description |
| --- | --- | --- |
| `poverty_total` | B17001_001E | Total population for whom poverty status is determined |
| `below_poverty_level` | B17001_002E | Income in the past 12 months below poverty level |

#### B19013 — Median Household Income in the Past 12 Months

*Universe: Households*

| Column | Code | Description |
| --- | --- | --- |
| `median_household_income` | B19013_001E | Median household income in the past 12 months (in inflation-adjusted dollars) |

#### B25003 — Tenure

*Universe: Occupied housing units*

| Column | Code | Description |
| --- | --- | --- |
| `total_occupied` | B25003_001E | Total occupied housing units |
| `owner_occupied` | B25003_002E | Owner occupied |
| `renter_occupied` | B25003_003E | Renter occupied |

#### B25064 — Median Gross Rent

*Universe: Renter-occupied housing units paying cash rent*

| Column | Code | Description |
| --- | --- | --- |
| `median_gross_rent` | B25064_001E | Median gross rent (dollars) |

#### B25077 — Median Value

*Universe: Owner-occupied housing units*

| Column | Code | Description |
| --- | --- | --- |
| `median_home_value` | B25077_001E | Median value (dollars) |