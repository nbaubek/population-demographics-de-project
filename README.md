# Population Demographics Data Engineering Pipeline Project

---

## Data Sources

1. [**Census Data**](https://www.census.gov/programs-surveys/popest/data.html). More about the ACS [here](https://www.census.gov/programs-surveys/acs.html) and [here](https://en.wikipedia.org/wiki/American_Community_Survey). The data of interest for us is **American Community Survey 5-Year Data (2009-2024)**. That is the backbone of this project. The ACS is an ongoing annual survey conducted by the U.S. Census Bureau. It provides a multidimensional snapshot of the U.S. population's demographic, social, economic, and housing characteristics — enabling analysis of how these factors interrelate across individuals, households, and geographies.

It captures five interconnected dimensions:

- Demographics — age, sex, race, ethnicity, nativity, citizenship (who they are)
- Household & Family Structure — household composition, relationship types, living arrangements (how they're organized)
- Housing — tenure (own/rent), housing costs, units, physical conditions (where and how they live)
- Economic Characteristics — employment, occupation, industry, income, poverty status (how they sustain themselves)
- Social Characteristics — education, language, disability, health insurance, commuting (how they participate in society)

**Geography hierarchy for each year**

- `state:*` → 52 rows (50 states + DC + Puerto Rico)
- `county:*` → ~3,200 rows (all US counties)
- `tract:*` with state/county filter → census tracts for that area. There are about ~84000 tracts across the US as of 2024.

Each geography level includes the FIPS code as a column (e.g. "state": "01" for Alabama), which gets preserved after renaming — that's the state column showing up in the output.

    - [API Guide](https://www.census.gov/data/developers/guidance/api-user-guide.Example_API_Queries.html#accordion-24b3247975-item-cc8dcd2152)
    - [Table IDs explained](https://www.census.gov/programs-surveys/acs/data/data-tables/table-ids-explained.html#accordion-889eff65b1-item-a1554d738d) . Each table is assigned an ID by which its content can be recognized.
    - [Some more explanations](https://www.youtube.com/watch?v=A2HrsOS8omI) regarding table Table IDs and Variable names
    - [Search Table IDs and Variables by the code](https://data.census.gov/table/)
    - Since this API allows to query by geographic entities, here's the [hierarchy](https://www2.census.gov/geo/pdfs/reference/geodiagram.pdf) of how those entities relate to each other.

2. [**World Bank API**](https://data360.worldbank.org/en/api)
3. [**Data Commons API**](https://docs.datacommons.org/api/)
4. **TIGER/Line GIS Data** — Topologically Integrated Geographic Encoding and Referencing system. Provides geographic boundary files (states, counties, census tracts) used for spatial joins and visualization. See [GIS Geography explainer](https://gisgeography.com/tiger-gis-data-topologically-integrated-geographic-encoding-referencing/).

---

**API access**

- Census Data apparently requires API key registration starting May 12th, 2026. You can get it [here](https://api.census.gov/data/key_signup.html)

---

## What makes this project interesting

The idea is to build a system that models *population characteristics* across geography and time. On top of that, I intended to build a data platform.

**What is Data Platform?**

At a high level:
- A data platform is a reusable system that enables ingestion, storage, transformation, governance, and consumption of data at scale.
    - The important word is: *reusable*
    - A pipeline solves one task.
    - A platform enables many future tasks.

**What challenges does this project present?**

1. 3 APIs one of which serves as the backbone, the other 2 are for enrichment.
2. graph model, dashboard, kepler.gl, etc.

[Kepler GL](https://kepler.gl/)

---

## What story this project uncovers

**What I'm trying to achieve**

The answer I'm trying to provide is:
> "Understanding how demographic and economic conditions evolve spatially and relationally across regions."

---

## Overview of the Project in Stages

**Stack:** Dagster (orchestration) + dlt (ingestion) + AWS S3/Glue/Athena (storage/query)

### Stage 1: Raw Data Ingestion (Parquet) ✅

- **Tool:** dlt pipeline → S3 (filesystem destination)
- **Output:** Parquet files in `s3://population-demographics-iceberg/raw/census_acs5/`
- **Assets:** `census_states`, `census_counties`, `census_tracts_ca`
- **Status:** Working — all 3 assets materialized successfully as `.parquet`
- **Dependencies:** `dlt[s3]`, `dlt[parquet]` (pyarrow)
- **Credentials:** AWS via `AWS_PROFILE` env var (no static keys in `secrets.toml`)

**S3 structure after ingestion:**
```
s3://population-demographics-iceberg/
├── bronze/
│   └── census_acs5/
│       ├── states/           (.parquet)
│       ├── counties/         (.parquet)
│       └── tracts_ca/         (.parquet)
└── silver/
    └── census_acs5/
        ├── states/           (Iceberg)
        ├── counties/         (Iceberg)
        └── tracts/           (Iceberg)
```

### Stage 2: Iceberg Conversion ✅

- **Tool:** Athena CTAS (CREATE TABLE AS SELECT to convert parquet → Iceberg)
- **Why:** Iceberg adds ACID transactions, time-travel, partition evolution
- **Process:**
  1. Drop any existing silver table (graceful handling if not exists)
  2. Create external table over bronze parquet in `population_demographics_silver`
  3. Run CTAS with schema transformations and deduplication
  4. Clean up external table
- **Result:** Iceberg tables in `population_demographics_silver` database
- **Assets:** `census_states_silver`, `census_counties_silver`, `census_tracts_ca_silver`
- **Schema transformations:**
  - `NAME` → `geography_name`
  - `state`/`county`/`tract` → `state_fips`/`county_fips`/`tract_fips` (LPAD padded)
  - `state` + `county` + `tract` → `geography_id` (concatenated FIPS)
  - Year constant added
  - All numeric columns cast from string (dlt writes Census API data as binary strings)
- **Partitioning:**
  - `census_states_silver`: by `state` (52 partitions)
  - `census_counties_silver`: by `state` only (avoid ~3000+ county partitions exceeding 100 writer limit)
  - `census_tracts_ca_silver`: by `state, county, tract`
- **Deduplication:** Latest record per geography (ROW_NUMBER PARTITION BY geography ORDER BY ingest_date DESC)
- **Key fixes applied:**
  - `is_external = false` in Iceberg WITH clause (managed tables only)
  - Explicit CAST to BIGINT/DOUBLE for all numeric columns (parquet binary vs expected types)
  - Raw partition columns included in CTAS SELECT output
  - Graceful DROP TABLE handling for metastore inconsistencies
- **Status:** Working — all 3 assets materialized successfully as Iceberg tables

### Stage 3: Data Transformation (Silver → Gold)

- **Tool:** dbt or SQL transformations via Athena
- **Silver:** Cleaned, deduplicated, joined census data
- **Gold:** Socioeconomic models, regional comparisons
- **Status:** Pending

### Stage 4: Analytics & Visualization

- **Tools:** Kepler.gl (spatial visualization), BI dashboards
- **Focus:** Spatiotemporal patterns of demographic change

---

## Infrastructure Setup (Terraform)

### Overview

Infrastructure is managed via Terraform in the `infra/` directory. Provisions AWS resources for data storage, cataloging, and querying.

### Resources Created

| Resource | Type | Purpose |
|----------|------|---------|
| `aws_s3_bucket.iceberg_data` | S3 | Main data lake storage |
| `aws_glue_catalog_database` (x3) | Glue | Bronze/Silver/Gold layer catalogs |
| `aws_s3_bucket.athena_results` | S3 | Athena query results storage |
| `aws_athena_workgroup` | Athena | Query execution engine (v3 for Iceberg) |
| `aws_iam_role.pipeline_role` | IAM | Pipeline access for Glue/Athena/S3 |

### Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.0
- AWS credentials configured (e.g., via `~/.aws/credentials` or environment variables)
- AWS account with permissions for S3, Glue, Athena, IAM

### Initial Setup

```bash
cd infra
terraform init
```

### Apply Infrastructure

```bash
terraform plan
terraform apply
```

### Outputs After Apply

| Output | Description |
|--------|-------------|
| `iceberg_bucket_name` | e.g. `population-demographics-iceberg` |
| `athena_results_bucket` | e.g. `population-demographics-athena-results` |
| `athena_workgroup_name` | `population-demographics` |
| `pipeline_role_arn` | IAM role ARN for pipeline access |

### Bucket Naming Convention

Base name + suffix:
- `{bucket_name}-iceberg` → raw data bucket
- `{bucket_name}-athena-results` → query results

Default base: `population-demographics`

### Configuration Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `aws_region` | `us-east-1` | AWS region |
| `bucket_name` | `population-demographics` | Base name for all buckets |

---

## Data Modeling

### Census ACS 5-Year Estimates (2009-2024)

All data comes from the [American Community Survey 5-Year Estimates](https://www.census.gov/programs-surveys/acs/data/data-tables/table-ids-explained.html). ACS is an ongoing survey providing demographic, social, economic, and housing characteristics. The 5-year estimates cover a rolling 5-year window (e.g., 2024 = 2019-2024).

**Note:** Education table B15003 uses modern codes (with aggregate totals, no gender split) from **2012 onwards**. Earlier years (2009-2011) use B15002 with gender split (male/female sections) and different variable codes. All other tables use stable codes across the full 2009-2024 range. 

### Census ACS Variables

#### B01001 — Sex by Age
*Universe: Total population*

| Column | Code | Description |
|--------|------|-------------|
| `total_population` | B01001_001E | Total population |
| `median_age` | B01002_001E | Median age |

#### B03002 — Hispanic or Latino Origin by Race
*Universe: Total population (excludes Hispanic/Latino origin)*

| Column | Code | Description |
|--------|------|-------------|
| `white_alone_not_hispanic` | B03002_003E | White alone, not Hispanic or Latino |
| `black_alone_not_hispanic` | B03002_004E | Black or African American alone, not Hispanic or Latino |
| `asian_alone_not_hispanic` | B03002_006E | Asian alone, not Hispanic or Latino |
| `hispanic_or_latino` | B03002_012E | Hispanic or Latino origin |

#### B07001 — Geographic Mobility by Sex
*Universe: Population 1 year and over*

| Column | Code | Description |
|--------|------|-------------|
| `migration_total` | B07001_001E | Total population (used for migration/ mobility metrics) |

#### B08301 — Means of Transportation to Work
*Universe: Workers 16 years and over*

| Column | Code | Description |
|--------|------|-------------|
| `commute_total_workers_16_plus` | B08301_001E | Total workers 16 years and over |
| `drove_alone_to_work` | B08301_003E | Car, truck, or van — drove alone |
| `walked_to_work` | B08301_019E | Walked |
| `worked_from_home` | B08301_021E | Worked from home |

#### B15003 — Educational Attainment
*Universe: Population 25 years and over*

| Column | Code | Description |
|--------|------|-------------|
| `education_total_25y_plus` | B15003_001E | Total population 25 years and over |
| `no_high_school_diploma` | B15003_002E | No schooling completed or kindergarten through grade 12 |
| `high_school_diploma` | B15003_017E | High school graduate, includes equivalency (GED) |
| `bachelors_degree` | B15003_022E | Bachelor's degree |
| `masters_degree` | B15003_023E | Master's degree |
| `professional_degree` | B15003_024E | Professional school degree |
| `doctorate_degree` | B15003_025E | Doctorate degree |

**Availability:** B15003 with modern codes exists from **2012 onwards**. Earlier years (2009-2011) used B15002 with gender split (male/female sections) and different variable codes. Since we only need aggregate totals, we use B15003 from 2012+ and exclude pre-2012 education data.

#### B17001 — Poverty Status by Sex by Age
*Universe: Population for whom poverty status is determined*

| Column | Code | Description |
|--------|------|-------------|
| `poverty_total` | B17001_001E | Total population (for poverty determination) |
| `below_poverty_level` | B17001_002E | Income below poverty level |

#### B19013 — Median Household Income
*Universe: Occupied housing units with household income*

| Column | Code | Description |
|--------|------|-------------|
| `median_household_income` | B19013_001E | Median household income |

#### B25003 — Tenure
*Universe: Occupied housing units*

| Column | Code | Description |
|--------|------|-------------|
| `total_occupied` | B25003_001E | Total occupied housing units |
| `owner_occupied` | B25003_002E | Owner occupied |
| `renter_occupied` | B25003_003E | Renter occupied |

#### B25064 — Median Gross Rent
*Universe: Occupied housing units paying rent*

| Column | Code | Description |
|--------|------|-------------|
| `median_gross_rent` | B25064_001E | Median gross rent (dollars) |

#### B25077 — Median Value
*Universe: Owner-occupied housing units*

| Column | Code | Description |
|--------|------|-------------|
| `median_home_value` | B25077_001E | Median value (dollars) |

---

## Project Structure

```
population-demographics-pipeline/
├── README.md
├── pyproject.toml              # Root project (dlt, dagster-dlt, etc.)
├── .env                         # Root env (Census API key only)
├── census_acs5_explorer.py     # Standalone Census API exploration script
├── worldbank-data.py           # World Bank API exploration script
├── infra/                      # Terraform (AWS resources)
│   ├── main.tf
│   ├── s3.tf
│   ├── glue.tf
│   ├── athena.tf
│   └── iam.tf
└── dagster-orch/                # Dagster orchestration project
    ├── pyproject.toml
    ├── src/dagster_orch/
    │   └── defs/census_api/    # Census dlt source + pipeline
    │       ├── loads.py
    │       └── defs.yaml
    └── .dlt/
        ├── config.toml         # S3 bucket URL, parquet format
        └── secrets.toml        # Census API key (gitignored)
```

---

## Conclusions about the project

Places are interconnected systems, not isolated demographic snapshots.