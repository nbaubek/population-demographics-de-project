# IRS Migration Data — Notes

## Source

IRS Statistics of Income (SOI) migration data is published annually at:
https://www.irs.gov/statistics/soi-tax-stats-migration-data

Files follow the naming pattern:
- State outflows: `stateoutflow{YY}{YY}.csv` (e.g., `stateoutflow1213.csv` for 2012–2013)
- County outflows: `countyoutflow{YY}{YY}.csv`

The partition year in the pipeline uses the **second year** of the migration period (e.g., `stateoutflow1213.csv` → year=2013 partition).

## Available Years

Migration data is available from tax year 2011–2012 through 2022–2023 (11 years of flows). The pipeline covers 2012–2023.

## Aggregate Codes (Excluded in Silver Layer)

IRS uses special destination FIPS codes for aggregated flows:

| Code | Meaning |
|------|---------|
| `96` | Total Migration — US and Foreign (all outflows from origin) |
| `97` | Total Migration — US (same state + different state, excludes foreign) |
| `98` | Total Migration — Foreign (Puerto Rico + Abroad) |
| `59` | Puerto Rico aggregate flows (IRS internal aggregation code, not a real FIPS) |
| `57` | Foreign (distinct from 98 — separate IRS classification) |

**Suppressed flows:** IRS suppresses flows below a privacy threshold. These are indicated by `n1 = -1` or `n2 = -1` and are excluded in the silver layer.

These codes are stored raw in the bronze layer (as-is from IRS CSV) and filtered out during silver layer INSERT via:
```sql
WHERE CAST(y2_statefips AS INT) NOT IN (96, 97, 98, 59, 57)
  AND n1 != '-1'
  AND n2 != '-1'
```

## Data Structure

### State Outflows CSV
| Column | Description |
|--------|-------------|
| `y1_statefips` | Origin state FIPS (2-digit) |
| `y2_statefips` | Destination state FIPS (2-digit, or aggregate code) |
| `y2_state` | Destination state abbreviation |
| `y2_state_name` | Destination state name |
| `n1` | Number of non-exempt returns (households) |
| `n2` | Number of exempt returns (individuals in those households) |
| `AGI` | Adjusted gross income in thousands of dollars |

### County Outflows CSV
| Column | Description |
|--------|-------------|
| `y1_statefips` | Origin state FIPS (2-digit) |
| `y1_countyfips` | Origin county FIPS (3-digit within state) |
| `y2_statefips` | Destination state FIPS (2-digit, or aggregate code) |
| `y2_countyfips` | Destination county FIPS (3-digit, or aggregate code) |
| `y2_state` | Destination state abbreviation |
| `y2_countyname` | Destination county name |
| `n1` | Non-exempt returns |
| `n2` | Exempt returns |
| `agi` | Adjusted gross income in thousands of dollars |

## Geography ID Construction

```
state_outflows:  LPAD(y1_statefips, 2, '0') → e.g., "06" for California
county_outflows: CONCAT(LPAD(y1_statefips, 2, '0'), LPAD(y1_countyfips, 3, '0')) → e.g., "06037" for Los Angeles County
```

## Non-Migrants

Rows where `origin_geography_id = dest_geography_id` represent non-migrants (people who stayed in the same geography). These are valid rows and are **not** filtered out — they represent the baseline population for graph edges.

## Encoding

County outflow CSV files contain non-UTF-8 characters. Bronze ingestion uses `errors='replace'` during decode before passing to polars CSV parser.

## Data Quality Notes

### County Row Count Variance After Filtering

Row counts for county outflows (post-filter, all codes removed) across years:

| Year | Rows After Filtering |
|------|---------------------|
| 2012 | 95,730 |
| 2013 | 97,678 |
| 2014 | 52,660 |
| 2015 | 42,192 |
| 2016–2023 | 53,000–60,000 |

The drop starting in 2014 is **expected and not a pipeline issue**. The IRS increased suppression thresholds around 2014 for privacy protection, removing more low-volume county-to-county flows from publication. Pre-2014 counts are higher because more flows were published before the methodology change. Post-change counts stabilize in the 53K–60K range.

Downstream consumers should be aware that 2014+ county analysis covers fewer flow pairs due to intentional IRS disclosure avoidance — the underlying migration still occurs but is not published.

## Relevant Links

- [IRS SOI Migration Data](https://www.irs.gov/statistics/soi-tax-stats-migration-data)
- [IRS SOI Migration Data Dictionary 2011–2012 (PDF)](https://www.irs.gov/pub/irs-soi/1213inpublicmigdoc.pdf)
- [IRS SOI Migration Data Dictionary 2022–2023 (PDF)](https://www.irs.gov/pub/irs-soi/2223inpublicmigdoc.pdf)
- [IRS SOI Data Documentation](https://www.irs.gov/statistics/soi-tax-stats-publications)