# IRS Personal Wealth Statistics — Notes

*Also potential candidate for data enrichment in this pipeline*

## Source

IRS Statistics of Income (SOI) Personal Wealth Statistics are published at:
https://www.irs.gov/statistics/soi-tax-stats-personal-wealth-statistics

Data is derived from **Form 706, United States Estate (and Generation Skipping Transfer) Tax Return**.

## Overview

The Personal Wealth Study uses the **Estate Multiplier technique** to estimate the wealth of the living population. Since not everyone files Form 706 (only estates above the filing threshold), the IRS uses estate tax data combined with mortality statistics to extrapolate wealth estimates for the broader population.

**Important Limitation:** Estimates are limited to persons whose personal wealth meets or exceeds the estate tax filing threshold in effect for the estimation period. This creates a structural undercount of lower-wealth households.

## Available Years

Data is published triennially (every 3 years):

| Year | Release Date |
|------|--------------|
| 2019 | 2022 |
| 2016 | 2019 |
| 2013 | 2016 |
| 2007 | 2010 |
| 2004 | 2007 |
| 2001 | 2004 |

## Data Contents

### Asset Categories
- Total assets (gross estate)
- Selected asset types (real estate, stocks, bonds, business interests, etc.)
- Debts and mortgages
- Net worth (assets minus debts)

### Demographic Breakdowns
- Gender
- Age brackets
- State of residence

### Geographic Granularity
- National totals
- State-level data

## File Format

Data is published as Excel (XLS/XLSX) files at:
https://www.irs.gov/pub/irs-soi/

Files follow naming patterns like `wealth{YY}*.xlsx` (e.g., `wealth19all.xlsx` for 2019 all states).

## Methodology Notes

### Estate Multiplier Technique

```
Living population wealth = (Estate wealth / Estate threshold population) × General population mortality rate
```

This technique accounts for the fact that estate tax filings represent a subset of high-wealth individuals. By applying mortality-based multipliers, the IRS estimates total wealth including those below the filing threshold.

### Filing Threshold

The estate tax filing threshold varies by year:

| Year | Filing Threshold (approx.) |
|------|---------------------------|
| 2001 | $675,000 |
| 2004 | $1,000,000 |
| 2007 | $1,000,000 |
| 2013 | $5,000,000 (ATRA) |
| 2016 | $5,490,000 |
| 2019 | $11,400,000 |

Threshold increases over time affect comparability across years.

## Relationship to Other Data

Unlike the **IRS Migration Data** (year-to-year address changes from Form 1040), the Personal Wealth Study captures wealth distribution snapshots from estate tax filings. This is a different data source with different:

- **Frequency:** Triennial vs. annual
- **Unit of analysis:** Individual decedents vs. tax households
- **Geographic granularity:** State-level vs. county-level (migration data)
- **Coverage:** High-wealth estates only vs. all filers

## Relevant Links

- [Personal Wealth Statistics Main Page](https://www.irs.gov/statistics/soi-tax-stats-personal-wealth-statistics)
- [Personal Wealth Study Metadata](https://www.irs.gov/statistics/soi-tax-stats-personal-wealth-study-metadata)
- [IRS SOI Data Tables](https://www.irs.gov/pub/irs-soi/)
- [Form 706 Instructions](https://www.irs.gov/forms-instructions/form-706-instructions)

## Data Quality Considerations

1. **Threshold effects:** Wealth estimates exclude those below the estate tax threshold, understating total wealth held by lower and middle-income households.

2. **Triennial gaps:** Limited data points make trend analysis difficult — only ~6 data points spanning 2001–2019.

3. **State disclosure suppression:** Small state samples may be suppressed to protect taxpayer confidentiality.

4. **Non-filer adjustment:** The estate multiplier assumes estate filing rates are consistent across demographics, which may not hold perfectly.