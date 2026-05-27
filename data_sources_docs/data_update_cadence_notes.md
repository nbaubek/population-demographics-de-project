# Data Update Cadence

This document describes the update schedules for the data sources used in the population demographics pipeline.

## TIGER Boundary Data (Bureau)

**Source:** U.S. Census Bureau Geography Division

**Frequency:** Annual

**Release Timing:** Late summer or early fall (e.g., 2025 vintages released September 2025; 2026 updates follow similar Q3 pattern)

**Vintage Date:** Boundaries reflect legal boundaries and names as of January 1st of the release year

## ACS Demographic Data (Census)

**Source:** U.S. Census Bureau American Community Survey (ACS)

| Estimate Type | Release Month | Population Threshold |
|---------------|---------------|---------------------|
| 1-Year Estimates | September | Areas with 65,000+ population |
| 1-Year Supplemental Estimates | October | Areas with 20,000+ population |
| 5-Year Estimates | December | All geographic areas (down to block group level) |

## IRS Statistics of Income (SOI) - Migration Data

**Source:** IRS Statistics of Income Division, Form 1040 tax return data

**Frequency:** Annual

**Release Timing:** Spring (typically reflects two consecutive processing years; e.g., State and County Migration Data for 2022–2023 was released March 2024)

**Data Tracking:** Year-to-year address changes reported on individual Form 1040 tax returns

## IRS Personal Wealth Statistics

**Source:** IRS Statistics of Income Division, Form 706 (United States Estate and Generation Skipping Transfer Tax Return)

**Frequency:** Triennial (every 3 years)

**Available Data Years:** 2019, 2016, 2013, 2007, 2004, 2001

**Release Timing:** Varies; data published after processing and validation

**Geographic Granularity:** National and state-level

**Data Contents:**
- Total and selected assets
- Debts and mortgages
- Net worth
- Breakdowns by gender, age, and state of residence

**Methodology:** Uses the Estate Multiplier technique to estimate wealth of the living population. Estimates are limited to persons whose wealth meets the estate tax filing threshold for the estimation period.

**Data Format:** XLS/XLSX files available at IRS Pub/IRS-SOI

**Reference URL:** https://www.irs.gov/statistics/soi-tax-stats-personal-wealth-statistics

---

*Last updated: 2026-05-26*