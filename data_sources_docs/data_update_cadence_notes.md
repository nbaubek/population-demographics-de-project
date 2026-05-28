# Data Update Cadence

This document describes the update schedules for the data sources used in the population demographics pipeline.

## TIGER Boundary Data (Bureau)

**Source:** U.S. Census Bureau Geography Division

**Frequency:** Annual

**Release Timing:** Late September (e.g., 2024 vintage released September 25, 2024; 2025 vintage released September 23, 2025)

**Vintage Date:** Boundaries reflect legal boundaries and names as of January 1st of the release year.

## ACS Demographic Data (Census)

**Source:** U.S. Census Bureau American Community Survey (ACS)


| Estimate Type | Release Month | Population Threshold |
|---------------|---------------|---------------------|
| 1-Year Estimates | September (Mid-month) | Areas with 65,000+ population |
| 1-Year Supplemental Estimates | October (Mid-month) | Areas with 20,000+ population |
| 5-Year Estimates | December / January | All geographic areas (down to block group level) |

*Note: While 5-Year Estimates are historically announced in December, final public dataset releases and dependent pipeline updates frequently extend into January (e.g., the 2020–2024 data was officially released on January 29, 2026).*

## IRS Statistics of Income (SOI) - Migration Data

**Source:** IRS Statistics of Income Division, Form 1040 tax return data

**Frequency:** Annual

**Release Timing:** Spring (typically reflects two consecutive processing years; e.g., State and County Migration Data for 2022–2023 was released March 2024; subsequent updates follow this annual Q1/Q2 window)

**Data Tracking:** Year-to-year address changes reported on individual Form 1040 tax returns.

## IRS Personal Wealth Statistics

**Source:** IRS Statistics of Income Division, Form 706 (United States Estate and Generation Skipping Transfer Tax Return)

**Frequency:** Triennial (every 3 years)

**Available Data Years:** 2022, 2019, 2016, 2013, 2010, 2007, 2004, 2001

**Release Timing:** Varies; data is typically published after intensive multi-year processing and validation (e.g., Tax Year 2019 data was published in April 2024, and Tax Year 2022 data was finalized in Spring 2026).

**Geographic Granularity:** National and state-level

**Data Contents:**
- Total and selected assets
- Debts and mortgages
- Net worth
- Breakdowns by gender, age, and state of residence

**Methodology:** Uses the Estate Multiplier technique to estimate the wealth of the living population. Estimates are limited to individuals whose gross assets meet or exceed the federal estate tax filing threshold.


---

*Last updated: 2026-05-26*