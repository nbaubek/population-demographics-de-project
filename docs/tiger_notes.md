# TIGER/Line Shapefiles — Notes

## Overview

TIGER/Line shapefiles from the Census Bureau provide geographic boundary files (states, counties, census tracts) with FIPS codes, names, and geometry (WKT format).

## Geometry WKT Storage

TIGER data is stored with `geometry_wkt` column containing Well-Known Text representations of polygon boundaries. This is used downstream for Kepler.gl visualization.

**Critical:** Missing geometry (`geometry_wkt IS NULL`) will silently pass through bronze and silver, then break Kepler.gl rendering. An asset check on the bronze layer catches this.

## Boundary Changes

### Tracts Change Every Decade
Census tracts are redefined every 10 years (2010, 2020, 2030). Tract boundaries for prior decades become invalid. The pipeline handles this through year-partitioned bronze and silver — each year's tract geometry is stored separately.

### County and State Boundaries Are Stable
County and state boundaries are largely stable across decades, but the pipeline still year-partitions all geographies for consistency and to support future TIGER updates.

## FIPS Code Conventions

- **State FIPS:** 2-digit, zero-padded (e.g., "06" for California)
- **County FIPS:** 3-digit within state (e.g., "037" for Los Angeles County, CA)
- **Tract FIPS:** 6-digit (e.g., "900101" for census tract 1.01 in a county)

The `geography_id` in silver is constructed as:
```
states:   LPAD(state_fips, 2, '0')
counties: CONCAT(LPAD(state_fips, 2, '0'), LPAD(county_fips, 3, '0'))
tracts:   CONCAT(LPAD(state_fips, 2, '0'), LPAD(county_fips, 3, '0'), tract_fips)
```

## pygris Limitations

The Census Bureau's `pygris` Python package provides a programmatic interface to TIGER/Line shapefiles. Known limitations:

1. **Large Downloads** — Tract-level downloads for all US states are large. The pipeline downloads state-by-state via `pygris.tracts(state="CA", year=2022)`.

2. **Year Mismatch** — `pygris` defaults to the most recent TIGER year, not necessarily the ACS year being ingested. The pipeline specifies `year=` explicitly.

3. **Geometry in Memory** — `pygris` returns `geopandas.GeoDataFrame` objects which must be converted to WKT strings for parquet storage.

## Relevant Links

- [TIGER/Line Shapefiles](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)
- [pygris documentation](https://pysal.org/pygris/)
- [FIPS Codes](https://www.census.gov/geo/reference/codes.html)