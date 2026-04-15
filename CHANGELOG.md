# CHANGELOG



## Version 0.2.4
April 14, 2026

### Features

- Introduced handling for caching outer-layer geographies when query via the `confined_download()` interface



## Version 0.2.3
April 05, 2026

### Features

- Introduced support for confining downloads to a different (outer-layer) set of geographic specifiers than otherwise supported



## Version 0.2.2
April 05, 2026

### Fixes

- Added FIPS codes for AIANNH and county subdivision geographies

- Changed FIPS libraries' naming convention

- Implemented handling for TIGER shapefiles and support for caching preferences



## Version 0.2.1
March 26, 2026

### Features

- Added handling for TIGER shapefiles (not yet rigged)

- Incorporated metadata on TIGER shapefile identifying info to be extracted alongside geometric info

### TODO

- Caching preferences to locally download high-byte size TIGER shapefiles

- Incorporating shapefile querying alongside data queries



## Version 0.2.0
March 26, 2026

### Features

- Removed support for querying data by collections of geographies

- Overhaul support for fetching synchronously and concurrently (via coroutine)

- Provided end-user support for viewing metadata on geographic information

- API keys can now be set in the OS environment (prioritized) or text file. Customization for the locations of either setting is allowed.

- Enabled caching for geographic specifiers (ensures thread safety and slightly faster process for large queries)

### TODO

- Set shapefile handling to development



## Version 0.1.0
March 01, 2026

- First release of `acspsuedo`