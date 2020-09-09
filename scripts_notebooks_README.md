Data Catalog, Scripts, Notebooks README
==============================

This README provides a high-level documentation of the scripts used to produce the analysis. Scripts are located in `src`; analysis is found in `notebooks`.

1. [Data Catalog and S3](#data-catalog-and-s3)
1. [Scripts](#scripts)
    * A. PCTS and City Planning Work
    * B. Zone Parser Work
    * C. Census Work
1. [Notebooks](#notebooks)
    * A. PCTS Work
    * B. Census Work
    * C. TOC Analysis
    * D. All Entitlements Analysis
    * Utility Functions for All Notebooks


## Data Catalog and S3
All files go through a data ingest, cleaning, and processing phase, and are saved in the S3 bucket for this project (city-planning-entitlements). They are loosely grouped into `data` and `gis` folders, for tabular and geospatial data respectively, and `source`, `raw`, `intermediate`, and `final` sub-folders. 

The most common files needed are saved in the `catalogs` folder. 
`manifest.yml` lists the URLs from the [GeoHub](http://geohub.lacity.org/), which are [saved and versioned](https://github.com/CityOfLosAngeles/planning-entitlements/blob/master/Makefile) into our S3 bucket using the `make-mirror` command. If the open data URLs break in the future, we have a working version in our S3. 
`catalog.yml` holds other data sources commonly used, which may not come from open data portals. These data sources can be databases, S3 files, and/or open data URLs.

## Scripts
Scripts are loosely grouped by A, B, C, etc. and numbered in the order they should be run.

### A. PCTS and City Planning Work
Scripts here deal with raw source files given by City Planning. Raw source files are saved into S3, but also processed to fit into our repository's workflow and organization.

* `A1_load_pcts`: Take the PCTS backup and load into POSTGRES database, so PCTS can be read from `catalog.yml`.
* `A2_import_assessor_parcels`: Load the 2006-2019 parcel data from LA County Tax Assessor and write it as a parquet. Clean up multiple entries across years and join with census tracts.
* `A3_store_parcel_work`: Complete all further parcel-related cleaning and processing. Tag duplicate parcels, join parcels to TOC Tiers.
* `A4_toc_work`: Upload and clean TOC-related files from City Planning. These files are used in `A3_store_parcel_work`. 
* `A5_create_pcts_master`: Make a master PCTS file and parent_case file.
* `A6_create_crosswalks`: Crosswalks are correspondence tables used to merge and join various datasets together. Create crosswalks to help us create our analysis datasets in a flexible way.

### B. Zone Parser Work
Scripts here deal with any outputs or related files needed after the [PCTS zone parser](./src/pcts_parser.py) is used. Figure out what errors arise and what information might need to be extracted and saved as crosswalk files.

* `B1_zone_parsing_codebook`: Store crosswalks and work related to using our zone parser.

### C. Census Work
* `C1_download_census`: R script to download Census tables using the Census API for 2010-2018 5-year ACS tract-level data. Save locally and upload to S3.
* `C2_clean_census`: Clean Census data and extract information needed from the variable string.
* `C3_clean_values`: Census tables can report dollar amounts, numbers, or percentages. Standardize Census data so that it displays number and percent columns, rather than being mixed across years for the same table, and/or mixed across tables.
* `C4_subset_census`: Subset Census data for the outcomes we want for our analysis.

### Other Scripts
* `utils`: Common utility functions to be used in any of the scripts, such as making a geodataframe from x, y coordinates, and making zipped shapefile.
* `pcts_parser`: Zone parser that parsers PCTS case string into prefix, suffix, height district, and overlays.  


## Notebooks
Notebooks are loosely grouped by A, B, C, etc. and numbered in the order they should be run. Numbering is fairly loose, as it is possible to run certain analyses without having run the prior ones in that group. 

### A. PCTS Work
* `A1-pcts-overview`: Get overview of what's in PCTS backup.
* `A2-pcts-validate`: Validate our cleaning of PCTS against counts by prefix and year.
* `A3-parse-zoning`: Use zone parser in `pcts_parser`, validate results, and refine parser. Notes and errors are recorded.  

### B. Census Work
* `B1-census-tract-stats`: Use census functions in `laplan.census` to create a master tract-level table with outcomes of interest.

### C. TOC Analysis
* `C1-build-toc-master`: Create master TOC analysis dataframe by combining PCTS, zoning, and TOC tiers. Master table includes all TOC-eligible parcels with number of TOC entitlements and non-TOC entitlements attached. Summary stats by zone class and TOC tiers.
* `C2-toc-tiers`: Reconstruct TOC tiers using `toc` from Metro bus, rail, and Metrolink. 
* `C3-toc-geometry-stats`: Exploratory analysis of eligible zoning within TOC tiers.
* `C4-toc-line-analysis`: Analysis of TOC entitlements by rule and by bus/rail lines.
* `C5-ent-line-analysis`: Analysis of all entitlements by rule and by bus/rail lines.
* `C6-toc-census-stats`: Census tract stats related to TOC-eligible tracts and tracts with TOC entitlements.
* `C7-ipyleaflet-TOC`: Exploratory analysis of mapping TOC entitlements.


### D. All Entitlements Analysis
* `D1-entitlement-demographics`: Create voila dashboard for PCTS suffixes and census stats.
* `D2-entitlement-regression`: Poisson regression on PCTS suffixes with census stats. 
* `D3-poisson-scale`: Exploratory analysis of Poisson regression using synthetic counts.

### Utility Functions for All Notebooks
* `laplan`: Python package with utility functions for zoning, entitlement, and Census data.
* `utils`: Common utility functions to be used in any of the scripts, such as making a geodataframe from x, y coordinates, and making zipped shapefile.
* `toc`: Functions to analyze TOC entitlements. Brings in GTFS feeds to determine reconstruct TOC tiers from Metro bus, Metro rail, and Metrolink. 
