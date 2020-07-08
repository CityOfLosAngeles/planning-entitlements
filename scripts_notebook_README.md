Data Catalog, Scripts, Notebooks README
==============================

This README provides a high-level documentation of the scripts used to produce the analysis. Scripts are located in `src`; analysis is found in `notebooks`.

1. [Data Catalog and S3](#data-catalog-and-s3)
1. [Scripts](#scripts)
    * A. PCTS and City Planning Work
    * B. Zone Parser Work
    * C. Census Work
1. [Notebooks](#notebooks)

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
* `A2_import_assessor_parcels`: Save the 2014 parcels shapefile as zipped shapefile and parquet (geoparquets require geopandas >= 0.8). Save 2019 parcels data as parquet.
* `A3_store_parcel_work`: Complete all further parcel-related cleaning and processing, especially since parcels can have different AINs but the same polygon coordinates. Join parcels to TOC Tiers.
* `A4_create_pcts_master`: Make a master PCTS file and parent_case file.
* `A5_create_crosswalks`: Crosswalks are correspondence tables used to merge and join various datasets together. Create crosswalks to help us create our analysis datasets in a flexible way.

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
* `pcts_parser`:  

## Notebooks