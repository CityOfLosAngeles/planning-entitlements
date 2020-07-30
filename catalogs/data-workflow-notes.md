# Data Workflow Notes
These notes clarify related datasets in `catalog.yml`. 

1. [PCTS Data](#pcts-data)
1. [Census Data](#census-data)
1. [TOC Analysis](#toc-analysis)

## PCTS Data
Raw PCTS data is `pcts`. Processed PCTS data is `pcts2`. The script `src/A4_create_pcts_master.py` creates the processed PCTS data and the parent cases in `pcts_parents`.  

To use PCTS data for analysis, use the `subset_pcts()` function within `pcts_census_utils.py`. One can subset the PCTS data by a certain start date and a list of prefixes / suffixes. By default, all the prefixes / suffixes are included. 

Following the subset function, one needs to merge in the parent cases, `pcts_parents`, to drop the child cases. The parent cases inherits all the prefixes and suffixes from all the child cases. 

## Census Data
The scripts that download the Census data using the Census API are in `src/C1_download_census.R` through `src/C4_subset_census.py`. 

`census_cleaned_full` is the output created in `src/C3_clean_census.py`. This is the full cleaned dataset from 2010-2018, including all the Census variables.

`census_cleaned` is the output created in `src/C4_subset_census.py`, which pares down the dataset to just outcomes of interest. For example, commute mode includes single-vehicle, carpool 2 people, carpool 3 people, etc. We care about non-car commute modes of walking / biking / transit, and those are the only ones kept from the commute mode table. 

`census_analysis_table` is the reshaped and combined tract-level output using 2018 5-year ACS data created in `notebooks/B1-census-tract-stats.ipynb`. **This file should be used for analysis, as it includes all the outcomes of interest for this project.** 


## TOC Analysis
* Reconstructed TOC tiers file not in catalog. Insetad, create locally with `toc_tiers = utils.reconstruct_toc_tiers_file()`
