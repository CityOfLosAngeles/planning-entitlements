# Data Workflow Notes
These notes clarify related datasets in `catalog.yml`. 

1. [PCTS Data](#pcts-data)
    * [Parcel Data](#parcel-data)
1. [Census Data](#census-data)
1. [TOC Analysis](#toc-analysis)

## PCTS Data
Raw PCTS data is `pcts`. Processed PCTS data is `pcts2`. The script `src/A4_create_pcts_master.py` creates the processed PCTS data and the parent cases in `pcts_parents`.  

To use PCTS data for analysis, use the `subset_pcts()` function within `pcts_census_utils.py`. One can subset the PCTS data by a certain start date and a list of prefixes / suffixes. By default, all the prefixes / suffixes are included. 

Following the subset function, one needs to merge in the parent cases, `pcts_parents`, to drop the child cases. The parent cases inherits all the prefixes and suffixes from all the child cases. 

### Parcel Data
The LA County Tax Assessor provides the parcel data, and parcel IDs are called AIN or APN. In our project, we use AIN (string) as the parcel ID.

Parcels are finicky to work with, not only because of the sheer size of the dataset, but because the same parcel geometry can be associated with multiple AINs. Often, we need to look at whether parcels fall within other polygons, such as a zoning boundary, a TOC tier boundary, etc. Instead of doing polygon-on-polygon spatial operations, we use the parcel centroid to see if that centroid falls within another larger polygon. 

Similarly, use the parcel centroid to determine whether the same parcel geometry is linked to several AINs. The script `src/A3_store_parcel_work.py` creates a `parcels_with_duplicates` dataset, but the `crosswalk_parcels_tracts` **should be used for analysis.** 
* Within `crosswalk_parcels_tracts`, the `num_AIN` column tells how many parcels share that geometry; 1 means there are no duplicates, 2 means 2 parcels share the same geometry, and so on. About 15% of the parcel observations have duplicate geometries, but it varies between 2 to several hundred. 
* The column `parcelsqft` is how many square feet that particular parcel is.
* THe column `parcel_tot` is the total square feet by summing up the square feet of all parcels within a tract *after* dropping the duplicate parcel geometries. This area will most certainly be smaller than the the tract's area, since streets also take up space within a tract.
* The column `TOC_Tier` ranges from 0 to 4, with 0 being no tier, and 1-4 being existing TOC tier levels.

When parcels are combined with PCTS entitlement data, we do not know ahead of time which AIN is assigned the entitlement. So, we keep all the possible parcels until PCTS data is merged in. Then, if there are any duplicate parcels, those need to be dropped. 

Notebook `notebooks/C6_toc-census-stats.ipynb` drops the duplicate parcels before calculating what percent of a tract is eligible based on number of AINs and area (parcel_tot).


## Census Data
The scripts that download the Census data using the Census API are in `src/C1_download_census.R` through `src/C4_subset_census.py`. 

`census_cleaned_full` is the output created in `src/C3_clean_census.py`. This is the full cleaned dataset from 2010-2018, including all the Census variables.

`census_cleaned` is the output created in `src/C4_subset_census.py`, which pares down the dataset to just outcomes of interest. For example, commute mode includes single-vehicle, carpool 2 people, carpool 3 people, etc. We care about non-car commute modes of walking / biking / transit, and those are the only ones kept from the commute mode table. 

`census_analysis_table` is the reshaped and combined tract-level output using 2018 5-year ACS data created in `notebooks/B1-census-tract-stats.ipynb`. **This file should be used for analysis, as it includes all the outcomes of interest for this project.** 


## TOC Analysis
* Reconstructed TOC tiers file not in catalog. Insetad, create locally with `toc_tiers = utils.reconstruct_toc_tiers_file()`
