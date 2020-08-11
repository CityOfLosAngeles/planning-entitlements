"""
Make a master PCTS file.
""" 
import pandas as pd
import geopandas as gpd
import intake
import pcts_parser

catalog = intake.open_catalog("./catalogs/*.yml")
bucket_name = 'city-planning-entitlements'

# Import data
cases = pd.read_parquet('./data/tCASE.parquet')
app = pd.read_parquet('./data/tAPLC.parquet')
geo_info = pd.read_parquet('./data/tPROP_GEO_INFO.parquet')
la_prop = pd.read_parquet('./data/tLA_PROP.parquet')


# Define functions to create master PCTS data
def merge_pcts():
    # Subset dataframes before merging
    keep_col = ['CASE_ID', 'APLC_ID', 'CASE_NBR', 
                    'CASE_SEQ_NBR', 'CASE_YR_NBR', 'CASE_ACTION_ID', 
                    'CASE_FILE_RCV_DT', 'CASE_FILE_DATE', 'PARNT_CASE_ID']

    cases1 = (cases.assign(
        # Grab the year-month from received date
        CASE_FILE_DATE = pd.to_datetime(cases['CASE_FILE_RCV_DT']).dt.to_period('M'),
    )[keep_col])
    
    app1 = app[['APLC_ID', 'PROJ_DESC_TXT']]
    geo_info1 = geo_info[['CASE_ID', 'PROP_ID']]
    la_prop1 = la_prop[la_prop.ASSR_PRCL_NBR.notna()][['PROP_ID', 'ASSR_PRCL_NBR']]
    
    # Identify parent cases
    cases1['parent_is_null'] = cases1.PARNT_CASE_ID.isna()
    cases1['PARENT_CASE'] = cases1.apply(lambda row: row.CASE_ID if row.parent_is_null == True 
                                             else row.PARNT_CASE_ID, axis = 1)
    
    # Keep cases from 2010 onward
    cases2 = cases1[cases1.CASE_FILE_DATE.dt.year >= 2010]
    
    # Merge with geo_info, la_prop, parcels to ID the parcels that have entitlements (10/2017 and after)
    m1 = pd.merge(cases2, geo_info1, on = 'CASE_ID', how = 'inner', validate = '1:m')
    m2 = pd.merge(m1, la_prop1, on = 'PROP_ID', how = 'inner', validate = 'm:1')
    m3 = pd.merge(m2, app1, on = 'APLC_ID', how = 'left', validate = 'm:1')
    
    m3 = (
        m3.assign(
            id = m3.CASE_SEQ_NBR.astype(int).astype(str) + '_' + m3.CASE_FILE_DATE.dt.year.astype(str)
        ).drop(columns = ['PROP_ID', 'parent_is_null'])
        .drop_duplicates()
        .rename(columns = {'ASSR_PRCL_NBR':'AIN'})
        .sort_values(['CASE_ID', 'AIN'])
        .reset_index(drop=True)
    )
  
    return m3


# Create master PCTS and parent cases df and export to S3
df = merge_pcts()
df.to_parquet(f's3://{bucket_name}/data/final/master_pcts.parquet')
