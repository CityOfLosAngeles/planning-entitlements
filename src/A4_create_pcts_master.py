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
    )
  
    return m3


# Find parents
def find_parents():
    # Import PCTS
    pcts = pd.read_parquet(f's3://{bucket_name}/data/final/master_pcts.parquet')
    keep = ['CASE_ID', 'PARENT_CASE', 'CASE_NBR']
    pcts = pcts[keep].drop_duplicates()

    # Parse PCTS string and grab suffix
    parsed_col_names = ['prefix', 'suffix']

    def parse_pcts(row):
        try:
            z = pcts_parser.PCTSCaseNumber(row.CASE_NBR)
            return pd.Series([z.prefix, z.suffix], index = parsed_col_names)
        except ValueError:
            return pd.Series([a.prefix, z.suffix], index = parsed_col_names)

    parsed = pcts.apply(parse_pcts, axis = 1)
    df = pd.concat([pcts, parsed], axis = 1)
    
    # Turn the list of prefixes into dummies
    df1 = pd.get_dummies(df.prefix.apply(pd.Series).stack()).sum(level=0).fillna(0)
    new = pd.merge(df, df1, left_index = True, right_index = True)

    # Turn the list of suffixes into dummies
    df2 = pd.get_dummies(df.suffix.apply(pd.Series).stack()).sum(level=0).fillna(0)
    new = pd.merge(new, df2, left_index = True, right_index = True)   
    
    parents = (new.drop(columns = ['CASE_ID', 'CASE_NBR', 'prefix', 'invalid', 'suffix'])
                .pivot_table(index = ['PARENT_CASE'], aggfunc = 'max')
                .reset_index()
            )

    return parents


# Create master PCTS and parent cases df and export to S3
df = merge_pcts()
df.to_parquet(f's3://{bucket_name}/data/final/master_pcts.parquet')

parent_cases = find_parents()
parent_cases.to_parquet(f's3://{bucket_name}/data/final/parents_with_prefix_suffix.parquet')