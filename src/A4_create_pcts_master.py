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


# Find parents
def find_parents():
    # Import PCTS
    pcts = pd.read_parquet(f's3://{bucket_name}/data/final/master_pcts.parquet')
    keep = ['CASE_ID', 'PARENT_CASE', 'CASE_NBR']
    pcts = pcts[keep].drop_duplicates()

    # Parse with the regex, rather than PCTSParser because it's faster this way
    cols = pcts.CASE_NBR.str.extract(pcts_parser.GENERAL_PCTS_RE)

    all_prefixes = cols[0]
    all_suffixes = cols[3].str[1:].str.split("-")


    m1 = (pd.merge(pcts, all_prefixes, how = "left", 
                    left_index=True, right_index=True)
            .rename(columns = {0: "prefix"})
            )

    df = (pd.merge(m1, all_suffixes, how = "left", 
                left_index=True, right_index=True)
        .rename(columns = {3: "suffix"})
        )

    # Turn the list of prefixes into dummies
    df1 = (pd.get_dummies(df.prefix.apply(pd.Series).stack()).sum(level=0)
        .fillna(0)
        .drop(columns = "ZAI")
        )

    # ZAI appears in prefixes..but is not a valid prefix, it's actually a suffix
    new = pd.merge(df, df1, left_index = True, right_index = True)
    
    # Turn the list of suffixes into dummies
    df2 = (pd.get_dummies(df.suffix.apply(pd.Series).stack()).sum(level=0)
        .fillna(0)
        .drop(columns = "")
        )
    new2 = pd.merge(new, df2, left_index = True, right_index = True) 
    

    # Resolve differences. 
    # A couple of suffixes appear in both prefix and suffix list.
    # This is because the suffix does appear in the prefix position.
    # We want to capture this info once and correctly move it into a suffix dummy. 
    appears_twice = ["CUB", "EIR", "CUZ", ]

    def grab_max(df, col):
        col_x = f"{col}_x"
        col_y = f"{col}_y"
        
        group_together = [col_x, col_y]
        
        new_name = f"{col}"
        
        df = (df.assign(
            new = df[group_together].max(axis=1)
            ).rename(columns = {"new": new_name})
            .drop(columns = group_together)
        )
        
        return df

    for c in appears_twice:
        new2 = grab_max(new2, c)


    # Reorder columns
    prefix_order = ['PARENT_CASE', 'AA', 'ADM', 'APCC', 'APCE', 'APCH', 'APCNV',
        'APCS', 'APCSV', 'APCW', 'CHC', 'CPC', 'DIR', 'ENV', 'PAR', 'PM', 'ZA']
    
    suffix_order = ['1A', '2A', 'AC', 'ACI', 'ADD1', 'ADU', 'AIC', 
        'BL', 'BSA', 'CA', 'CASP', 'CATEX', 'CC', 'CC1', 'CC3', 'CCMP', 'CDO', 
        'CDP', 'CE', 'CEX', 'CLQ', 'CM', 'CN', 'COA', 'COC', 'CPIO', 'CPIOA', 
        'CPIOC', 'CPIOE', 'CPU', 'CR', 'CRA', 'CU', 'CUB', 'CUC', 'CUE', 'CUW', 'CUX', 'CUZ', 
        'CWC', 'CWNC', 'DA', 'DB', 'DD', 'DEM', 'DI', 'DPS', 'DRB', 'EAF', 'EIR', 'ELD', 
        'EXT', 'EXT2', 'EXT3', 'EXT4', 'F', 'GB', 'GPA', 'GPAJ', 'HCA', 'HCM', 'HD', 'HPOZ', 
        'ICO', 'INT', 'M1', 'M2', 'M3', 'M6', 'M7', 'M8', 'M9', 'M10', 'M11', 'MA', 'MAEX',
        'MCUP', 'MEL', 'MND', 'MPA', 'MPC', 'MPR', 'MSC', 'MSP', 'NC', 'ND', 'NR',
        'O', 'OVR', 'P', 'PA', 'PA1', 'PA2', 'PA3', 'PA4', 'PA5', 'PA6', 'PA7', 'PA9', 'PA10',
        'PA15', 'PA16', 'PA17', 'PAB', 'PAD', 'PMEX', 'PMLA', 'PMW', 'POD', 
        'PP', 'PPR', 'PPSP', 'PSH', 'PUB', 'QC', 'RAO', 'RDP', 'RDPA',
        'REC1', 'REC2', 'REC3', 'REC4', 'REC5', 'REV', 'RFA', 'RV',
        'SCEA', 'SCPE', 'SE', 'SIP', 'SL', 'SLD', 'SM', 'SN', 'SP', 'SPE', 'SPP', 'SPPA', 'SPPM', 
        'SPR', 'SUD', 'SUP1', 'TC', 'TDR', 'TOC', 'UAIZ', 'UDU', 'VCU', 'VSO', 'VZC', 'VZCJ',
        'WDI', 'WTM', 'YV', 'ZAA', 'ZAD', 'ZAI', 'ZBA', 'ZC', 'ZCJ', 'ZV', 
        ]

    col_order = prefix_order + suffix_order 

    parents = (new2[col_order]
                .sort_values('PARENT_CASE')
                .reset_index(drop=True)
            )
    
    # Clean up
    # Now get the max for all the dummies, keep only 1 observation of each PARENT_CASE
    parents = parents.pivot_table(index = ['PARENT_CASE'], aggfunc = 'max').reset_index()

    return parents


# Create master PCTS and parent cases df and export to S3
df = merge_pcts()
df.to_parquet(f's3://{bucket_name}/data/final/master_pcts.parquet')

parent_cases = find_parents()
parent_cases.to_parquet(f's3://{bucket_name}/data/final/parents_with_prefix_suffix.parquet')