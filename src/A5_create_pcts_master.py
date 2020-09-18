"""
Make a master PCTS file.
""" 
import pandas as pd
import geopandas as gpd
import intake

catalog = intake.open_catalog("./catalogs/*.yml")
bucket_name = 'city-planning-entitlements'

# Import data
cases = pd.read_parquet(f's3://{bucket_name}/data/raw/tCASE.parquet')
app = pd.read_parquet(f's3://{bucket_name}/data/raw/tAPLC.parquet')
geo_info = pd.read_parquet(f's3://{bucket_name}/data/raw/tPROP_GEO_INFO.parquet')
la_prop = pd.read_parquet(f's3://{bucket_name}/data/raw/tLA_PROP.parquet')

crosswalk_parcels_tracts = (catalog.crosswalk_parcels_tracts.read()
    [["uuid", "AIN"]]
)

# Define functions to create master PCTS data
def merge_pcts(cases, geo_info, la_prop, app, crosswalk_parcels_tracts):
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
    
    final = join_tables(cases2, geo_info1, la_prop1, app1, crosswalk_parcels_tracts)
  
    return final

def join_tables(cases2, geo_info1, la_prop1, app1, crosswalk_parcels_tracts):
    # Merge with geo_info, la_prop, parcels to ID the parcels that have entitlements
    # Inner join would lose appeals cases, switch to left join instead.
    # (1) Merge cases and geo_info to get PROP_ID
    m1 = pd.merge(cases2, geo_info1, on = 'CASE_ID', how = 'left', validate = '1:m')

    correct_joins = m1[m1.PROP_ID.notna()]
    incorrect_joins = m1[m1.PROP_ID.isna()]

    # (2) Merge geo_info and la_prop to link PROP_IDs and AINs
    # Drop PROP_IDs that wouldn't have gotten linked to any AIN anyway
    m2 = (pd.merge(geo_info1[["PROP_ID"]].drop_duplicates(), 
               la_prop1, 
               on = "PROP_ID", how = "left", validate = "1:m")
      .rename(columns = {"ASSR_PRCL_NBR": "AIN"})
     )

    print(f"# obs in m2: {len(m2)}")
    m2 = m2[m2.AIN.isin(crosswalk_parcels_tracts.AIN)]
    print(f"# obs in m2 after dropping AINs not in our crosswalk: {len(m2)}")

    # (3a) Fix incorrect obs with a m:m merge so they can get PROP_ID using PARENT_CASE
    incorrect_joins_with_propid = pd.merge(
                            incorrect_joins.drop(columns = ["PROP_ID"]), 
                            geo_info1.rename(columns = {"CASE_ID": "PARENT_CASE"}), 
                            on = "PARENT_CASE", how = "left", validate = "m:m"
    )

    incorrect_joins_with_ain = pd.merge(
        incorrect_joins_with_propid, m2,
        on = "PROP_ID", how = "left", validate = "m:1"
    )
    
    # (3b) Get rid of obs where we can't link to PROP_ID and AIN
    incorrect_joins_now_fixed = incorrect_joins_with_ain[incorrect_joins_with_ain.PROP_ID.notna()]
    
    # (4a) Merge in AIN info using PROP_ID for correct ones after m1
    correct_joins_with_ain = pd.merge(correct_joins, m2, 
                            on = "PROP_ID", how = "inner", validate = "m:1")
    
    # (4b) Concatenate the 2 parts together
    m3 = (pd.concat([
            correct_joins_with_ain, 
            incorrect_joins_now_fixed
        ], axis=0)
        .sort_values(["CASE_ID", "AIN", "PROP_ID"])
        .drop_duplicates(subset = ["CASE_ID", "AIN"])
        .reset_index(drop=True)
    )

    # (5) Merge in app to get project description
    m4 = pd.merge(m3, app1, on = "APLC_ID", how = "left", validate = "m:1")

    m5 = (
        m4.drop(columns = ['PROP_ID', 'parent_is_null'])
        # Nothing dropped here, but just in case
        .drop_duplicates()
        .sort_values(['CASE_ID', 'AIN'])
        .reset_index(drop=True)
    )

    print_statements(m1, correct_joins, incorrect_joins, 
                     incorrect_joins_with_propid, incorrect_joins_with_ain, 
                     geo_info1, incorrect_joins_now_fixed, 
                     m3, m4, m5)

    return m5

def print_statements(m1, correct_joins, incorrect_joins, 
                     incorrect_joins_with_propid, incorrect_joins_with_ain, 
                     geo_info1, incorrect_joins_now_fixed, 
                     m3, m4, m5):
    print(f"# obs when we join cases and geo_info: {len(m1)}")
    print(f"# obs where PROP_ID was NaN: {len(incorrect_joins)}")
    print(f"% where PROP_ID was NaN: {len(incorrect_joins) / len(m1)}")

    print(f"# unique CASE_IDs in correct_joins: {correct_joins.CASE_ID.nunique()}")
    print(f"# unique CASE_IDs in incorrect_joins: {incorrect_joins.CASE_ID.nunique()}")

    print(f"# unique PARENT_CASEs in correct_joins: {correct_joins.PARENT_CASE.nunique()}")
    print(f"# unique PARENT_CASEs in incorrect_joins: {incorrect_joins.PARENT_CASE.nunique()}")
   
   # Of these incorrect joins, do they share parent cases with ones that were joined?
    print("# unique PARENT_CASEs that were correctly joined, but also appear in incorrect_joins")
    print(f"{incorrect_joins[incorrect_joins.PARENT_CASE.isin(correct_joins.PARENT_CASE)].PARENT_CASE.nunique()}")

    print(f"# obs in incorrect_joins before m:m merge: {len(incorrect_joins)}")
    print(f"# unqiue PARENT_CASEs in incorrect_joins before m:m merge: {incorrect_joins.PARENT_CASE.nunique()}")
    print(f"# obs in incorrect_joins after m:m merge: {len(incorrect_joins_with_propid)}")
    print(f"# unqiue PARENT_CASEs in incorrect_joins after m:m merge: {incorrect_joins_with_propid.PARENT_CASE.nunique()}")

    print(f"# obs in incorrect_joins once we add in AIN: {len(incorrect_joins_with_ain)}")
    print(f"# unqiue PARENT_CASEs once we add in AIN: {incorrect_joins_with_ain.PARENT_CASE.nunique()}")
    
    lost_parents = (incorrect_joins_with_ain[incorrect_joins_with_ain.PROP_ID.isna()]
                    [["PARENT_CASE"]].drop_duplicates()
                )

    print(f"# unique lost PARENT_CASEs: {len(lost_parents)}")
    print(f"Double check, try to find some in geo_info: {len(geo_info1[geo_info1.CASE_ID.isin(lost_parents.PARENT_CASE)])}")

    print(f"# obs in incorrect_joins that were fixed: {len(incorrect_joins_now_fixed)}")
    print(f"# unique PARENT_CASEs in incorrect_joins that were fixed: {incorrect_joins_now_fixed.PARENT_CASE.nunique()}")
    print(f"# unique PARENT_CASEs in incorrect_joins before all this: {incorrect_joins.PARENT_CASE.nunique()}")
    
    print(f"# obs in m3: {len(m3)}")
    print(f"# obs in m1: {len(m1)}")

    print(f"# unique CASE_IDs in m3: {m3.CASE_ID.nunique()}")
    print(f"# unique CASE_IDs in m1: {m1.CASE_ID.nunique()}")

    print(f"# unique PARENT_CASEs in m3: {m3.PARENT_CASE.nunique()}")
    print(f"# unique PARENT_CASEs in m1: {m1.PARENT_CASE.nunique()}")

    print(f"# obs in m4: {len(m4)}")
    print(f"# unique CASE_ID in m4: {m4.CASE_ID.nunique()}")
    print(f"# unique PARENT_CASEs in m4: {m4.PARENT_CASE.nunique()}")

    print(f"# obs in m5: {len(m5)}")
    print(f"# unique CASE_ID in m5: {m5.CASE_ID.nunique()}")
    print(f"# unique PARENT_CASEs in m5: {m5.PARENT_CASE.nunique()}")


# Create master PCTS and parent cases df and export to S3
df = merge_pcts(cases, geo_info, la_prop, app, crosswalk_parcels_tracts)
#df.to_parquet(f's3://{bucket_name}/data/final/master_pcts.parquet')
