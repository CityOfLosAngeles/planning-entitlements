# Subset the ACS tables to just outcomes of interest
import numpy as np
import pandas as pd

bucket_name = 'city-planning-entitlements'

df = pd.read_parquet(f's3://{bucket_name}/data/final/census_cleaned_full.parquet')

def tables_to_keep(df):
    # Define the rows we want to keep
    keep_vehicles = ['total', 'veh0']
    keep_commute = ['total', 'car1', 'transit', 'bike', 'walk']
    keep_tenure = ['total', 'renter']
    
    income_cond = ((df.table=='incomerange') | (df.table=='income') )
    vehicles_cond = ( (df.table=='vehicles') & (df.second_var.isin(keep_vehicles)) )
    commute_cond = ( (df.table=='commute') & (df.second_var.isin(keep_commute)) )
    tenure_cond = ( (df.table=='tenure') & (df.second_var.isin(keep_tenure)) )
    race_cond = ( (df.table=='race') | (df.table=='raceethnicity') )
    
    df2 = df[ income_cond | vehicles_cond | commute_cond | tenure_cond | race_cond ]
    
    # Subset columns
    keep_cols = ['GEOID', 'variable', 'year', 'table', 'main_var', 'last2',
                'second_var', 'new_var', 'pct', 'num']
    
    df2 = df2[keep_cols].sort_values(['table', 'variable', 'year']).reset_index(drop=True)
    
    return df2

df = tables_to_keep(df)
df.to_parquet(f's3://{bucket_name}/data/final/census_cleaned.parquet')