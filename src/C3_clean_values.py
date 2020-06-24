# Clean the Census table values to be consistent across years
"""
ACS might report values as percents or numbers --
This only affects S1903 (median hh income by race). 

For consistency across years, create 2 columns: number and percent.
Need the number column later on, when tracts are aggregated up to larger geographies.
Also, treat dollar values as number values (but do not fill in percent column).
ACS always reports in the current year's inflation adjusted dollars.
"""

import numpy as np
import pandas as pd
from tqdm import tqdm 
tqdm.pandas() 
from datetime import datetime

bucket_name = 'city-planning-entitlements'

#--------------------------------------------------------------------#
## Functions to be used
#--------------------------------------------------------------------#
# (1) Sort out which tables use numbers, percents, dollars, or a mix

# Case 1: Income table is a mix of numbers, dollars, and percents
def clean_income(df):
    # Create column called var_type
    def income_type(row):
        if (row.main_var=='hh') & (row.second_var=='total'):
            return 'number'
        elif (row.main_var == 'medincome'):
            return 'dollar'
        elif (row.main_var=='hh') & (row.second_var != 'hh') & (row.year <= 2016):
            return 'percent'
        elif (row.main_var=='hh') & (row.second_var != 'hh') & (row.year >= 2017):
            return 'number'

    df = df.assign(
            var_type = df.progress_apply(income_type, axis = 1),
            # Create a denominator column. Use this to convert percent values into numbers.
            denom = df.progress_apply(lambda row: row.estimate if row.new_var=='hh_total' 
                             else np.nan, axis = 1)
        )

    return df


# Case 2: tables with mostly percents, but give also total (the denominator)
def clean_tables_percents_with_total(df):
    # Create column called var_type
    def var_type(row):
        if (row.last2=='01') & (row.second_var=='total'):
            return 'number'
        elif row.last2!='01':
            return 'percent'
    
    df = df.assign(
            var_type = df.progress_apply(var_type, axis = 1),
            # Create a denominator column. Use this to convert percent values into numbers.
            denom = df.progress_apply(lambda row: row.estimate if row.second_var=='total' 
                             else np.nan, axis = 1)
        )
    
    return df


# Case 3: tables with only numbers
def clean_tables_numbers_only(df):
    df = df.assign(
            var_type = 'number',
            # Create a denominator column. Use this to convert percent values into numbers.
            denom = df.progress_apply(lambda row: row.estimate if row.second_var=='total' 
                             else np.nan, axis = 1)
        )
    
    return df


# (2) Now create "percent" and "number" columns
def create_pct_num_cols(df):
    # Fill in denom so it's the same for each tract-year
    df = df.assign(
            denom = df.denom.fillna(df.groupby(
                    ['GEOID', 'year', 'table', 'main_var'])['denom'].transform('max')),
        )
    
    def create_pct_col(row):
        if row.var_type == 'percent':
            return (row.estimate / 100)
        elif (row.var_type == 'number') & (row.denom > 0):
            return (row.estimate / row.denom)
        elif (row.var_type == 'number') & (row.denom == 0):
            return 0
        elif row.var_type == 'dollar':
            return np.nan
        
    def create_num_col(row):
        if row.var_type in ['number', 'dollar']:
            return row.estimate
        elif row.var_type == 'percent':
            return (row.estimate / 100 * row.denom)
        
    # Add pct and num columns
    df = df.assign(
            denom = df.denom.astype("Int64"),
            pct = df.progress_apply(create_pct_col, axis=1),
            num = df.progress_apply(create_num_col, axis=1).round(0),
        )
    

    return df


#--------------------------------------------------------------------#
# Apply functions
#--------------------------------------------------------------------#
time0 = datetime.now()
print(f'Start time: {time0}')

df = pd.read_parquet(f's3://{bucket_name}/data/intermediate/census_tagged.parquet')


# (1) Sort out which tables use numbers, percents, dollars, or a mix
income = df[df.table=='income']

pct_table_list = ['commute', 'vehicles']
percent_tables = df[df.table.isin(pct_table_list)]

num_table_list = ['race', 'tenure', 'incomerange']
number_tables = df[df.table.isin(num_table_list)]


income = clean_income(income)
percent_tables = clean_tables_percents_with_total(percent_tables)
number_tables = clean_tables_numbers_only(number_tables)

time1 = datetime.now()
print(f'Clean tables by type: {time1 - time0}')


# (2) Now create "percent" and "number" columns
df2 = (
    pd.concat([
        income,
        percent_tables,
        number_tables,
    ], sort=False)
    .reset_index(drop=True)
)

df2 = create_pct_num_cols(df2)

time2 = datetime.now()
print(f'Create pct and num cols: {time2 - time0}')

# Export to S3
df2.to_parquet(f's3://{bucket_name}/data/final/census_cleaned_full.parquet')

time3 = datetime.now()
print(f'Total execution time: {time3 - time0}')