# Utils to when merging data from PCTS and Census
import numpy as np
import pandas as pd

bucket_name = "city-planning-entitlements"

#---------------------------------------------------------------------------------------#
## Census functions
#---------------------------------------------------------------------------------------#
"""
This most straightforward way to reshape from long to wide
Use number values, not percents, we can always derive percents later on if we need.
If we're aggregating to geographies that involve slicing parts of tracts, we need numbers, not percents.
"""
def grab_census_table(table_name, year, main_var):
    df = pd.read_parquet(f's3://{bucket_name}/data/final/census_cleaned.parquet')
    cols = ['GEOID', 'new_var', 'num']
    df = df[(df.year == year) & 
            (df.table == table_name) & 
            (df.main_var == main_var)][cols]
    return df


def make_wide(df, numerator_var, denominator_var, numerator_renamed, denominator_renamed): 
    numerator_renamed = f'{numerator_renamed}'
    denominator_renamed = f'{denominator_renamed}'
    
    df = df.assign(
        numerator = df.apply(lambda row: row.num if row.new_var==numerator_var 
                                     else np.nan, axis=1),
        denominator = df.apply(lambda row: row.num if row.new_var==denominator_var 
                                       else np.nan, axis=1),
    )
    
    
    df = df.assign(
        numerator = df.numerator.fillna(df.groupby('GEOID')['numerator'].transform('max')),
        denominator = df.denominator.fillna(df.groupby('GEOID')['denominator'].transform('max')),
    )
    
    
    keep_col = ['GEOID', 'numerator', 'denominator']
    
    df = (df[keep_col].drop_duplicates()
          .sort_values('GEOID')
          # If the max by GEOID was still NaN, fill it in now with 0
          .assign(
              numerator = df.numerator.fillna(0).astype(int),
              denominator = df.denominator.fillna(0).astype(int),
          )
          .rename(columns = {'numerator': numerator_renamed, 
                               'denominator': denominator_renamed})   
          .reset_index(drop=True)
         )
    
    return df


def aggregate_group(df, aggregate_me):
    df = (df.assign(
        new_var2 = df.apply(lambda row: 'aggregated_group' if any(x in row.new_var for x in aggregate_me)
                             else row.new_var, axis = 1)
        ).groupby(['GEOID', 'new_var2'])
          .agg({'num':'sum'})
          .reset_index()
          .rename(columns = {'new_var2':'new_var'})
    )
    
    return df