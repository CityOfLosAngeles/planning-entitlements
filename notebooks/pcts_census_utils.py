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
    # Cache the df as an attribute on the function
    # so we only have to read it from s3 once.
    df = getattr(grab_census_table, "df", None)
    if df is None:
        df = pd.read_parquet(f's3://{bucket_name}/data/final/census_cleaned.parquet')
        grab_census_table.df = df
    # Subset the df according to year and variable
    cols = ['GEOID', 'new_var', 'num']
    df = df[(df.year == year) & 
            (df.table == table_name) & 
            (df.main_var == main_var)][cols]
    return df


def make_wide(df, cols):
    return (
        df[df.new_var.isin(cols)]
        .assign(num=df.num.astype("Int64"))
        .pivot(index="GEOID", columns="new_var", values="num")
        .reset_index()
        .rename_axis(None, axis=1)
    )

def aggregate_group(df, aggregate_me, name="aggregated_group"):
    df = (df.assign(
        new_var2 = df.apply(lambda row: name if any(x in row.new_var for x in aggregate_me)
                             else row.new_var, axis = 1)
        ).groupby(['GEOID', 'new_var2'])
          .agg({'num':'sum'})
          .reset_index()
          .rename(columns = {'new_var2':'new_var'})
    )
    
    return df