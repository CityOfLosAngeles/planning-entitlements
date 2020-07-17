# Clean Census data
import numpy as np
import pandas as pd
import re

from datetime import datetime
from tqdm import tqdm 
tqdm.pandas() 

bucket_name = 'city-planning-entitlements'

"""
# Compile individual census tables into 1 parquet file
full_df = pd.DataFrame()

for name in ['commute', 'income', 'income_range', 'vehicles', 'tenure', 'race', 'raceethnicity']:
    file_name = f'{name}_tract'
    df = pd.read_csv(f's3://{bucket_name}/data/source/{file_name}.csv', dtype={"GEOID": "str"})
    df = df[['GEOID', 'variable', 'estimate', 'year']]
    df['GEOID'] = df.GEOID.str.pad(width = 11, side = 'left', fillchar = '0')
    full_df = full_df.append(df, sort = False)
    
full_df.to_parquet(f's3://{bucket_name}/data/raw/raw_census.parquet')
"""

#--------------------------------------------------------------------#
## Functions to be used
#--------------------------------------------------------------------#
# (1) Tag ACS table
acs_tables = {
    'S1903': 'income', 
    'B19001': 'incomerange',
    'S0801': 'commute',
    'S0802': 'vehicles',
    'B25008': 'tenure',
    'B02001': 'race',
    'B01001': 'raceethnicity',
}

def tag_acs_table(df):
    pattern = re.compile('([A-Za-z0-9]+)_')
    
    df['table'] = df.progress_apply(
        lambda row: acs_tables.get(pattern.match(row.variable).group(1)),
        axis = 1
    )
    
    # Find the other B19001A, B19001B, etc tables and tag them
    df['table'] = df.progress_apply(
        lambda row: 'incomerange' if 'B19001' in row.variable else row.table,
        axis = 1
    )

    df['table'] = df.progress_apply(
        lambda row: 'raceethnicity' if 'B01001' in row.variable else row.table,
        axis = 1
    )
    
    return df


# (2) Tag main variable
def income_vars(row):
    if '_C01' in row.variable:
        return 'hh'
    elif '_C02' in row.variable:
        return 'medincome'
    elif '_C03' in row.variable:
        return 'medincome'
    
def incomerange_vars(row):
    if 'B19001_' in row.variable:
        return 'total'
    elif 'B19001A' in row.variable:
        return 'white'
    elif 'B19001B' in row.variable:
        return 'black'
    elif 'B19001C' in row.variable:
        return 'amerind'
    elif 'B19001D' in row.variable:
        return 'asian'
    elif 'B19001E' in row.variable:
        return 'pacis'
    elif 'B19001F' in row.variable:
        return 'other'
    elif 'B19001G' in row.variable:
        return 'race2'
    elif 'B19001H' in row.variable:
        return 'nonhisp'
    elif 'B19001I' in row.variable:
        return 'hisp'
    
def vehicle_vars(row):
    if 'C01' in row.variable:
        return 'workers'

def commute_vars(row):
    if 'C01' in row.variable:
        return 'workers'
    elif 'C02' in row.variable:
        return 'male'
    elif 'C03' in row.variable:
        return 'female'

def tenure_vars(row):
    if 'B25008' in row.variable:
        return 'pop'
    
def race_vars(row):
    if 'B02001' in row.variable:
        return 'pop'

def race_eth_vars(row):
    if 'B01001_' in row.variable:
        return 'total'
    elif 'B01001A' in row.variable:
        return 'white'
    elif 'B01001B' in row.variable:
        return 'black'
    elif 'B01001C' in row.variable:
        return 'amerind'
    elif 'B01001D' in row.variable:
        return 'asian'
    elif 'B01001E' in row.variable:
        return 'pacis'  
    elif 'B01001F' in row.variable:
        return 'other'
    elif 'B01001G' in row.variable:
        return 'race2'
    elif 'B01001H' in row.variable:
        return 'nonhisp'
    elif 'B01001I' in row.variable:
        return 'hisp'        


main_vars_dict = {
    'income': income_vars,
    'incomerange': incomerange_vars,
    'vehicles': vehicle_vars,
    'commute': commute_vars,
    'tenure': tenure_vars,
    'race': race_vars,
    'raceethnicity': race_eth_vars,
}


# (3) Tag secondary variable
# Secondary variable - use last 2 characters
income = {'01': 'total', '02': 'white', '03': 'black', '04': 'amerind', '05': 'asian',
       '06': 'pacis', '07': 'other', '08': 'race2', '09': 'hisp', '10': 'nonhisp'}

incomerange = {'01': 'total', '02': 'lt10', '03': 'r10to14', '04': 'r15to19', '05': 'r20to24',
           '06': 'r25to29', '07': 'r30to34', '08': 'r35to39', '09': 'r40to44', '10': 'r45to49',
           '11': 'r50to59', '12': 'r60to74', '13': 'r75to99', '14': 'r100to124', '15': 'r125to149',
           '16': 'r150to199', '17': 'gt200'}

vehicles = {'01': 'total', '94': 'veh0', '95': 'veh1', '96': 'veh2', '97': 'veh3'}

commute = {'01': 'total', '03': 'car1', '05': 'car2', '06': 'car3', '07': 'car4',
          '09': 'transit', '10': 'walk', '11': 'bike', '12': 'other', '13': 'telecommute'}

tenure = {'01': 'total', '02': 'owner', '03': 'renter'}

race = {'01': 'total', '02': 'white', '03': 'black', '04': 'amerind', '05': 'asian',
           '06': 'pacis', '07': 'other', '08': 'race2'}

raceethnicity = {'01':'total'}

def tag_secondary_variable(df):    
    df['last2'] = df['variable'].str[-2:]
     
    def pick_secondary_var(row):
        if row.table=='income':
            return income[row.last2]
        elif row.table=='incomerange':
            return incomerange[row.last2]
        elif row.table=="vehicles":
            return vehicles[row.last2]
        elif row.table=="commute":
            return commute[row.last2]
        elif row.table=="tenure":
            return tenure[row.last2]
        elif row.table=="race":
            return race[row.last2]
        elif row.table=="raceethnicity":
            return raceethnicity[row.last2]
   
    df['second_var'] = df.progress_apply(pick_secondary_var, axis = 1)
   
    return df

#--------------------------------------------------------------------#
# Apply functions
#--------------------------------------------------------------------#
time0 = datetime.now()
print(f'Start time: {time0}')

df = pd.read_parquet(f's3://{bucket_name}/data/raw/raw_census.parquet')

time1 = datetime.now()
print(f'Read in parquet: {time1}')

# (1) Tag ACS table
df = tag_acs_table(df)

time2 = datetime.now()
print(f'Tag ACS table: {time2 - time1}')

# (2) Tag main variable
df['main_var'] = df.progress_apply(lambda row: main_vars_dict[row['table']](row), axis = 1)

time3 = datetime.now()
print(f'Tag main var: {time3 - time2}')

# (3) Tag secondary variable
df = tag_secondary_variable(df)

time4 = datetime.now()
print(f'Tag secondary var: {time4 - time3}')

# Create new_var column
df['new_var'] = df.main_var + "_" + df.second_var

# Export
df.to_parquet(f's3://{bucket_name}/data/intermediate/census_tagged.parquet')

time5 = datetime.now()
print(f'Total execution time: {time5 - time0}')
