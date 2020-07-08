"""
This zone parsing codebook will be used along the notebooks/utils.py ZoningInfo data class and 
src/pcts_parser.py ZoningInfo data class.

After the zoning string is parsed, there were still observations that failed to be parsed
Those were manually coded. 
Save the failed to be parsed codebook and general codebook for zoning string into S3.
"""
import numpy as np
import pandas as pd 

bucket_name = 'city-planning-entitlements'

# Import the codebook we want to use for parse fails
df = pd.read_excel(f's3://{bucket_name}/references/Zoning_Parser_Codebook.xlsx', 
            sheet_name = 'use_for_parse_fails')

# Make sure column types are the same as the other observations
col_names = ['Q', 'T', 'D']
df[col_names] = df[col_names].astype(bool)


for col in ['zone_class', 'specific_plan', 'height_district']:
    df[col] = df[col].fillna('')
    df[col] = df[col].astype(str)


df.to_parquet(f's3://{bucket_name}/data/crosswalk_zone_parse_fails.parquet')


# Import other sheets in the codebook and upload to S3
for name in ['zone_class', 'supplemental_use_overlay', 'specific_plan']:
    df = pd.read_excel(f's3://{bucket_name}/references/Zoning_Parser_Codebook.xlsx', 
                sheet_name = f'{name}')
    df.to_parquet(f's3://{bucket_name}/data/crosswalk_{name}.parquet')
