"""

Attached is a script that takes a file running 
on the platform and sends it to civis database 
"""

import civis
import intake
import pandas as pd

import laplan
catalog = intake.open_catalog("../catalogs/catalog.yml")

# Use lightweight version of grabbing processed data
# utils.entitlements_per_tract() has a lot of joins and aggregation, unnecessary
def get_processed_data():
    pcts = catalog.pcts.read()
    pcts = laplan.pcts.subset_pcts(pcts, verbose=True)
    pcts = laplan.pcts.drop_child_cases(pcts, keep_child_entitlements=True)
    
    pcts.to_parquet("s3://city-planning-entitlements/test_df.parquet")
    
    return pcts

df = get_processed_data()

print(f"# obs: {len(df)}")
print(f"earliest date: {df.FILE_DATE.min()}")
print(f"most recent date: {df.FILE_DATE.max()}")
civis.io.dataframe_to_civis(df, 'City of Los Angeles - Postgres', 'scratch.test_planning_pcts_per_tract')     