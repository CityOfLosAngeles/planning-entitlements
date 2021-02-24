"""
Make a master PCTS file by using the PCTS clone in the style of A5_create_pcts_master.py.
""" 
import os

import intake
import pandas
import pyodbc
from cryptography import fernet

catalog = intake.open_catalog("../catalogs/*.yml")
bucket = 'city-planning-entitlements'

# Create connection to PCTS clone

key = os.environ.get('key_gis')
cipher = fernet.Fernet(key)

server = cipher.decrypt(os.environ.get('pcts_clone').encode()).decode()
uid = cipher.decrypt(os.environ.get('pcts_read_u').encode()).decode()
pwd = cipher.decrypt(os.environ.get('pcts_read_p').encode()).decode()

conn_string = f'DRIVER={{SQL Server}};SERVER={server};' \
            f'DATABASE=PCTS;UID={uid};PWD={pwd}'
conn = pyodbc.connect(conn_string)

# Here we use a query derived from one used by the PCTS reporting module.
# That query joins a number of tables which we don't have in our backup,
# or which we are not interested in. It also uses some Oracle-specific syntax,
# such as NVL, and the `(+)` directive for outer joins.
# We adapt by doing the following:
#    1. Only join application, location, and la_prop tables. We don't join with
#       GEO_INFO, as it seems to be missing important rows.
#    2. Replace the outer joins with the appeals table by one done with pandas,
#       since SQLite doesn't support outer joins.
#    3. Include some more location data, such as AIN, council district,
#       plan area number, etc. Notably, we don't include census tract, since we
#       derive census tract via our own means.
# A more detailed derivation of this query can be found in notebooks/pcts-case-query.ipynb.
sql="""
SELECT DISTINCT
        CC.CASE_ID as CASE_ID,
        CC.CASE_NBR AS CASE_NUMBER,
        COALESCE(CC.CASE_FILE_RCV_DT, CC.CRTN_DT) AS FILE_DATE,
        CC.APLC_ID AS APPLICATION_ID,
        CC.CASE_SEQ_NBR AS CASE_SEQUENCE_NUMBER,
        CC.CASE_YR_NBR AS CASE_YEAR_NUMBER,
        CC.PARNT_CASE_ID AS PARENT_CASE_ID,
        CC.CASE_ACTION_ID AS CASE_ACTION_ID,
        PP.STR_NBR + ' ' + PP.STR_DIR_CD + ' ' + PP.STR_NM AS ADDRESS,
        PP.CNCL_DIST_NBR AS COUNCIL_DISTRICT,
        PP.PIN AS PIN,
        PP.PLAN_AREA_NBR as PLAN_AREA,
        PP.BOE_DIST_MAP_NBR AS BOE_DISTRICT,
        PP.APC_AREA_CD AS APC_AREA,
        PP.ZONE_REG_CD as ZONING,
        PP.ASSR_PRCL_NBR as AIN,
        CC.EXPEDITED_CASE_FLG AS "EXPEDITED_CASE",
        CC.TRACT_CASE_FLG AS "INCIDENTAL_CASE",
        LC.PROJ_DESC_TXT AS "PROJECT_DESCRIPTION"
FROM cts.TCASE CC
        INNER JOIN cts.TAPLC LC ON CC.APLC_ID=LC.APLC_ID
        INNER JOIN cts.TLOC LL ON LC.APLC_ID=LL.APLC_ID
        INNER JOIN cts.TLA_PROP PP on LL.LOC_ID=PP.PROP_ID
"""

pcts = pandas.read_sql(sql, conn)

# The original query makes an outer join with the appeals table.
# THe previous backup uses sqlite, which doesn't support full outer joins.
# Instead, we read the table into pandas and make the outer join there.
# We also drop null CASE_NUMBERs after the join, effectively making it
# a left join, but we are endeavoring to keep close to the original query.
appeals_query = 'SELECT * FROM cts.TAPEL_CASE'
appeals = pandas.read_sql(appeals_query, conn)

pcts = pandas.merge(
    pcts,
    appeals[["CASE_ID", "BZA_PUBLC_HEAR_DT", "BZA_DECISN_DT"]].rename(
        columns={"BZA_PUBLC_HEAR_DT": "APPEAL_HEARING_DATE", "BZA_DECISN_DT": "APPEAL_DECISION_DATE"}
    ),
    on="CASE_ID",
    how="outer",
).dropna(subset=["CASE_NUMBER"])

# Merge in census tract data
parcel_to_tract = catalog.crosswalk_parcels_tracts.read()[["GEOID", "AIN"]]
pcts = pandas.merge(
    pcts,
    parcel_to_tract,
    on="AIN",
    how="inner",
    validate="m:1",
)
    
# Fix up some dtypes, sort, and reset the index
# in preparation for export to parquet.
pcts = pcts.astype({
    "CASE_ID": "Int64",
    "FILE_DATE": "datetime64[ns]",
    "APPLICATION_ID": "Int64",
    "CASE_SEQUENCE_NUMBER": "Int64",
    "CASE_YEAR_NUMBER": "Int64",
    "PARENT_CASE_ID": "Int64",
    "CASE_ACTION_ID": "Int64",
    "PLAN_AREA": "Int64",
}).sort_values(["FILE_DATE", "CASE_ID", "AIN"]).reset_index(drop=True)

# Upload to s3.
pcts.to_parquet(f"s3://{bucket}/data/final/pcts_clone.parquet")