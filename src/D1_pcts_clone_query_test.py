import os
import pandas
import traceback
import pyodbc
from cryptography import fernet

key = os.environ.get('key_gis')
cipher = fernet.Fernet(key)

server = cipher.decrypt(os.environ.get('pcts_clone').encode()).decode()
uid = cipher.decrypt(os.environ.get('pcts_read_u').encode()).decode()
pwd = cipher.decrypt(os.environ.get('pcts_read_p').encode()).decode()

conn_string = f'DRIVER={{SQL Server}};SERVER={server};' \
            f'DATABASE=PCTS;UID={uid};PWD={pwd}'
conn = pyodbc.connect(conn_string)

tables_query = "SELECT * FROM INFORMATION_SCHEMA.TABLES"
tables = pandas.read_sql(tables_query, conn)


def flag_rows_exist(row):
    query = f"SELECT TOP 10 * FROM {row.TABLE_SCHEMA + '.' + row.TABLE_NAME}"
    print(f'Querying {row.TABLE_NAME}')
    try:
        rows = len(pandas.read_sql(query, conn).index)
        if rows > 0:
            return 'exists'
        else:
            return 'empty'
    except:
        return traceback.format_exc()

tables.loc[:,'rows_flag'] = tables.apply(lambda row: flag_rows_exist(row), axis=1)
tables.to_csv('pcts_clone_query_test.csv')