"""
Load MS Access backup of PCTS database, save it to Postgres
as well as a SQLite database for easier analysis.

This requires the Microsoft Access ODBC driver to be installed:
https://www.microsoft.com/en-us/download/details.aspx?id=13255

It also requires pyodbc, sqlalchemy, and sqlalchemy_access
"""

import csv
import io
import os
import sys
import urllib

import sqlalchemy
import sqlalchemy_access


def ms_access_connect(fname):
    """
    Connect to the MS Access DB
    """
    conn_str = urllib.parse.quote_plus(
        "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={os.path.abspath(fname)};"
    )
    connection_url = f"access+pyodbc:///?odbc_connect={conn_str}"
    engine = sqlalchemy.create_engine(connection_url)
    return engine


def copy_column(c, supports_datetime=True):
    """
    Copy column structure from the source db to the destination.
    This also tweaks some types so that the Access DB flavor of
    database conforms with standard SQL.
    """
    if isinstance(c.type, sqlalchemy_access.base.LONGCHAR):
        return sqlalchemy.Column(c.name, sqlalchemy.String)
    elif isinstance(c.type, sqlalchemy_access.base.YESNO):
        return sqlalchemy.Column(c.name, sqlalchemy.Boolean)
    elif isinstance(c.type, sqlalchemy.DATETIME) and not supports_datetime:
        return sqlalchemy.Column(c.name, sqlalchemy.TIMESTAMP)
    else:
        return c.copy()


if __name__ == "__main__":
    """
    Load data from a PCTS backup and transfer it to another database.

    Usage: invoke from the command line with

        python load_pcts.py SOURCE_FILE DEST_DB SCHEMA

    where SOURCE_FILE is the path to the PCTS bakcup, DEST_DB is a sqlalchemy
    style connection string, and SCHEMA is an optional destination schema.

    To load to a SQLite database, run

        python load_pcts.py PCTS.accdb sqlite:///PCTS.sqlite

    To load to a PostgreSQL database with schema 'pcts', run

        python load_pcts.py PCTS.accdb $POSTGRES_URI pcts
    """
    if len(sys.argv) < 3:
        print("Please provide an MS Access backup and a destination DB")
        exit()

    # Connect to source and destination DB
    from_engine = ms_access_connect(sys.argv[1])
    to_engine = sqlalchemy.create_engine(sys.argv[2])
    supports_datetime = to_engine.dialect.name != "postgresql"
    schema = None
    if len(sys.argv) == 4:
        schema = sys.argv[3]

    # Get the table structure from the source
    from_meta = sqlalchemy.MetaData()
    from_meta.reflect(bind=from_engine)

    # Copy the table structure from the source DB to the destination.
    to_meta = sqlalchemy.MetaData(schema=schema)
    for table in from_meta.sorted_tables:
        sqlalchemy.Table(
            table.name,
            to_meta,
            *[copy_column(c, supports_datetime) for c in table.columns],
        )
    to_meta.create_all(bind=to_engine)

    # Copy the actual table data.
    for from_table, to_table in zip(from_meta.sorted_tables, to_meta.sorted_tables):
        print(f"Copying data from {from_table} to {to_table}")
        table_data = from_engine.execute(from_table.select()).fetchall()
        table_data = [dict(row) for row in table_data]

        # If using postgres, use the native COPY FROM functionality of
        # CSV loading. This is faster and more stable.
        if to_engine.dialect.name == "postgresql":
            # Prepare an in-memory CSV string of the data.
            # The standard library CSV writer actually does a better job than
            # the pandas CSV writer, as nullable integers are handled correctly.
            # Furthermore, we strip NULL characters from strings, as PostreSQL
            # chokes on those.
            fp = io.StringIO()
            writer = csv.DictWriter(fp, list(table_data[0].keys()))
            writer.writerows(
                [
                    {
                        k: v.replace("\x00", "") if isinstance(v, str) else v
                        for k, v in row.items()
                    }
                    for row in table_data
                ]
            )
            fp.seek(0)

            # Load the data using a raw psycopg cursor
            dest = f'"{schema}"."{to_table.name}"' if schema else f'"{to_table.name}"'
            cursor = to_engine.raw_connection().cursor()
            cursor.copy_expert(f"COPY {dest} FROM STDIN WITH (FORMAT CSV)", fp)

            # Clean up.
            cursor.connection.commit()
            cursor.close()

        else:  # Otherwise use a standard insert.
            to_engine.execute(to_table.insert(table_data))
