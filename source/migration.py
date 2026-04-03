import sqlite3
import duckdb
import pandas as pd

from modules.func import enum_tables

def run(sqlite_connection: sqlite3.Connection, duckdb_connection: duckdb.DuckDBPyConnection) -> None:

    # Tabelle disponibili in sqlite
    sqlite_tables = enum_tables(sqlite_connection)

    for table in sqlite_tables:
        # Lettura in memoria
        df = pd.read_sql(f"SELECT * FROM {table}", sqlite_connection)
        # Migrazione a duckdb
        duckdb_connection.sql(f"""
            CREATE OR REPLACE TABLE {table} AS
            SELECT * FROM df
        """)

    # Correzione tipi di dato
    duckdb_connection.sql("""
        ALTER TABLE DimCustomers ALTER StartDate TYPE TIMESTAMP;
        ALTER TABLE DimCustomers ALTER EndDate TYPE TIMESTAMP;
        ALTER TABLE DimCustomers ALTER IsCurrent TYPE BOOLEAN;
    """)
    duckdb_connection.sql("""
        ALTER TABLE DimGeography ALTER PostalCode SET DATA TYPE BIGINT
        USING TRY_CAST(NULLIF(NULLIF(PostalCode, 'nan'), '') AS BIGINT);
    """)
