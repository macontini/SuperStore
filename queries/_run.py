from pathlib import Path
import duckdb
import pandas as pd

from modules.const import QUERIES_DIR

def exec_query(conn: duckdb.DuckDBPyConnection, sql_file: Path) -> pd.DataFrame:
    with open(sql_file, 'r', encoding='utf-8') as f:
        query = f.read().strip()
    return conn.sql(query).df()

def run(conn: duckdb.DuckDBPyConnection, directory: Path):
    files = sorted(directory.glob('*.sql'))
    for file in files:
        print(f"\n>>> {file.stem}")
        df = exec_query(conn, file)
        print(df.head().fillna('').to_markdown())
        input("Premere Enter per proseguire...\n")
