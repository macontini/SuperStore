import csv
import re
import sqlite3
import duckdb
from pathlib import Path
from typing import Iterable

from .types import Column, Index

def connect_to_db(db_path: Path) -> sqlite3.Connection:
    """Crea una connessione a SQLite3 e attiva i vincoli di integrità."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def connect_to_duckdb(duckdb_path: Path) -> duckdb.DuckDBPyConnection:
    """Crea una connessione a DuckDB."""
    return duckdb.connect(duckdb_path)

def enum_tables(conn: sqlite3.Connection) -> list[str]:
    cursor = conn.cursor()
    query = """
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        ORDER BY name;
    """
    cursor.execute(query)
    return [name[0] for name in cursor.fetchall() if "sqlite" not in name[0]]

def create_table_with_constraints(conn: sqlite3.Connection, tbl_name: str, columns: Iterable[Column]) -> None:
    cursor = conn.cursor()
    col_defs = ',\n\t'.join(col.to_sql() for col in columns)
    foreign_defs = [col.to_foreign() for col in columns if col.foreign is not None]
    query = """
        CREATE TABLE IF NOT EXISTS {0} (\n\t{1}\n{2}\n);
        """.format(
            tbl_name,
            col_defs,
            (',\n\t' + ',\n\t'.join(foreign_defs)) if foreign_defs else ""
        )
    print(f"Creazione tabella `{tbl_name}`.")
    cursor.execute(query)
    conn.commit()

def bulk_insert_from_csv_into_tbl(conn: sqlite3.Connection, csv_fpath: Path, tbl_name: str = "StrictCopy") -> None:
    with open(csv_fpath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        fieldnames = next(reader)

        # RowID è keyword riservata quindi "Row ID" diventa "Row_ID"
        fieldnames = [re.sub(r"[\s\-]", "" if col != "Row ID" else "_", col) for col in fieldnames]
        query = "INSERT INTO {0} ({1}) VALUES ({2})".format(
                tbl_name,
                ',\n\t'.join(fieldnames),
                ', '.join(['?' for _ in range(len(fieldnames))])
            )

        conn.execute("BEGIN")
        try:
            cursor = conn.cursor()
            print(f"Alimentazione tabella `{tbl_name}` con i dati di {csv_fpath}...")
            cursor.executemany(query, reader)
        except Exception as e:
            conn.rollback()
            print("Errore durante l'inserimento dati:", e, end='\n\n')
        else:
            conn.commit()
            print()

def create_index(conn: sqlite3.Connection, idx: Index) -> None:
    columns = idx.on_columns if isinstance(idx.on_columns, str) else ', '.join(idx.on_columns)
    query = f"""
        CREATE {"UNIQUE" if idx.unique else ""} INDEX IF NOT EXISTS {idx.to_sql()}
        ON {idx.on_table} ({columns});
    """
    cursor = conn.cursor()
    cursor.execute(query)

def bulk_update_from_csv_into_tbl(conn: sqlite3.Connection, csv_fpath: Path, tbl_name: str = "StrictCopy") -> None:
    with open(csv_fpath, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        fieldnames = next(reader)

        # RowID è keyword riservata quindi "Row ID" -> "Row_ID"
        fieldnames = [re.sub(r"[\s\-]", "" if col != "Row ID" else "_", col) for col in fieldnames]
        query = """
            INSERT INTO {0} ({1})
            VALUES ({2})
        """.format(
                tbl_name,
                ', '.join(fieldnames),
                ', '.join(['?' for _ in range(len(fieldnames))])
            )

        cols_to_update = fieldnames[1:]
        query += """
            ON CONFLICT(Row_ID)
            DO UPDATE SET {0}
        """.format(
            ',\n\t'.join(f"{x} = excluded.{x}" for x in cols_to_update)
        )

        try:
            cursor = conn.cursor()
            cursor.executemany(query, reader)
        except Exception as e:
            print(f"{e.__class__.__name__} - Errore durante l'inserimento dati:", e)
            if e.__class__ in (sqlite3.OperationalError, sqlite3.ProgrammingError):
                print(query.strip())
        else:
            conn.commit()
            print(f"Tabella `{tbl_name}` aggiornata con i dati di {csv_fpath}.")
