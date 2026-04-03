from dataclasses import dataclass
from sqlite3 import Connection
from typing import Any, Iterable, Optional

# Tipizzazione di chiave esterna
@dataclass
class ForeignKey:
    tbl_referenced: str
    col_referenced: str

# Tipizzazione colonna della tabella
@dataclass
class Column:
    name: str
    type_: str
    constraint: str = ""
    foreign: Optional[ForeignKey] = None

    def to_sql(self) -> str:
        # Es.
        # "Row_ID INTEGER PRIMARY KEY AUTOINCREMENT"
        return f"{self.name} {self.type_} {self.constraint}".strip()

    def to_foreign(self) -> str:
        if self.foreign is None:
            return ""
        tbl_ref = self.foreign.tbl_referenced
        col_ref = self.foreign.col_referenced
        return f"FOREIGN KEY ({self.name}) REFERENCES {tbl_ref}({col_ref})"

# Tipizzazione degli indici
@dataclass
class Index:
    on_table: str
    on_columns: str | Iterable[str]
    unique: bool = False

    def to_sql(self) -> str:
        cols = self.on_columns
        if isinstance(cols, str):
            return f"idx_{self.on_table}_{cols}"
        return f"idx_{self.on_table}_{'_'.join(self.on_columns)}"

# Datatypes checker
class TypeCheck:
    def __init__(self, conn: Connection, tbl_name: str) -> None:
        self.connection = conn
        self.tbl_name = tbl_name

    def error_row(self, col: str, val: Any) -> Optional[int]:
        cursor = self.connection.cursor()
        query = f"SELECT _rowid_ FROM {self.tbl_name} WHERE {col} = ?"
        cursor.execute(query, (val,))
        return cursor.fetchone()[0]

    def check_dates_format(self, date_col_name: str, dt_format: str, limit: Optional[int] = None) -> bool:
        from datetime import datetime
        cursor = self.connection.cursor()
        query = "SELECT {0} FROM {1} ".format(date_col_name, self.tbl_name) + ("LIMIT {0}".format(limit) if limit else "")
        cursor.execute(query.strip())
        rows = cursor.fetchall()

        i = 0
        for row in rows:
            date_str = row[0]
            try:
                _ = datetime.strptime(date_str, dt_format)
                i += 1
            except ValueError:
                row = self.error_row(date_col_name, date_str)
                print(f"Column `{date_col_name}` - ERRORE Riga {row}:", date_str, 'non è nel formato', dt_format)
                raise
        print(f"`{date_col_name}` column check (format {dt_format}): scanned {i} rows")
        return True

    def check_number_format(self, num_col_name: str, numtype: type, limit: Optional[int] = None) -> bool:
        cursor = self.connection.cursor()
        query = "SELECT {0} FROM {1} ".format(num_col_name, self.tbl_name) + ("LIMIT {0}".format(limit) if limit else "")
        cursor.execute(query.strip())
        rows = cursor.fetchall()

        i = 0
        for row in rows:
            num = row[0]
            try:
                _ = numtype(num)
                i += 1
            except ValueError:
                row = self.error_row(num_col_name, num)
                print(f"Column `{num_col_name}` - ERRORE Riga {row}:", num, 'non è convertibile in', numtype.__name__)
                raise
        print(f"`{num_col_name}` column check (type {numtype.__name__}): scanned {i} rows.")
        return True

    def check_null_values(self, col_name: str) -> bool:
        cursor = self.connection.cursor()
        query = "SELECT {0} FROM {1} WHERE {0} IS NULL OR {0}=''".format(col_name, self.tbl_name)
        cursor.execute(query.strip())
        rows = cursor.fetchall()

        if rows:
            print(f"Column `{col_name}`: {len(rows)} NULL values.")
            return False

        print(f"Column `{col_name}`: no NULL values found")
        return True
