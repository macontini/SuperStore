import sqlite3
import argparse
from colorama import Fore, Style

from modules.const import DB_FILE_PATH, DUCKDB_FILE_PATH
from modules.func import connect_to_db, connect_to_duckdb, enum_tables
from modules.types import TypeCheck

from source import (
    factsales_init,
    bulk_update_from_csv,
    migration
)

RED = Fore.RED
BLU = Fore.BLUE
GREEN = Fore.GREEN
YELLOW = Fore.YELLOW
RESET = Style.RESET_ALL

db_exists = DB_FILE_PATH.exists()

"""
Gli argomenti years=5 (default) e slowly_changing_customers=300 (default)
andranno passati come argomento all'interprete direttamente
es. python main.py -y 5 --scd2 300
"""

def get_args() -> argparse.Namespace:
    def check_arg(val):
        try:
            val = int(val)
            if val <= 0:
                raise argparse.ArgumentTypeError(f"{val} non è un numero intero positivo.")
            return val
        except ValueError:
            raise argparse.ArgumentTypeError(f"{val} non è trasformabile in intero.")

    parser = argparse.ArgumentParser(description="Semplice pipeline di manipolazione dati")

    parser.add_argument(
        '-y', '--years',
        type=check_arg,
        default=5,
        help="Anni > 0 (default: 5)",
    )
    parser.add_argument(
        '-s', '--scd2',
        type=check_arg,
        default=300,
        help="Clienti che modificano Segment nel tempo (default: 300)."
    )

    return parser.parse_args()

def bootstrap_database(conn: sqlite3.Connection, years: int, changing_cust: int):
    from source import (
        random_yearly_data,
        random_scd2_data,
        tbls_init,
        index_init
    )

    # --------------------
    # RANDOM DATA GENERATION BY YEAR
    # --------------------
    random_yearly_data.run(years=years)

    # --------------------
    # RANDOM DATA GENERATION (SCD TYPE2)
    # --------------------
    random_scd2_data.run(slowly_changing_customers=changing_cust)
    print('='*100)

    print(f"\n>>> {YELLOW}Creazione tabelle...{RESET}")
    tbls_init.run(conn)
    print('='*100)
    
    print(f"\n>>> {YELLOW}Creazione indici...{RESET}")
    index_init.run(conn)
    print('='*100)

# --------------------
# STAR SCHEMA INIT
# --------------------
if not db_exists:

    # Ottenimento anni e clienti cangianti da riga di comando
    args = get_args()
    print(f"{GREEN}Argomenti passati:{RESET}")
    print(f"{YELLOW}--years {GREEN}{args.years}{RESET}")
    print(f"{YELLOW}--scd2 {GREEN}{args.scd2}{RESET}")

    # Inizializzazione Star Schema - popolazione tabelle dimensioni
    with connect_to_db(DB_FILE_PATH) as conn:
        bootstrap_database(conn, years=args.years, changing_cust=args.scd2)            

        # Popolazione tabella fatti
        print(f"\n>>> {YELLOW}Popolazione tabella FactSales...{RESET}")
        try:
            factsales_init.run(conn)
        except sqlite3.IntegrityError as e:
            print(f"{RED}{e.sqlite_errorname} - {'\n'.join(e.args)} - Tabella già alimentata.\n{RESET}")
        except sqlite3.OperationalError as e:
            print(f"{RED}{e.sqlite_errorname} - {e}.\n{RESET}")

    print('-'*100)

# --------------------
# SHOW TABLES
# --------------------
print(f"{GREEN}Stampa delle tabelle esistenti...{RESET}")
with connect_to_db(DB_FILE_PATH) as conn:
    tables = enum_tables(conn)
    print(f"{GREEN}Tabelle nel database:{RESET}")
    print(*tables, sep='\n', end='\n\n')
print('-'*100)

# --------------------
# BULK INSERT FROM CSV
# --------------------
with connect_to_db(DB_FILE_PATH) as conn:
    print(f"\n>>> {YELLOW}Bulk update usando dati random generati da `train.csv`...{RESET}")
    try:
        bulk_update_from_csv.run(conn, scd_type2=False)
    except sqlite3.IntegrityError as e:
        print(f"{RED}{e.sqlite_errorname} - {'\n'.join(e.args)}.\n{RESET}")
    except sqlite3.OperationalError as e:
        print(f"{RED}{e.sqlite_errorname} - {e}.\n{RESET}")
print('-'*100)

# --------------------
# BULK INSERT FROM CSV (SCD TYPE 2)
# --------------------
with connect_to_db(DB_FILE_PATH) as conn:
    print(f"\n>>> {YELLOW}Inserimento dati per test SCD Type 2 su DimCustomers...{RESET}")
    try:
        bulk_update_from_csv.run(conn, scd_type2=True)
    except sqlite3.IntegrityError as e:
        print(f"{RED}{e.sqlite_errorname} - {'\n'.join(e.args)}.\n{RESET}")
    except sqlite3.OperationalError as e:
        print(f"{RED}{e.sqlite_errorname} - {e}.\n{RESET}")
print('-'*100)

# --------------------
# SQLite -> DuckDB
# --------------------
with connect_to_db(DB_FILE_PATH) as conn1, connect_to_duckdb(DUCKDB_FILE_PATH) as conn2:
    print(f"{GREEN}Migrazione da SQLite3 a DuckDB...{RESET}")
    migration.run(conn1, conn2)
print('-'*100)

