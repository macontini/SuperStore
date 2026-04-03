from pathlib import Path

this_dir = Path(__file__).parent

CWD = this_dir.parent
DB_FILE_PATH = CWD / 'superstore.db'
DUCKDB_FILE_PATH = CWD / 'superstore.duckdb'

CSV_DIR = CWD / 'csv'
TRAIN_CSV_PATH = CSV_DIR / 'train.csv'
SCD2_DATA_CSV_PATH = CSV_DIR / 'train_SCD.csv'

QUERIES_DIR = CWD / 'queries'