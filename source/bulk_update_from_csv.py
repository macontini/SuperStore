import sqlite3
from time import sleep

from modules.func import bulk_update_from_csv_into_tbl
from modules.const import CSV_DIR, SCD2_DATA_CSV_PATH
from source.tbls_init import dim_layer


def run(conn: sqlite3.Connection, scd_type2: bool = False) -> None:

    if not scd_type2:
        files = sorted(filter(lambda f: 'train' not in f.stem, CSV_DIR.glob('*.csv')))
    else:
        files = [SCD2_DATA_CSV_PATH]

    print(f"UPSERT using data from:", *[f.name for f in files], sep='\n', end='\n\n')

    for csv_fpath in files:
        bulk_update_from_csv_into_tbl(conn, csv_fpath, "StrictCopy")

    cursor = conn.cursor()

    # UPSERT (Master)
    print("\nAggiornamento (UPSERT) tabella `Master`")

    query = """
    INSERT OR REPLACE INTO Master (
        Row_ID, OrderID, OrderDate, ShipDate, ShipMode,
        CustomerID, CustomerName, Segment,
        Country, City, State, PostalCode, Region,
        ProductID, Category, SubCategory, ProductName, Sales
    )
    SELECT
        Row_ID,
        OrderID,
        SUBSTR(OrderDate, 7, 4) || '-' || SUBSTR(OrderDate, 4, 2) || '-' || SUBSTR(OrderDate, 1, 2),
        SUBSTR(ShipDate, 7, 4) || '-' || SUBSTR(ShipDate, 4, 2) || '-' || SUBSTR(ShipDate, 1, 2),
        ShipMode,
        CustomerID,
        CustomerName,
        Segment,
        Country,
        City,
        State,
        PostalCode,
        Region,
        ProductID,
        Category,
        SubCategory,
        ProductName,
        CAST(Sales AS REAL)
    FROM StrictCopy;
    """

    cursor.execute(query)
    conn.commit()

    for tbl_name, cols in dim_layer.items():

        print(f"Aggiornamento (UPSERT) tabella `{tbl_name}`...")

        columns = cols.copy()
        columns.pop(0)  # rimuove surrogate key

        # DIM PRODUCTS / GEOGRAPHY
        if tbl_name in ("DimProducts", "DimGeography"):

            unique_constr_cols = {
                "DimProducts": [c.name for c in columns if "UNIQUE" in c.constraint],
                "DimGeography": ["PostalCode", "City"],
            }

            query = f"""
                INSERT INTO {tbl_name} ({', '.join(c.name for c in columns)})
                SELECT DISTINCT
                    {', '.join(f"Master.{c.name}" for c in columns)}
                FROM Master
                LEFT JOIN {tbl_name}
                    ON {' AND '.join(f"{tbl_name}.{col} = Master.{col}" for col in unique_constr_cols[tbl_name])}
                WHERE {' AND '.join(f"{tbl_name}.{col} IS NULL" for col in unique_constr_cols[tbl_name])};
            """

            cursor.execute(query)

        # DIM CUSTOMERS
        if tbl_name == "DimCustomers":

            # INITIAL LOAD (NO SCD2)
            if not scd_type2:

                query = """
                    INSERT INTO DimCustomers (
                        CustomerID,
                        CustomerName,
                        Segment,
                        StartDate,
                        EndDate,
                        IsCurrent
                    )
                    SELECT DISTINCT
                        CustomerID,
                        CustomerName,
                        Segment,
                        STRFTIME('%Y-%m-%d %H:%M:%f','now'),
                        NULL,
                        1
                    FROM Master m
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM DimCustomers d
                        WHERE d.CustomerID = m.CustomerID
                    );
                """

                cursor.execute(query)

            # SCD TYPE 2
            else:
                #per non ottenere duplicazioni nelle colonne datetime
                sleep(1)

                cursor.executescript("""

                DROP VIEW IF EXISTS BatchCustomers;

                CREATE TEMP VIEW BatchCustomers AS
                SELECT
                    CustomerID,
                    CustomerName,
                    Segment
                FROM StrictCopy
                GROUP BY CustomerID;

                -- Chiude versioni correnti se cambiate
                UPDATE DimCustomers
                SET 
                    EndDate = STRFTIME('%Y-%m-%d %H:%M:%f','now'),
                    IsCurrent = 0
                FROM BatchCustomers b
                WHERE b.CustomerID = DimCustomers.CustomerID
                    AND DimCustomers.IsCurrent = 1
                    AND (
                        b.CustomerName <> DimCustomers.CustomerName
                        OR b.Segment <> DimCustomers.Segment
                    );

                -- Inserisce solo una nuova versione per CustomerID
                INSERT INTO DimCustomers (
                    CustomerID,
                    CustomerName,
                    Segment,
                    StartDate,
                    EndDate,
                    IsCurrent
                )
                SELECT
                    b.CustomerID,
                    b.CustomerName,
                    b.Segment,
                    STRFTIME('%Y-%m-%d %H:%M:%f','now'),
                    NULL,
                    1
                FROM BatchCustomers b
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM DimCustomers d
                    WHERE d.CustomerID = b.CustomerID
                        AND d.IsCurrent = 1
                );

                DROP VIEW BatchCustomers;

                """)
