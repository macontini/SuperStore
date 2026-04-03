import sqlite3

from modules.const import TRAIN_CSV_PATH
from modules.types import Column, ForeignKey
from modules.func import (
    create_table_with_constraints,
    bulk_insert_from_csv_into_tbl
)

# Dizionario esterno alla funzione `run` così può essere esportato
dim_layer: dict[str, list[Column]] = {
    # DimProducts
    "DimProducts": [
        Column("ProductKey", "INTEGER", "PRIMARY KEY AUTOINCREMENT"),
        Column("ProductID", "TEXT", "UNIQUE"),
        Column("ProductName", "TEXT"),
        Column("Category", "TEXT"),
        Column("SubCategory", "TEXT")
    ],
    # DimCustomers
    "DimCustomers": [
        Column("CustomerKey", "INTEGER", "PRIMARY KEY AUTOINCREMENT"),
        Column("CustomerID", "TEXT", "NOT NULL"),
        Column("CustomerName", "TEXT"),
        Column("Segment", "TEXT")
    ],
    # DimGeography
    "DimGeography": [
        Column("GeoKey", "INTEGER", "PRIMARY KEY AUTOINCREMENT"),
        Column("PostalCode", "TEXT"),
        Column("City", "TEXT"),
        Column("State", "TEXT"),
        Column("Country", "TEXT"),
        Column("Region", "TEXT"),
    ]
}
dim_layer['DimCustomers'].extend([ # SCD Type 2
    Column("StartDate", "TEXT", "NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    Column("EndDate", "TEXT", "DEFAULT NULL"),
    Column("IsCurrent", "INTEGER", "DEFAULT 1")
])


def run(conn: sqlite3.Connection) -> None:

    bulk_layer = {
        # Creazione tabella StrictCopy
        "StrictCopy": [
            Column(name='Row_ID', type_='NVARCHAR(255)', constraint='UNIQUE NOT NULL'),
            Column(name='OrderID', type_='NVARCHAR(255)', constraint=''),
            Column(name='OrderDate', type_='NVARCHAR(255)', constraint=''),
            Column(name='ShipDate', type_='NVARCHAR(255)', constraint=''),
            Column(name='ShipMode', type_='NVARCHAR(255)', constraint=''),
            Column(name='CustomerID', type_='NVARCHAR(255)', constraint=''),
            Column(name='CustomerName', type_='NVARCHAR(255)', constraint=''),
            Column(name='Segment', type_='NVARCHAR(255)', constraint=''),
            Column(name='Country', type_='NVARCHAR(255)', constraint=''),
            Column(name='City', type_='NVARCHAR(255)', constraint=''),
            Column(name='State', type_='NVARCHAR(255)', constraint=''),
            Column(name='PostalCode', type_='NVARCHAR(255)', constraint=''),
            Column(name='Region', type_='NVARCHAR(255)', constraint=''),
            Column(name='ProductID', type_='NVARCHAR(255)', constraint=''),
            Column(name='Category', type_='NVARCHAR(255)', constraint=''),
            Column(name='SubCategory', type_='NVARCHAR(255)', constraint=''),
            Column(name='ProductName', type_='NVARCHAR(255)', constraint=''),
            Column(name='Sales', type_='NVARCHAR(255)', constraint='')
            ],
        # Creazione tabella Master
        "Master": [
            Column(name='Row_ID', type_='INTEGER', constraint='PRIMARY KEY'),
            Column(name='OrderID', type_='TEXT', constraint='NOT NULL'),
            Column(name='OrderDate', type_='TEXT', constraint='NOT NULL'),
            Column(name='ShipDate', type_='TEXT', constraint=''),
            Column(name='ShipMode', type_='TEXT', constraint='NOT NULL'),
            Column(name='CustomerID', type_='TEXT', constraint='NOT NULL'),
            Column(name='CustomerName', type_='TEXT', constraint='NOT NULL'),
            Column(name='Segment', type_='TEXT', constraint='NOT NULL'),
            Column(name='Country', type_='TEXT', constraint='NOT NULL'),
            Column(name='City', type_='TEXT', constraint='NOT NULL'),
            Column(name='State', type_='TEXT', constraint='NOT NULL'),
            Column(name='PostalCode', type_='TEXT', constraint=''),
            Column(name='Region', type_='TEXT', constraint='NOT NULL'),
            Column(name='ProductID', type_='TEXT', constraint='NOT NULL'),
            Column(name='Category', type_='TEXT', constraint='NOT NULL'),
            Column(name='SubCategory', type_='TEXT', constraint=''),
            Column(name='ProductName', type_='TEXT', constraint='NOT NULL'),
            Column(name='Sales', type_='REAL', constraint='NOT NULL')
            ]
    }

    with conn:

        # Creazione tabelle di partenza e inserimento dati bulk in StrictCopy
        for tbl, cols in bulk_layer.items():
            create_table_with_constraints(conn, tbl, cols)

        bulk_insert_from_csv_into_tbl(conn, TRAIN_CSV_PATH, "StrictCopy")

        # Popolazione tabella Master da StrictCopy
        query = """
            INSERT INTO Master ({0})
                SELECT
                    Row_ID,
                    OrderID,
                    SUBSTR(OrderDate, 7, 4) || '-' || SUBSTR(OrderDate, 4, 2) || '-' || SUBSTR(OrderDate, 1, 2) AS OrderDate,
                    SUBSTR(ShipDate, 7, 4) || '-' || SUBSTR(ShipDate, 4, 2) || '-' || SUBSTR(ShipDate, 1, 2) AS ShipDate,
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
                    CAST(Sales AS REAL) AS Sales
                FROM StrictCopy
                WHERE 1=1 --- Fondamentale altrimenti OperationalError (syntax)
            ON CONFLICT (Row_ID) DO NOTHING;
        """.format(',\n'.join(map(lambda col: col.name, bulk_layer['Master'])))
        cursor = conn.cursor()
        cursor.execute(query)
    # ----------------------------------------

    with conn:
        # Creazione e popolazione tabelle Dim
        for tbl, cols in dim_layer.items():
            create_table_with_constraints(conn, tbl, cols)

            cols = [c.name for c in cols if "DEFAULT" not in c.constraint]

            # La prima colonna va tolta perché non è in Master
            cols.pop(0)

            query = """
                INSERT INTO {0} ({1})
                    SELECT DISTINCT {1}
                    FROM Master
                    WHERE 1=1
                ON CONFLICT DO NOTHING;
            """.format(
                tbl,
                ', '.join(cols)
            )
            cursor.execute(query)
    # ----------------------------------------

    # Tabella dei fatti
    fact_layer = [
        Column("Row_ID", "INTEGER", "PRIMARY KEY"),
        Column("OrderID", "TEXT"),
        Column("OrderDate", "TEXT"),
        Column("ShipDate", "TEXT"),
        Column("ShipMode", "TEXT"),
        Column("ProductKey", "INTEGER", foreign=ForeignKey("DimProducts", "ProductKey")),
        Column("CustomerKey", "INTEGER", foreign=ForeignKey("DimCustomers", "CustomerKey")),
        Column("GeoKey", "INTEGER", foreign=ForeignKey("DimGeography", "GeoKey")),
        Column("Sales", "REAL"),
    ]

    with conn:
        # Creazione tabella dei fatti
        create_table_with_constraints(conn, "FactSales", fact_layer)
    # ----------------------------------------
