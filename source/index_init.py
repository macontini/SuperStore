import sqlite3

from modules.func import create_index
from modules.types import Index

"""
Prestare attenzione al collocamento di questo script nella pipeline.
Dopo la creazione di indici, infatti, l'INSERT è più lento, e su DimCustomer
avverrà proprio un INSERT ai fini di test SCD Type 2
"""

def run(conn: sqlite3.Connection) -> None:

    indexes: list[Index] = [

        # DimProducts
        Index(on_table="DimProducts", on_columns="ProductID", unique=True),

        # DimCustomers
        Index(on_table="DimCustomers", on_columns="CustomerID", unique=False), # Non può essere unique altrimenti niente SCD2
        Index(on_table="DimCustomers", on_columns="CustomerName"),

        # SCD Type 2
        Index(on_table="DimCustomers", on_columns=["CustomerID", "StartDate"], unique=True),

        # DimGeography
        Index(on_table="DimGeography", on_columns=["PostalCode", "City", "State", "Country"], unique=True),

        # FactSales
        Index(on_table="FactSales", on_columns="OrderDate"),
        Index(on_table="FactSales", on_columns="ProductKey"),
        Index(on_table="FactSales", on_columns="CustomerKey"),
        Index(on_table="FactSales", on_columns="GeoKey")

    ]

    with conn:
        for idx in indexes:
            print(f"Creazione indice: {idx.to_sql()}")
            create_index(conn, idx)
    # ----------------------------------------
