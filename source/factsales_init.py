import sqlite3

def run(conn: sqlite3.Connection) -> None:
    # Popolazione tabella dei fatti
    cursor = conn.cursor()
    query = """INSERT INTO FactSales
        SELECT
            m.Row_ID,
            m.OrderID,
            m.OrderDate,
            m.ShipDate,
            m.ShipMode,
            dp.ProductKey,
            dc.CustomerKey,
            dg.GeoKey,
            m.Sales
        FROM Master m
        LEFT JOIN DimProducts dp 
            ON dp.ProductID = m.ProductID 
            AND dp.ProductName = m.ProductName
            AND dp.Category = m.Category
            AND dp.SubCategory = m.SubCategory
        LEFT JOIN DimCustomers dc 
            ON dc.CustomerID = m.CustomerID
            AND dc.CustomerName = m.CustomerName
            AND dc.IsCurrent = 1
        LEFT JOIN DimGeography dg 
            ON dg.PostalCode = m.PostalCode
            AND dg.City = m.City;
    """
    cursor.execute(query)
    conn.commit()

    rows_n = cursor.execute("SELECT COUNT(*) FROM FactSales").fetchone()[0]
    print(f"Tabella `FactSales`: inserite {rows_n} righe dalla tabella `Master`")
