import marimo

__generated_with = "0.22.0"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # SuperStore
    """)
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import duckdb

    DATABASE_URL = r"C:\Users\macontini\SQL\SQLv2\superstore.duckdb"
    engine = duckdb.connect(DATABASE_URL, read_only=True)
    return (engine,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 01 - YoY Growth
    """)
    return


@app.cell
def _(dimproducts, engine, factsales, mo):
    _df = mo.sql(
        f"""
        -- 1) Revenue per anno e categoria, variazione percentuale anno su anno.

        -- year
        -- category
        -- revenue
        -- prev_revenue
        -- yoy_pct
        WITH grouped AS (
          SELECT
            EXTRACT(YEAR FROM f.OrderDate) AS Year,
            p.Category,
            ROUND(SUM(f.Sales), 2) AS Revenue
          FROM FactSales f
          JOIN DimProducts p
              ON p.ProductKey = f.ProductKey
          WHERE f.OrderDate IS NOT NULL
          GROUP BY Year,
              p.Category
        )
        SELECT
          *,
          LAG(Revenue) OVER (
              PARTITION BY Category
              ORDER BY Year
          ) AS Prev_Revenue,
          ROUND((Revenue - Prev_Revenue) * 100.0 / NULLIF(Prev_Revenue, 0), 2) AS yoy_pct
        FROM grouped
        ORDER BY Category, Year
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 02 - Cohort
    """)
    return


@app.cell
def _(engine, factsales, mo):
    _df = mo.sql(
        f"""
        -- 2) Raggruppamento di clienti per anno del primo acquisto (cohort),
        -- e misura del revenue generato da quella cohort negli anni successivi. 

        -- cohort_year
        -- order_year
        -- periods_since_first
        -- customers
        -- revenue
        -- avg_revenue_per_customer
        WITH cte AS (
            SELECT
              CustomerKey,
              MIN(EXTRACT(YEAR FROM OrderDate)) OVER (
                  PARTITION BY CustomerKey
              ) AS Cohort_Year,
              EXTRACT(YEAR FROM OrderDate) AS Order_Year,
              Sales
            FROM FactSales
        )
        SELECT
          Cohort_Year,
          Order_Year,
          Order_Year - Cohort_Year AS Periods_Since_First,
          COUNT(DISTINCT CustomerKey) AS Customers,
          ROUND(SUM(Sales), 2) AS Revenue,
          ROUND(Revenue * 1.0 / NULLIF(Customers, 0), 2) AS Avg_Revenue_per_Customer
        FROM cte
        GROUP BY Cohort_Year, Order_Year
        ORDER BY Cohort_Year, Order_Year
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 03 - ABC Analysis
    """)
    return


@app.cell
def _(dimproducts, engine, factsales, mo):
    _df = mo.sql(
        f"""
        -- 3) Ordinamento di prodotti per revenue decrescente,
        -- calcolo del revenue cumulato,
        -- classifica in A (primo 80%), B (80–95%), C (coda).

        -- product_id
        -- product_name
        -- category
        -- subcategory
        -- revenue
        -- revenue_pct
        -- cumulative_pct
        -- class
        WITH cte AS (
            SELECT
                p.ProductID,
                p.ProductName,
                p.Category,
                p.SubCategory,
                ROUND(SUM(f.Sales), 2) AS Revenue,
                ROUND(Revenue * 100.0 / NULLIF((SELECT SUM(Sales) FROM FactSales), 0), 6) AS revenue_pct
            FROM DimProducts p
            JOIN FactSales f
                ON f.ProductKey = p.ProductKey
            GROUP BY ALL
        ),
        ranked AS (
            SELECT
                *,
                ROUND(SUM(revenue_pct) OVER (
                    ORDER BY revenue_pct DESC
                ), 6) AS cumulative_pct
            FROM cte
        )
        SELECT
            *,
            CASE
                WHEN cumulative_pct <= 80 THEN 'A'
                WHEN cumulative_pct <= 95 THEN 'B'
                WHEN cumulative_pct <= 100 THEN 'C'
            END AS class
        FROM ranked
        ORDER BY Revenue DESC
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 04 - Shipping
    """)
    return


@app.cell
def _(engine, factsales, mo):
    _df = mo.sql(
        f"""
        -- 4) Calcolo delay effettivo (ShipDate − OrderDate) per ogni ShipMode,
        -- confronto con il delay atteso (codificato in `modules.randomizer.Randomizer`),
        -- e isolamento ordini anomali oltre la soglia.

        -- 'Same Day':       range(0, 2)
        -- 'Standard Class': range(4, 8)
        -- 'First Class':    range(1, 4)
        -- 'Second Class':   range(2, 6)

        -- ship_mode
        -- orders
        -- avg_delay_days
        -- min_delay_days
        -- max_delay_days
        -- expected_min
        -- expected_max
        -- anomalies
        -- anomaly_pct
        WITH ranges AS (
            SELECT
                ShipMode,
                CASE ShipMode
                    WHEN 'Same Day' THEN 0
                    WHEN 'Standard Class' THEN 4
                    WHEN 'First Class' THEN 1
                    WHEN 'Second Class' THEN 2
                END AS Expected_Min,
                CASE ShipMode
                    WHEN 'Same Day' THEN 1
                    WHEN 'Standard Class' THEN 7
                    WHEN 'First Class' THEN 3
                    WHEN 'Second Class' THEN 5
                END AS Expected_Max,
            FROM FactSales
            GROUP BY ShipMode
        )
        SELECT
            f.ShipMode,
            ROUND(AVG(ShipDate - OrderDate), 2) AS Avg_Delay_Days,
            MIN(ShipDate - OrderDate) AS Min_Delay_Days,
            MAX(ShipDate - OrderDate) AS Max_Delay_Days,
            ANY_VALUE(r.Expected_Min) AS Expected_Min,
            ANY_VALUE(r.Expected_Max) AS Expected_Max,
            SUM(CASE
                WHEN (ShipDate - OrderDate) BETWEEN r.Expected_Min AND r.Expected_Max THEN 0
                ELSE 1
            END) AS Anomalies,
            ROUND(Anomalies * 100.0 / NULLIF(COUNT(*), 0), 3) AS Anomaly_pct
        FROM FactSales f
        JOIN ranges r
            ON r.ShipMode = f.ShipMode
        GROUP BY f.ShipMode
        ORDER BY Anomaly_pct DESC
        """,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 05 - Geo
    """)
    return


@app.cell
def _(dimgeography, engine, factsales, mo):
    _df = mo.sql(
        f"""
        -- 5) Geographic market penetration  
        -- Revenue e numero ordini per Stato,
        -- con in aggiunta il revenue medio per ordine
        -- e il rank nazionale.
        -- Identificare Stati ad alto volume ma basso ticket medio (potenziale upselling) e viceversa.

        -- region               - Regione
        -- state                - Stato
        -- orders               - Numero ordini distinti
        -- revenue              - Revenue totale
        -- avg_order_value      - Revenue medio per ordine
        -- national_revenue_pct - Percentuale sul revenue nazionale
        -- revenue_rank         - Rank per revenue all'interno della regione
        -- segment              - high_volume_low_ticket/low_volume_high_ticket/other
        WITH cte AS (
            SELECT
                g.Region,
                g.State,
                COUNT(DISTINCT OrderID) AS Orders,
                ROUND(SUM(f.Sales), 2) AS Revenue,
                ROUND(AVG(f.Sales), 2) AS Avg_Order_Value
            FROM FactSales f
            JOIN DimGeography g
                ON g.GeoKey = f.GeoKey
            GROUP BY g.Region, g.State
        ),
        quantiles AS (
            SELECT
                QUANTILE_CONT(Orders, 0.5) AS median_orders,
                QUANTILE_CONT(Revenue, 0.5) AS median_revenue
            FROM cte
        )
        SELECT
            cte.*,
            ROUND(Revenue * 100.0 / (SELECT SUM(Sales) FROM FactSales), 2) AS National_Revenue_Pct,
            RANK() OVER (PARTITION BY Region ORDER BY Revenue DESC) AS Revenue_Rank,
            CASE
                WHEN Orders >= q.median_orders AND Revenue <= q.median_revenue THEN 'High Volume Low Ticket'
                WHEN Orders <= q.median_orders AND Revenue >= q.median_revenue THEN 'Low Volume High Ticket'
                ELSE 'Other'
            END AS Segment
        FROM cte, quantiles q
        ORDER BY Region, Revenue_Rank
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()
