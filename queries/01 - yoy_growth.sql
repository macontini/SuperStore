/*
1. YoY Revenue growth per Category  
Revenue per anno e categoria, variazione percentuale anno su anno.

---

| Colonna        | Descrizione |
|---             | ---|
| `year`         | Anno dell'ordine |
| `category`     | Categoria prodotto |
| `revenue`      | Revenue totale dell'anno |
| `prev_revenue` | Revenue anno precedente |
| `yoy_pct`      | Variazione % su anno precedente |
*/

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
