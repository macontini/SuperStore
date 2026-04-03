/*
3. ABC analysis sui prodotti  
Ordinamento dei prodotti per revenue decrescente,
calcolo del revenue cumulato,
classifica in A (primo 80%), B (80–95%), C (coda).

---

| Colonna          | Descrizione |
|---               | ---|
| `product_id`     | Identificativo prodotto |
| `product_name`   | Nome prodotto |
| `category`       | Categoria |
| `subcategory`    | Sottocategoria |
| `revenue`        | Revenue totale del prodotto |
| `revenue_pct`    | Percentuale sul revenue totale |
| `cumulative_pct` | Percentuale cumulata (ordinata per revenue desc) |
| `class`          | Classificazione: `A` (0–80%), `B` (80–95%), `C` (95–100%) |
*/

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
