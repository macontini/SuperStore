/*
5. Geographic market penetration  
Revenue e numero ordini per Stato,
con in aggiunta il revenue medio per ordine
e il rank nazionale.
Identificare Stati ad alto volume ma basso ticket medio (potenziale upselling) e viceversa.

---

| Colonna                | Descrizione |
|---                     | ---|
| `region`               | Regione |
| `state`                | Stato |
| `orders`               | Numero ordini distinti |
| `revenue`              | Revenue totale |
| `avg_order_value`      | Revenue medio per ordine |
| `national_revenue_pct` | Percentuale sul revenue nazionale |
| `revenue_rank`         | Rank per revenue all'interno della regione |
| `segment`              | `high_volume_low_ticket` / `low_volume_high_ticket` / `other` |
*/

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
