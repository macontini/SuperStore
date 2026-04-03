/*
2. Customer cohort retention  
Raggruppamento di clienti per anno del primo acquisto (cohort), e misura del revenue generato da quella cohort negli anni successivi. 

---

| Colonna                    | Descrizione |
|---                         | ---|
| `cohort_year`              | Anno del primo acquisto del cliente |
| `order_year`               | Anno in cui la cohort ha generato revenue |
| `periods_since_first`      | Distanza in anni tra `order_year` e `cohort_year` |
| `customers`                | Clienti distinti attivi in quell'anno |
| `revenue`                  | Revenue totale della cohort in quell'anno |
| `avg_revenue_per_customer` | Revenue medio per cliente attivo |
*/

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
