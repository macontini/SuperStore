/*
4. Shipping performance  
Calcolo delay effettivo (ShipDate − OrderDate) per ogni ShipMode,
confronto con il delay atteso (codificato in `modules.randomizer.Randomizer`, vd sotto),
e isolamento ordini anomali oltre la soglia.

'Same Day':       range(0, 2)
'Standard Class': range(4, 8)
'First Class':    range(1, 4)
'Second Class':   range(2, 6)

---

| Colonna          | Descrizione |
|---               | ---|
| `ship_mode`      | Modalità di spedizione |
| `orders`         | Numero ordini |
| `avg_delay_days` | Delay medio effettivo in giorni (ShipDate − OrderDate) |
| `min_delay_days` | Delay minimo |
| `max_delay_days` | Delay massimo |
| `expected_min`   | Delay minimo atteso per quel ShipMode |
| `expected_max`   | Delay massimo atteso per quel ShipMode |
| `anomalies`      | Ordini fuori dalla finestra attesa |
| `anomaly_pct`    | Percentuale di ordini anomali |
*/

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
