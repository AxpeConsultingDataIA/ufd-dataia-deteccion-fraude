-- TARGET DEFINITION DIFFERENT DAYS ANALYSYS
-- This query calculates the number of fraud cases that start within different time frames (30, 60, 90, 120, and 150 days) 
-- after a specific reference date (2025-03-30).

SELECT
    SUM(CASE WHEN CAST(fecha_inicio_anomalia_formateada AS TIMESTAMP) <= CAST('2025-03-30' AS TIMESTAMP) + INTERVAL '30' DAY THEN 1 ELSE 0 END) AS fraudes_en_30_dias,
    SUM(CASE WHEN CAST(fecha_inicio_anomalia_formateada AS TIMESTAMP) <= CAST('2025-03-30' AS TIMESTAMP) + INTERVAL '60' DAY THEN 1 ELSE 0 END) AS fraudes_en_60_dias,
    SUM(CASE WHEN CAST(fecha_inicio_anomalia_formateada AS TIMESTAMP) <= CAST('2025-03-30' AS TIMESTAMP) + INTERVAL '90' DAY THEN 1 ELSE 0 END) AS fraudes_en_90_dias,
    SUM(CASE WHEN CAST(fecha_inicio_anomalia_formateada AS TIMESTAMP) <= CAST('2025-03-30' AS TIMESTAMP) + INTERVAL '120' DAY THEN 1 ELSE 0 END) AS fraudes_en_120_dias,
    SUM(CASE WHEN CAST(fecha_inicio_anomalia_formateada AS TIMESTAMP) <= CAST('2025-03-30' AS TIMESTAMP) + INTERVAL '150' DAY THEN 1 ELSE 0 END) AS fraudes_en_150_dias
FROM (
    SELECT
        *,
        -- Step 1: Format the date from DD/MM/YYYY to YYYY-MM-DD
        SUBSTR(fecha_inicio_anomalia, 7, 4) || '-' || 
        SUBSTR(fecha_inicio_anomalia, 4, 2) || '-' || 
        SUBSTR(fecha_inicio_anomalia, 1, 2) || ' ' ||
        SUBSTR(fecha_inicio_anomalia, 12, 8) as fecha_inicio_anomalia_formateada
    FROM
        master_irregularidades_fraudes.expedientes
    WHERE
        tipo_anomalia = 'FRAUDE'
)
WHERE
    CAST(fecha_inicio_anomalia_formateada AS TIMESTAMP) > CAST('2025-03-30' AS TIMESTAMP)
    AND CAST(fecha_inicio_anomalia_formateada AS TIMESTAMP) <= CAST('2025-03-30' AS TIMESTAMP) + INTERVAL '150' DAY;