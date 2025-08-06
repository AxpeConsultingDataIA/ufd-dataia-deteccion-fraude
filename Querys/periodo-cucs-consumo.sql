SELECT 
    cups_sgc,
    
    -- Esta semana
    SUM(CASE WHEN fh >= DATE_TRUNC('week', CURRENT_DATE) THEN ai END) AS esta_semana_suma,
    ROUND(AVG(CASE WHEN fh >= DATE_TRUNC('week', CURRENT_DATE) THEN ai END), 2) AS esta_semana_media,
    MAX(CASE WHEN fh >= DATE_TRUNC('week', CURRENT_DATE) THEN ai END) AS esta_semana_maximo,
    MIN(CASE WHEN fh >= DATE_TRUNC('week', CURRENT_DATE) THEN ai END) AS esta_semana_minimo,
    ROUND(STDDEV(CASE WHEN fh >= DATE_TRUNC('week', CURRENT_DATE) THEN ai END), 2) AS esta_semana_stddev,
    ROUND(VAR_SAMP(CASE WHEN fh >= DATE_TRUNC('week', CURRENT_DATE) THEN ai END), 2) AS esta_semana_varianza,
    
    -- Semana pasada
    SUM(CASE WHEN fh >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '7' DAY) 
                  AND fh < DATE_TRUNC('week', CURRENT_DATE) THEN ai END) AS semana_pasada_suma,
    ROUND(AVG(CASE WHEN fh >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '7' DAY) 
                        AND fh < DATE_TRUNC('week', CURRENT_DATE) THEN ai END), 2) AS semana_pasada_media,
    MAX(CASE WHEN fh >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '7' DAY) 
                  AND fh < DATE_TRUNC('week', CURRENT_DATE) THEN ai END) AS semana_pasada_maximo,
    MIN(CASE WHEN fh >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '7' DAY) 
                  AND fh < DATE_TRUNC('week', CURRENT_DATE) THEN ai END) AS semana_pasada_minimo,
    ROUND(STDDEV(CASE WHEN fh >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '7' DAY) 
                           AND fh < DATE_TRUNC('week', CURRENT_DATE) THEN ai END), 2) AS semana_pasada_stddev,
    ROUND(VAR_SAMP(CASE WHEN fh >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '7' DAY) 
                            AND fh < DATE_TRUNC('week', CURRENT_DATE) THEN ai END), 2) AS semana_pasada_varianza,
    
    -- Misma semana año pasado
    SUM(CASE WHEN WEEK(fh) = WEEK(CURRENT_DATE) 
                  AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END) AS misma_semana_anio_pasado_suma,
    ROUND(AVG(CASE WHEN WEEK(fh) = WEEK(CURRENT_DATE) 
                        AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END), 2) AS misma_semana_anio_pasado_media,
    MAX(CASE WHEN WEEK(fh) = WEEK(CURRENT_DATE) 
                  AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END) AS misma_semana_anio_pasado_maximo,
    MIN(CASE WHEN WEEK(fh) = WEEK(CURRENT_DATE) 
                  AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END) AS misma_semana_anio_pasado_minimo,
    ROUND(STDDEV(CASE WHEN WEEK(fh) = WEEK(CURRENT_DATE) 
                           AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END), 2) AS misma_semana_anio_pasado_stddev,
    ROUND(VAR_SAMP(CASE WHEN WEEK(fh) = WEEK(CURRENT_DATE) 
                            AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END), 2) AS misma_semana_anio_pasado_varianza,
    
    -- Este mes
    SUM(CASE WHEN fh >= DATE_TRUNC('month', CURRENT_DATE) THEN ai END) AS este_mes_suma,
    ROUND(AVG(CASE WHEN fh >= DATE_TRUNC('month', CURRENT_DATE) THEN ai END), 2) AS este_mes_media,
    MAX(CASE WHEN fh >= DATE_TRUNC('month', CURRENT_DATE) THEN ai END) AS este_mes_maximo,
    MIN(CASE WHEN fh >= DATE_TRUNC('month', CURRENT_DATE) THEN ai END) AS este_mes_minimo,
    ROUND(STDDEV(CASE WHEN fh >= DATE_TRUNC('month', CURRENT_DATE) THEN ai END), 2) AS este_mes_stddev,
    ROUND(VAR_SAMP(CASE WHEN fh >= DATE_TRUNC('month', CURRENT_DATE) THEN ai END), 2) AS este_mes_varianza,
    
    -- Mes pasado
    SUM(CASE WHEN fh >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH) 
                  AND fh < DATE_TRUNC('month', CURRENT_DATE) THEN ai END) AS mes_pasado_suma,
    ROUND(AVG(CASE WHEN fh >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH) 
                        AND fh < DATE_TRUNC('month', CURRENT_DATE) THEN ai END), 2) AS mes_pasado_media,
    MAX(CASE WHEN fh >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH) 
                  AND fh < DATE_TRUNC('month', CURRENT_DATE) THEN ai END) AS mes_pasado_maximo,
    MIN(CASE WHEN fh >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH) 
                  AND fh < DATE_TRUNC('month', CURRENT_DATE) THEN ai END) AS mes_pasado_minimo,
    ROUND(STDDEV(CASE WHEN fh >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH) 
                           AND fh < DATE_TRUNC('month', CURRENT_DATE) THEN ai END), 2) AS mes_pasado_stddev,
    ROUND(VAR_SAMP(CASE WHEN fh >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH) 
                            AND fh < DATE_TRUNC('month', CURRENT_DATE) THEN ai END), 2) AS mes_pasado_varianza,
    
    -- Mismo mes año pasado
    SUM(CASE WHEN MONTH(fh) = MONTH(CURRENT_DATE) 
                  AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END) AS mismo_mes_anio_pasado_suma,
    ROUND(AVG(CASE WHEN MONTH(fh) = MONTH(CURRENT_DATE) 
                        AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END), 2) AS mismo_mes_anio_pasado_media,
    MAX(CASE WHEN MONTH(fh) = MONTH(CURRENT_DATE) 
                  AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END) AS mismo_mes_anio_pasado_maximo,
    MIN(CASE WHEN MONTH(fh) = MONTH(CURRENT_DATE) 
                  AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END) AS mismo_mes_anio_pasado_minimo,
    ROUND(STDDEV(CASE WHEN MONTH(fh) = MONTH(CURRENT_DATE) 
                           AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END), 2) AS mismo_mes_anio_pasado_stddev,
    ROUND(VAR_SAMP(CASE WHEN MONTH(fh) = MONTH(CURRENT_DATE) 
                            AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END), 2) AS mismo_mes_anio_pasado_varianza,
    
    -- Este trimestre
    SUM(CASE WHEN fh >= DATE_TRUNC('quarter', CURRENT_DATE) THEN ai END) AS este_trimestre_suma,
    ROUND(AVG(CASE WHEN fh >= DATE_TRUNC('quarter', CURRENT_DATE) THEN ai END), 2) AS este_trimestre_media,
    MAX(CASE WHEN fh >= DATE_TRUNC('quarter', CURRENT_DATE) THEN ai END) AS este_trimestre_maximo,
    MIN(CASE WHEN fh >= DATE_TRUNC('quarter', CURRENT_DATE) THEN ai END) AS este_trimestre_minimo,
    ROUND(STDDEV(CASE WHEN fh >= DATE_TRUNC('quarter', CURRENT_DATE) THEN ai END), 2) AS este_trimestre_stddev,
    ROUND(VAR_SAMP(CASE WHEN fh >= DATE_TRUNC('quarter', CURRENT_DATE) THEN ai END), 2) AS este_trimestre_varianza,
    
    -- Trimestre pasado
    SUM(CASE WHEN fh >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3' MONTH) 
                  AND fh < DATE_TRUNC('quarter', CURRENT_DATE) THEN ai END) AS trimestre_pasado_suma,
    ROUND(AVG(CASE WHEN fh >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3' MONTH) 
                        AND fh < DATE_TRUNC('quarter', CURRENT_DATE) THEN ai END), 2) AS trimestre_pasado_media,
    MAX(CASE WHEN fh >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3' MONTH) 
                  AND fh < DATE_TRUNC('quarter', CURRENT_DATE) THEN ai END) AS trimestre_pasado_maximo,
    MIN(CASE WHEN fh >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3' MONTH) 
                  AND fh < DATE_TRUNC('quarter', CURRENT_DATE) THEN ai END) AS trimestre_pasado_minimo,
    ROUND(STDDEV(CASE WHEN fh >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3' MONTH) 
                           AND fh < DATE_TRUNC('quarter', CURRENT_DATE) THEN ai END), 2) AS trimestre_pasado_stddev,
    ROUND(VAR_SAMP(CASE WHEN fh >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3' MONTH) 
                            AND fh < DATE_TRUNC('quarter', CURRENT_DATE) THEN ai END), 2) AS trimestre_pasado_varianza,
    
    -- Mismo trimestre año pasado
    SUM(CASE WHEN QUARTER(fh) = QUARTER(CURRENT_DATE) 
                  AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END) AS mismo_trimestre_anio_pasado_suma,
    ROUND(AVG(CASE WHEN QUARTER(fh) = QUARTER(CURRENT_DATE) 
                        AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END), 2) AS mismo_trimestre_anio_pasado_media,
    MAX(CASE WHEN QUARTER(fh) = QUARTER(CURRENT_DATE) 
                  AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END) AS mismo_trimestre_anio_pasado_maximo,
    MIN(CASE WHEN QUARTER(fh) = QUARTER(CURRENT_DATE) 
                  AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END) AS mismo_trimestre_anio_pasado_minimo,
    ROUND(STDDEV(CASE WHEN QUARTER(fh) = QUARTER(CURRENT_DATE) 
                           AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END), 2) AS mismo_trimestre_anio_pasado_stddev,
    ROUND(VAR_SAMP(CASE WHEN QUARTER(fh) = QUARTER(CURRENT_DATE) 
                            AND YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END), 2) AS mismo_trimestre_anio_pasado_varianza,
    
    -- Este año
    SUM(CASE WHEN YEAR(fh) = YEAR(CURRENT_DATE) THEN ai END) AS este_anio_suma,
    ROUND(AVG(CASE WHEN YEAR(fh) = YEAR(CURRENT_DATE) THEN ai END), 2) AS este_anio_media,
    MAX(CASE WHEN YEAR(fh) = YEAR(CURRENT_DATE) THEN ai END) AS este_anio_maximo,
    MIN(CASE WHEN YEAR(fh) = YEAR(CURRENT_DATE) THEN ai END) AS este_anio_minimo,
    ROUND(STDDEV(CASE WHEN YEAR(fh) = YEAR(CURRENT_DATE) THEN ai END), 2) AS este_anio_stddev,
    ROUND(VAR_SAMP(CASE WHEN YEAR(fh) = YEAR(CURRENT_DATE) THEN ai END), 2) AS este_anio_varianza,
    
    -- Año pasado
    SUM(CASE WHEN YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END) AS anio_pasado_suma,
    ROUND(AVG(CASE WHEN YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END), 2) AS anio_pasado_media,
    MAX(CASE WHEN YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END) AS anio_pasado_maximo,
    MIN(CASE WHEN YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END) AS anio_pasado_minimo,
    ROUND(STDDEV(CASE WHEN YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END), 2) AS anio_pasado_stddev,
    ROUND(VAR_SAMP(CASE WHEN YEAR(fh) = YEAR(CURRENT_DATE - INTERVAL '1' YEAR) THEN ai END), 2) AS anio_pasado_varianza

FROM transformation_esir.s02 a
INNER JOIN master_esir_scada.grid_contadores b ON a.cnt_id = b.cnt_sgc
WHERE 
    -- Optimización de particiones: conversión de tipos para Trino
    (
        -- Año actual completo (para este año, trimestre, mes, semanas)
        (a.partition_0 = CAST(YEAR(CURRENT_DATE) AS VARCHAR))
        OR
        -- Año pasado completo (para comparaciones anuales y mismos períodos año pasado)
        (a.partition_0 = CAST(YEAR(CURRENT_DATE - INTERVAL '1' YEAR) AS VARCHAR))
    )
    -- Filtro adicional por fecha para mayor precisión
    AND a.fh >= CURRENT_DATE - INTERVAL '15' MONTH
GROUP BY b.cups_sgc
ORDER BY b.cups_sgc;