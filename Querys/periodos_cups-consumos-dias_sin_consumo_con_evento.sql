WITH fechas_referencia AS (
  SELECT 
        CURRENT_DATE AS hoy,
 
        -- Semanas
        DATE_TRUNC('week', CURRENT_DATE) AS inicio_semana_actual,
        DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '6' DAY AS fin_semana_actual,
        DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '7' DAY AS inicio_semana_pasada,
        DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1' DAY AS fin_semana_pasada,
 
        DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1' YEAR) AS inicio_misma_semana_anio_pasado,
        DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '6' DAY AS fin_misma_semana_anio_pasado,
 
        -- Meses
        DATE_TRUNC('month', CURRENT_DATE) AS inicio_mes_actual,
        DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1' MONTH - INTERVAL '1' DAY AS fin_mes_actual,
 
        DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH) AS inicio_mes_pasado,
        DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' DAY AS fin_mes_pasado,
 
        DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' YEAR) AS inicio_mismo_mes_anio_pasado,
        DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '1' MONTH - INTERVAL '1' DAY AS fin_mismo_mes_anio_pasado,
 
        -- Trimestres
        DATE_TRUNC('quarter', CURRENT_DATE) AS inicio_trimestre_actual,
        DATE_TRUNC('quarter', CURRENT_DATE) + INTERVAL '3' MONTH - INTERVAL '1' DAY AS fin_trimestre_actual,
 
        DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3' MONTH) AS inicio_trimestre_pasado,
        DATE_TRUNC('quarter', CURRENT_DATE) - INTERVAL '1' DAY AS fin_trimestre_pasado,
 
        DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '1' YEAR) AS inicio_mismo_trimestre_anio_pasado,
        DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '3' MONTH - INTERVAL '1' DAY AS fin_mismo_trimestre_anio_pasado,
 
        -- Años
        DATE_TRUNC('year', CURRENT_DATE) AS inicio_anio_actual,
        DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1' YEAR - INTERVAL '1' DAY AS fin_anio_actual,
 
        DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1' YEAR) AS inicio_anio_pasado,
        DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '1' YEAR - INTERVAL '1' DAY AS fin_anio_pasado
),
datos_filtrados AS (
  SELECT
    a.cnt_id,
    b.cups_sgc,
    a.fh,
    a.ai,
    a.partition_0,
    a.partition_1,
    a.partition_2,
    FROM_BASE(a.bc, 16) AS bc_decimal
  FROM transformation_esir.s02 a
  INNER JOIN master_esir_scada.grid_contadores b
    ON a.cnt_id = b.cnt_sgc
  WHERE -- Dos años (actual y anterior) para el partition_0
        a.partition_0 IN (
            CAST(YEAR(CURRENT_DATE) AS VARCHAR),
            CAST(YEAR(CURRENT_DATE - INTERVAL '1' YEAR) AS VARCHAR)
        )
        AND a.fh >= CURRENT_DATE - INTERVAL '15' MONTH
        AND b.origen = 'ZEUS'
        AND b.provincia_sgc in ('TOLEDO','CIUDAD REAL')
),
datos_por_dia AS (
  -- Base para las métricas de eventos: días sin consumo con eventos por día
  SELECT 
    df.cups_sgc,
    DATE(df.fh) as fecha_dia,
    CASE WHEN SUM(df.ai) = 0 THEN 1 ELSE 0 END as sin_consumo,
    CASE WHEN COUNT(DISTINCT s09.c) > 0 THEN 1 ELSE 0 END as con_eventos,
    CASE WHEN COUNT(DISTINCT CASE WHEN s09.et = 4 THEN s09.c END) > 0 THEN 1 ELSE 0 END as con_eventos_grupo4
  FROM datos_filtrados df
  LEFT JOIN transformation_esir.s09 s09 
    ON df.cnt_id = s09.cnt_id
    AND df.partition_0 = s09.partition_0
    AND df.partition_1 = s09.partition_1  
    AND df.partition_2 = s09.partition_2
    AND DATE_TRUNC('hour', df.fh) = DATE_TRUNC('hour', s09.fh)
  GROUP BY df.cups_sgc, DATE(df.fh)
),
metricas_consumos AS (
  SELECT
    df.cups_sgc,
    -- Semana actual
    -- estadísticos del consumo
    SUM(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END) AS consumo_suma_semana_actual,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END), 2) AS consumo_media_semana_actual,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END) AS consumo_max_semana_actual,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END) AS consumo_min_semana_actual,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END), 2) AS consumo_stddev_semana_actual,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END), 2) AS consumo_var_semana_actual,
    -- skweness
    ROUND(SKEWNESS(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END), 2) skewness_semana_actual,
    -- consumo zero y consumo umbral
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual) AS consumo_zero_semana_actual,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual) AS consumo_umbral_semana_actual,
    -- ratio dia noche
    SUM(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND EXTRACT(HOUR FROM df.fh) BETWEEN 8 AND 19 THEN df.ai END) AS consumo_dia_semana_actual,
    SUM(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND (EXTRACT(HOUR FROM df.fh) < 8 OR EXTRACT(HOUR FROM df.fh) >= 20) THEN df.ai END) AS consumo_noche_semana_actual,

    -- Semana pasada
    -- estadísticos del consumo
    SUM(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END) AS consumo_suma_semana_pasada,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END), 2) AS consumo_media_semana_pasada,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END) AS consumo_max_semana_pasada,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END) AS consumo_min_semana_pasada,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END), 2) AS consumo_stddev_semana_pasada,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END), 2) AS consumo_var_semana_pasada,
    -- skweness
    ROUND(SKEWNESS(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END), 2) skewness_semana_pasada,
    -- consumo zero y umbral
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada) AS consumo_zero_semana_pasada,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada) AS consumo_umbral_semana_pasada,
    -- ratio dia noche
    SUM(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND EXTRACT(HOUR FROM df.fh) BETWEEN 8 AND 19 THEN df.ai END) AS consumo_dia_semana_pasada,
    SUM(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND (EXTRACT(HOUR FROM df.fh) < 8 OR EXTRACT(HOUR FROM df.fh) >= 20) THEN df.ai END) AS consumo_noche_semana_pasada,

    -- Misma semana año pasado
    -- estadísticos consumo
    SUM(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END) AS consumo_suma_semana_anio_pasado,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END), 2) AS consumo_media_semana_anio_pasado,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END) AS consumo_max_semana_anio_pasado,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END) AS consumo_min_semana_anio_pasado,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END), 2) AS consumo_stddev_semana_anio_pasado,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END), 2) AS consumo_var_semana_anio_pasado,
    -- skweness
    ROUND(SKEWNESS(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END), 2) skewness_semana_anio_pasado,
    -- consumo zero y umbral
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado) AS consumo_zero_semana_anio_pasado,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado) AS consumo_umbral_semana_anio_pasado,
    -- ratio dia noche
    SUM(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND EXTRACT(HOUR FROM df.fh) BETWEEN 8 AND 19 THEN df.ai END) AS consumo_dia_semana_anio_pasado,
    SUM(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND (EXTRACT(HOUR FROM df.fh) < 8 OR EXTRACT(HOUR FROM df.fh) >= 20) THEN df.ai END) AS consumo_noche_semana_anio_pasado,

    -- Mes actual
    -- estadísticos de consumo
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END) AS consumo_suma_mes_actual,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END), 2) AS consumo_media_mes_actual,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END) AS consumo_max_mes_actual,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END) AS consumo_min_mes_actual,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END), 2) AS consumo_stddev_mes_actual,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END), 2) AS consumo_var_mes_actual,
    -- skweness
    ROUND(SKEWNESS(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END), 2) skewness_mes_actual,
    -- consumo zero y umbral
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual) AS consumo_zero_mes_actual,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual) AS consumo_umbral_mes_actual,
    -- ratio dia noche
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND EXTRACT(HOUR FROM df.fh) BETWEEN 8 AND 19 THEN df.ai END) AS consumo_dia_mes_actual,
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND (EXTRACT(HOUR FROM df.fh) < 8 OR EXTRACT(HOUR FROM df.fh) >= 20) THEN df.ai END) AS consumo_noche_mes_actual,


    -- Mes pasado
    -- estadísticos de consumo
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END) AS consumo_suma_mes_pasado,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END), 2) AS consumo_media_mes_pasado,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END) AS consumo_max_mes_pasado,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END) AS consumo_min_mes_pasado,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END), 2) AS consumo_stddev_mes_pasado,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END), 2) AS consumo_var_mes_pasado,
    -- skweness
    ROUND(SKEWNESS(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END), 2) skewness_mes_pasado,
    -- consumo zero y umbral
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado) AS consumo_zero_mes_pasado,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado) AS consumo_umbral_mes_pasado,
    -- ratio dia noche
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND EXTRACT(HOUR FROM df.fh) BETWEEN 8 AND 19 THEN df.ai END) AS consumo_dia_mes_pasado,
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND (EXTRACT(HOUR FROM df.fh) < 8 OR EXTRACT(HOUR FROM df.fh) >= 20) THEN df.ai END) AS consumo_noche_mes_pasado,

    -- Mismo mes año pasado
    -- estadísticos del consumo
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END) AS consumo_suma_mes_anio_pasado,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END), 2) AS consumo_media_mes_anio_pasado,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END) AS consumo_max_mes_anio_pasado,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END) AS consumo_min_mes_anio_pasado,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END), 2) AS consumo_stddev_mes_anio_pasado,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END), 2) AS consumo_var_mes_anio_pasado,
    -- skweness
    ROUND(SKEWNESS(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END), 2) skewness_mes_anio_pasado,
    -- consumo zero y umbral
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado) AS consumo_zero_mes_anio_pasado,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado) AS consumo_umbral_mes_anio_pasado,
    -- ratio dia noche
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND EXTRACT(HOUR FROM df.fh) BETWEEN 8 AND 19 THEN df.ai END) AS consumo_dia_mes_anio_pasado,
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND (EXTRACT(HOUR FROM df.fh) < 8 OR EXTRACT(HOUR FROM df.fh) >= 20) THEN df.ai END) AS consumo_noche_mes_anio_pasado,


    -- Trimestre actual
    SUM(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END) AS consumo_suma_trimestre_actual,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END), 2) AS consumo_media_trimestre_actual,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END) AS consumo_max_trimestre_actual,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END) AS consumo_min_trimestre_actual,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END), 2) AS consumo_stddev_trimestre_actual,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END), 2) AS consumo_var_trimestre_actual,
    -- skweness
    ROUND(SKEWNESS(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END), 2) skewness_trimestre_actual,
    -- consumo zero y umbral
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual) AS consumo_zero_trimestre_actual,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual) AS consumo_umbral_trimestre_actual,
    -- ratio dia noche
    SUM(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND EXTRACT(HOUR FROM df.fh) BETWEEN 8 AND 19 THEN df.ai END) AS consumo_dia_trimestre_actual,
    SUM(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND (EXTRACT(HOUR FROM df.fh) < 8 OR EXTRACT(HOUR FROM df.fh) >= 20) THEN df.ai END) AS consumo_noche_trimestre_actual,

    -- Trimestre pasado
    -- estadísticos consumo
    SUM(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END) AS consumo_suma_trimestre_pasado,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END), 2) AS consumo_media_trimestre_pasado,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END) AS consumo_max_trimestre_pasado,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END) AS consumo_min_trimestre_pasado,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END), 2) AS consumo_stddev_trimestre_pasado,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END), 2) AS consumo_var_trimestre_pasado,
    -- skweness
    ROUND(SKEWNESS(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END), 2) skewness_trimestre_pasado,
    -- consumo zero y umbral
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado) AS consumo_zero_trimestre_pasado,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado) AS consumo_umbral_trimestre_pasado,
    -- ratio dia noche
    SUM(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND EXTRACT(HOUR FROM df.fh) BETWEEN 8 AND 19 THEN df.ai END) AS consumo_dia_trimestre_pasado,
    SUM(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND (EXTRACT(HOUR FROM df.fh) < 8 OR EXTRACT(HOUR FROM df.fh) >= 20) THEN df.ai END) AS consumo_noche_trimestre_pasado,

    -- Mismo trimestre año pasado
    -- estadísticos consumo
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END) AS consumo_suma_trimestre_anio_pasado,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END), 2) AS consumo_media_trimestre_anio_pasado,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END) AS consumo_max_trimestre_anio_pasado,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END) AS consumo_min_trimestre_anio_pasado,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END), 2) AS consumo_stddev_trimestre_anio_pasado,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END), 2) AS consumo_var_trimestre_anio_pasado,
    -- skweness
    ROUND(SKEWNESS(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END), 2) skewness_trimestre_anio_pasado,
    -- consumo zero y umbral
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado) AS consumo_zero_trimestre_anio_pasado,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado) AS consumo_umbral_trimestre_anio_pasado,
    -- ratio dia noche
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND EXTRACT(HOUR FROM df.fh) BETWEEN 8 AND 19 THEN df.ai END) AS consumo_dia_trimestre_anio_pasado,
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND (EXTRACT(HOUR FROM df.fh) < 8 OR EXTRACT(HOUR FROM df.fh) >= 20) THEN df.ai END) AS consumo_noche_trimestre_anio_pasado,


    -- Año actual
    -- estadísticos consumo
    SUM(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END) AS consumo_suma_anio_actual,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END), 2) AS consumo_media_anio_actual,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END) AS consumo_max_anio_actual,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END) AS consumo_min_anio_actual,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END), 2) AS consumo_stddev_anio_actual,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END), 2) AS consumo_var_anio_actual,
    -- skweness
    ROUND(SKEWNESS(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END), 2) skewness_anio_actual,
    -- consumo zero y umbral
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual) AS consumo_zero_anio_actual,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual) AS consumo_umbral_anio_actual,
    -- ratio dia noche
    SUM(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND EXTRACT(HOUR FROM df.fh) BETWEEN 8 AND 19 THEN df.ai END) AS consumo_dia_anio_actual,
    SUM(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND (EXTRACT(HOUR FROM df.fh) < 8 OR EXTRACT(HOUR FROM df.fh) >= 20) THEN df.ai END) AS consumo_noche_anio_actual,

    -- Año pasado
    -- estadísticos consumo
    SUM(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END) AS consumo_suma_anio_pasado,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END), 2) AS consumo_media_anio_pasado,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END) AS consumo_max_anio_pasado,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END) AS consumo_min_anio_pasado,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END), 2) AS consumo_stddev_anio_pasado,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END), 2) AS consumo_var_anio_pasado,
    -- skweness
    ROUND(SKEWNESS(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END), 2) skewness_anio_pasado,
    -- consumo zero y umbral
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado) AS consumo_zero_anio_pasado,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado) AS consumo_umbral_anio_pasado,
    -- ratio dia noche
    SUM(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND EXTRACT(HOUR FROM df.fh) BETWEEN 8 AND 19 THEN df.ai END) AS consumo_dia_anio_pasado,
    SUM(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND (EXTRACT(HOUR FROM df.fh) < 8 OR EXTRACT(HOUR FROM df.fh) >= 20) THEN df.ai END) AS consumo_noche_anio_pasado

  FROM datos_filtrados df, fechas_referencia f
  GROUP BY df.cups_sgc
),


metricas_eventos AS (
  -- NUEVO: Métricas de días sin consumo con eventos por período
  SELECT
    dpd.cups_sgc,
    
    -- Semana actual - eventos
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_semana_actual AND f.fin_semana_actual 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_semana_actual,
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_semana_actual AND f.fin_semana_actual 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos_grupo4 = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_grupo4_semana_actual,
    COUNT(*) FILTER (WHERE dpd.fecha_dia BETWEEN f.inicio_semana_actual AND f.fin_semana_actual) AS count_eventos_semana_actual,
    
    -- Semana pasada - eventos
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_semana_pasada,
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos_grupo4 = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_grupo4_semana_pasada,
    COUNT(*) FILTER (WHERE dpd.fecha_dia BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada) AS count_eventos_semana_pasada,
    
    -- Misma semana año pasado - eventos
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_semana_anio_pasado,
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos_grupo4 = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_grupo4_semana_anio_pasado,
    COUNT(*) FILTER (WHERE dpd.fecha_dia BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado) AS count_eventos_semana_anio_pasado,
    
    -- Mes actual - eventos
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_mes_actual AND f.fin_mes_actual 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_mes_actual,
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_mes_actual AND f.fin_mes_actual 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos_grupo4 = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_grupo4_mes_actual,
    COUNT(*) FILTER (WHERE dpd.fecha_dia BETWEEN f.inicio_mes_actual AND f.fin_mes_actual) AS count_eventos_mes_actual,

    
    -- Mes pasado - eventos
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_mes_pasado,
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos_grupo4 = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_grupo4_mes_pasado,
    COUNT(*) FILTER (WHERE dpd.fecha_dia BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado) AS count_eventos_mes_pasado,
    
    -- Mismo mes año pasado - eventos
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_mes_anio_pasado,
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos_grupo4 = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_grupo4_mes_anio_pasado,
    COUNT(*) FILTER (WHERE dpd.fecha_dia BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado) AS count_eventos_mes_anio_pasado,
    
    -- Trimestre actual - eventos
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_trimestre_actual,
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos_grupo4 = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_grupo4_trimestre_actual,
    COUNT(*) FILTER (WHERE dpd.fecha_dia BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual) AS count_eventos_trimestre_actual,
    
    -- Trimestre pasado - eventos
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_trimestre_pasado,
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos_grupo4 = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_grupo4_trimestre_pasado,
    COUNT(*) FILTER (WHERE dpd.fecha_dia BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado) AS count_eventos_trimestre_pasado,
    
    -- Mismo trimestre año pasado - eventos
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_trimestre_anio_pasado,
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos_grupo4 = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_grupo4_trimestre_anio_pasado,
    COUNT(*) FILTER (WHERE dpd.fecha_dia BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado) AS count_eventos_trimestre_anio_pasado,
    
    -- Año actual - eventos
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_anio_actual AND f.fin_anio_actual 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_anio_actual,
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_anio_actual AND f.fin_anio_actual 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos_grupo4 = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_grupo4_anio_actual,
    COUNT(*) FILTER (WHERE dpd.fecha_dia BETWEEN f.inicio_anio_actual AND f.fin_anio_actual) AS count_eventos_anio_actual,
    
    -- Año pasado - eventos
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_anio_pasado,
    SUM(CASE WHEN dpd.fecha_dia BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado 
                  AND dpd.sin_consumo = 1 AND dpd.con_eventos_grupo4 = 1 THEN 1 ELSE 0 END) AS eventos_dias_sin_consumo_con_evento_grupo4_anio_pasado,
    COUNT(*) FILTER (WHERE dpd.fecha_dia BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado) AS count_eventos_anio_pasado

  FROM datos_por_dia dpd, fechas_referencia f
  GROUP BY dpd.cups_sgc
),
metricas_eventos_grupo4_por_tipo AS (
  SELECT
    df.cups_sgc,
    
    -- Semana actual - eventos grupo 4 por tipo
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 1 THEN 1 END) AS eventos_grupo4_tipo1_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 2 THEN 1 END) AS eventos_grupo4_tipo2_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 3 THEN 1 END) AS eventos_grupo4_tipo3_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 4 THEN 1 END) AS eventos_grupo4_tipo4_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 5 THEN 1 END) AS eventos_grupo4_tipo5_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 6 THEN 1 END) AS eventos_grupo4_tipo6_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 7 THEN 1 END) AS eventos_grupo4_tipo7_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 8 THEN 1 END) AS eventos_grupo4_tipo8_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 9 THEN 1 END) AS eventos_grupo4_tipo9_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 10 THEN 1 END) AS eventos_grupo4_tipo10_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 11 THEN 1 END) AS eventos_grupo4_tipo11_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 12 THEN 1 END) AS eventos_grupo4_tipo12_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 13 THEN 1 END) AS eventos_grupo4_tipo13_semana_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual AND s09.et = 4 AND s09.c = 14 THEN 1 END) AS eventos_grupo4_tipo14_semana_actual,
    
    -- Semana pasada - eventos grupo 4 por tipo
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 1 THEN 1 END) AS eventos_grupo4_tipo1_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 2 THEN 1 END) AS eventos_grupo4_tipo2_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 3 THEN 1 END) AS eventos_grupo4_tipo3_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 4 THEN 1 END) AS eventos_grupo4_tipo4_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 5 THEN 1 END) AS eventos_grupo4_tipo5_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 6 THEN 1 END) AS eventos_grupo4_tipo6_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 7 THEN 1 END) AS eventos_grupo4_tipo7_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 8 THEN 1 END) AS eventos_grupo4_tipo8_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 9 THEN 1 END) AS eventos_grupo4_tipo9_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 10 THEN 1 END) AS eventos_grupo4_tipo10_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 11 THEN 1 END) AS eventos_grupo4_tipo11_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 12 THEN 1 END) AS eventos_grupo4_tipo12_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 13 THEN 1 END) AS eventos_grupo4_tipo13_semana_pasada,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada AND s09.et = 4 AND s09.c = 14 THEN 1 END) AS eventos_grupo4_tipo14_semana_pasada,
    
    -- Misma semana año pasado - eventos grupo 4 por tipo
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 1 THEN 1 END) AS eventos_grupo4_tipo1_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 2 THEN 1 END) AS eventos_grupo4_tipo2_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 3 THEN 1 END) AS eventos_grupo4_tipo3_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 4 THEN 1 END) AS eventos_grupo4_tipo4_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 5 THEN 1 END) AS eventos_grupo4_tipo5_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 6 THEN 1 END) AS eventos_grupo4_tipo6_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 7 THEN 1 END) AS eventos_grupo4_tipo7_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 8 THEN 1 END) AS eventos_grupo4_tipo8_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 9 THEN 1 END) AS eventos_grupo4_tipo9_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 10 THEN 1 END) AS eventos_grupo4_tipo10_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 11 THEN 1 END) AS eventos_grupo4_tipo11_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 12 THEN 1 END) AS eventos_grupo4_tipo12_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 13 THEN 1 END) AS eventos_grupo4_tipo13_semana_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado AND s09.et = 4 AND s09.c = 14 THEN 1 END) AS eventos_grupo4_tipo14_semana_anio_pasado,
    
    -- Mes actual - eventos grupo 4 por tipo
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 1 THEN 1 END) AS eventos_grupo4_tipo1_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 2 THEN 1 END) AS eventos_grupo4_tipo2_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 3 THEN 1 END) AS eventos_grupo4_tipo3_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 4 THEN 1 END) AS eventos_grupo4_tipo4_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 5 THEN 1 END) AS eventos_grupo4_tipo5_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 6 THEN 1 END) AS eventos_grupo4_tipo6_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 7 THEN 1 END) AS eventos_grupo4_tipo7_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 8 THEN 1 END) AS eventos_grupo4_tipo8_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 9 THEN 1 END) AS eventos_grupo4_tipo9_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 10 THEN 1 END) AS eventos_grupo4_tipo10_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 11 THEN 1 END) AS eventos_grupo4_tipo11_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 12 THEN 1 END) AS eventos_grupo4_tipo12_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 13 THEN 1 END) AS eventos_grupo4_tipo13_mes_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual AND s09.et = 4 AND s09.c = 14 THEN 1 END) AS eventos_grupo4_tipo14_mes_actual,
    
    -- Mes pasado - eventos grupo 4 por tipo
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 1 THEN 1 END) AS eventos_grupo4_tipo1_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 2 THEN 1 END) AS eventos_grupo4_tipo2_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 3 THEN 1 END) AS eventos_grupo4_tipo3_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 4 THEN 1 END) AS eventos_grupo4_tipo4_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 5 THEN 1 END) AS eventos_grupo4_tipo5_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 6 THEN 1 END) AS eventos_grupo4_tipo6_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 7 THEN 1 END) AS eventos_grupo4_tipo7_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 8 THEN 1 END) AS eventos_grupo4_tipo8_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 9 THEN 1 END) AS eventos_grupo4_tipo9_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 10 THEN 1 END) AS eventos_grupo4_tipo10_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 11 THEN 1 END) AS eventos_grupo4_tipo11_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 12 THEN 1 END) AS eventos_grupo4_tipo12_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 13 THEN 1 END) AS eventos_grupo4_tipo13_mes_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado AND s09.et = 4 AND s09.c = 14 THEN 1 END) AS eventos_grupo4_tipo14_mes_pasado,
    
    -- Mismo mes año pasado - eventos grupo 4 por tipo
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 1 THEN 1 END) AS eventos_grupo4_tipo1_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 2 THEN 1 END) AS eventos_grupo4_tipo2_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 3 THEN 1 END) AS eventos_grupo4_tipo3_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 4 THEN 1 END) AS eventos_grupo4_tipo4_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 5 THEN 1 END) AS eventos_grupo4_tipo5_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 6 THEN 1 END) AS eventos_grupo4_tipo6_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 7 THEN 1 END) AS eventos_grupo4_tipo7_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 8 THEN 1 END) AS eventos_grupo4_tipo8_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 9 THEN 1 END) AS eventos_grupo4_tipo9_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 10 THEN 1 END) AS eventos_grupo4_tipo10_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 11 THEN 1 END) AS eventos_grupo4_tipo11_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 12 THEN 1 END) AS eventos_grupo4_tipo12_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 13 THEN 1 END) AS eventos_grupo4_tipo13_mes_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado AND s09.et = 4 AND s09.c = 14 THEN 1 END) AS eventos_grupo4_tipo14_mes_anio_pasado,
    
    -- Trimestre actual - eventos grupo 4 por tipo
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 1 THEN 1 END) AS eventos_grupo4_tipo1_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 2 THEN 1 END) AS eventos_grupo4_tipo2_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 3 THEN 1 END) AS eventos_grupo4_tipo3_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 4 THEN 1 END) AS eventos_grupo4_tipo4_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 5 THEN 1 END) AS eventos_grupo4_tipo5_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 6 THEN 1 END) AS eventos_grupo4_tipo6_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 7 THEN 1 END) AS eventos_grupo4_tipo7_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 8 THEN 1 END) AS eventos_grupo4_tipo8_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 9 THEN 1 END) AS eventos_grupo4_tipo9_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 10 THEN 1 END) AS eventos_grupo4_tipo10_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 11 THEN 1 END) AS eventos_grupo4_tipo11_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 12 THEN 1 END) AS eventos_grupo4_tipo12_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 13 THEN 1 END) AS eventos_grupo4_tipo13_trimestre_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual AND s09.et = 4 AND s09.c = 14 THEN 1 END) AS eventos_grupo4_tipo14_trimestre_actual,
    
    -- Trimestre pasado - eventos grupo 4 por tipo
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 1 THEN 1 END) AS eventos_grupo4_tipo1_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 2 THEN 1 END) AS eventos_grupo4_tipo2_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 3 THEN 1 END) AS eventos_grupo4_tipo3_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 4 THEN 1 END) AS eventos_grupo4_tipo4_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 5 THEN 1 END) AS eventos_grupo4_tipo5_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 6 THEN 1 END) AS eventos_grupo4_tipo6_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 7 THEN 1 END) AS eventos_grupo4_tipo7_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 8 THEN 1 END) AS eventos_grupo4_tipo8_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 9 THEN 1 END) AS eventos_grupo4_tipo9_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 10 THEN 1 END) AS eventos_grupo4_tipo10_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 11 THEN 1 END) AS eventos_grupo4_tipo11_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 12 THEN 1 END) AS eventos_grupo4_tipo12_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 13 THEN 1 END) AS eventos_grupo4_tipo13_trimestre_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado AND s09.et = 4 AND s09.c = 14 THEN 1 END) AS eventos_grupo4_tipo14_trimestre_pasado,
    
    -- Mismo trimestre año pasado - eventos grupo 4 por tipo
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 1 THEN 1 END) AS eventos_grupo4_tipo1_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 2 THEN 1 END) AS eventos_grupo4_tipo2_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 3 THEN 1 END) AS eventos_grupo4_tipo3_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 4 THEN 1 END) AS eventos_grupo4_tipo4_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 5 THEN 1 END) AS eventos_grupo4_tipo5_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 6 THEN 1 END) AS eventos_grupo4_tipo6_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 7 THEN 1 END) AS eventos_grupo4_tipo7_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 8 THEN 1 END) AS eventos_grupo4_tipo8_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 9 THEN 1 END) AS eventos_grupo4_tipo9_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 10 THEN 1 END) AS eventos_grupo4_tipo10_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 11 THEN 1 END) AS eventos_grupo4_tipo11_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 12 THEN 1 END) AS eventos_grupo4_tipo12_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 13 THEN 1 END) AS eventos_grupo4_tipo13_trimestre_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado AND s09.et = 4 AND s09.c = 14 THEN 1 END) AS eventos_grupo4_tipo14_trimestre_anio_pasado,
    
    -- Año actual - eventos grupo 4 por tipo
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 1 THEN 1 END) AS eventos_grupo4_tipo1_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 2 THEN 1 END) AS eventos_grupo4_tipo2_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 3 THEN 1 END) AS eventos_grupo4_tipo3_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 4 THEN 1 END) AS eventos_grupo4_tipo4_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 5 THEN 1 END) AS eventos_grupo4_tipo5_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 6 THEN 1 END) AS eventos_grupo4_tipo6_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 7 THEN 1 END) AS eventos_grupo4_tipo7_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 8 THEN 1 END) AS eventos_grupo4_tipo8_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 9 THEN 1 END) AS eventos_grupo4_tipo9_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 10 THEN 1 END) AS eventos_grupo4_tipo10_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 11 THEN 1 END) AS eventos_grupo4_tipo11_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 12 THEN 1 END) AS eventos_grupo4_tipo12_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 13 THEN 1 END) AS eventos_grupo4_tipo13_anio_actual,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual AND s09.et = 4 AND s09.c = 14 THEN 1 END) AS eventos_grupo4_tipo14_anio_actual,
    
    -- Año pasado - eventos grupo 4 por tipo
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 1 THEN 1 END) AS eventos_grupo4_tipo1_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 2 THEN 1 END) AS eventos_grupo4_tipo2_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 3 THEN 1 END) AS eventos_grupo4_tipo3_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 4 THEN 1 END) AS eventos_grupo4_tipo4_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 5 THEN 1 END) AS eventos_grupo4_tipo5_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 6 THEN 1 END) AS eventos_grupo4_tipo6_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 7 THEN 1 END) AS eventos_grupo4_tipo7_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 8 THEN 1 END) AS eventos_grupo4_tipo8_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 9 THEN 1 END) AS eventos_grupo4_tipo9_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 10 THEN 1 END) AS eventos_grupo4_tipo10_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 11 THEN 1 END) AS eventos_grupo4_tipo11_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 12 THEN 1 END) AS eventos_grupo4_tipo12_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 13 THEN 1 END) AS eventos_grupo4_tipo13_anio_pasado,
    COUNT(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado AND s09.et = 4 AND s09.c = 14 THEN 1 END) AS eventos_grupo4_tipo14_anio_pasado

  FROM datos_filtrados df
  CROSS JOIN fechas_referencia f
  LEFT JOIN transformation_esir.s09 s09 
    ON df.cnt_id = s09.cnt_id
    AND df.partition_0 = s09.partition_0
    AND df.partition_1 = s09.partition_1  
    AND df.partition_2 = s09.partition_2
    AND DATE_TRUNC('hour', df.fh) = DATE_TRUNC('hour', s09.fh)
    AND s09.et = 4  -- Solo eventos del grupo 4
  GROUP BY df.cups_sgc
)

-- SELECT FINAL: Combina todas las métricas de forma modular
SELECT 
  mc.cups_sgc,
  
  -- MÉTRICAS DE CONSUMO (66 columnas)
  mc.consumo_suma_semana_actual,
  mc.consumo_media_semana_actual,
  mc.consumo_max_semana_actual,
  mc.consumo_min_semana_actual,
  mc.consumo_stddev_semana_actual,
  mc.consumo_var_semana_actual,
  mc.skewness_semana_actual,
  mc.consumo_zero_semana_actual,
  mc.consumo_umbral_semana_actual,
  ROUND(mc.consumo_dia_semana_actual/NULLIF(mc.consumo_noche_semana_actual, 0), 2) as ratio_dia_noche_semana_actual,
  mc.consumo_suma_semana_pasada,
  mc.consumo_media_semana_pasada,
  mc.consumo_max_semana_pasada,
  mc.consumo_min_semana_pasada,
  mc.consumo_stddev_semana_pasada,
  mc.consumo_var_semana_pasada,
  mc.skewness_semana_pasada,
  mc.consumo_zero_semana_pasada,
  mc.consumo_umbral_semana_pasada,
  ROUND(((mc.consumo_suma_semana_actual - mc.consumo_suma_semana_pasada) / mc.consumo_suma_semana_pasada) * 100, 2) as pct_change_cons_semana_pasada,
  ROUND(mc.consumo_dia_semana_pasada/NULLIF(mc.consumo_noche_semana_pasada, 0), 2) as ratio_dia_noche_semana_pasada,
  mc.consumo_suma_semana_anio_pasado,
  mc.consumo_media_semana_anio_pasado,
  mc.consumo_max_semana_anio_pasado,
  mc.consumo_min_semana_anio_pasado,
  mc.consumo_stddev_semana_anio_pasado,
  mc.consumo_var_semana_anio_pasado,
  mc.skewness_semana_anio_pasado,
  mc.consumo_zero_semana_anio_pasado,
  mc.consumo_umbral_semana_anio_pasado,
  ROUND(((mc.consumo_suma_semana_actual - mc.consumo_suma_semana_anio_pasado) / mc.consumo_suma_semana_anio_pasado) * 100, 2) as pct_change_cons_misma_semana_anio_pasado,
  ROUND(mc.consumo_dia_semana_anio_pasado/NULLIF(mc.consumo_noche_semana_anio_pasado, 0), 2) as ratio_dia_noche_semana_anio_pasado,
  mc.consumo_suma_mes_actual,
  mc.consumo_media_mes_actual,
  mc.consumo_max_mes_actual,
  mc.consumo_min_mes_actual,
  mc.consumo_stddev_mes_actual,
  mc.consumo_var_mes_actual,
  mc.skewness_mes_actual,
  mc.consumo_zero_mes_actual,
  mc.consumo_umbral_mes_actual,
  ROUND(mc.consumo_dia_mes_actual/NULLIF(mc.consumo_noche_mes_actual, 0), 2) as ratio_dia_noche_mes_actual,
  mc.consumo_suma_mes_pasado,
  mc.consumo_media_mes_pasado,
  mc.consumo_max_mes_pasado,
  mc.consumo_min_mes_pasado,
  mc.consumo_stddev_mes_pasado,
  mc.consumo_var_mes_pasado,
  mc.skewness_mes_pasado,
  mc.consumo_zero_mes_pasado,
  mc.consumo_umbral_mes_pasado,
  ROUND(((mc.consumo_suma_mes_actual - mc.consumo_suma_mes_pasado) / mc.consumo_suma_mes_pasado) * 100, 2) as pct_change_cons_mes_pasado,
  ROUND(mc.consumo_dia_mes_pasado/NULLIF(mc.consumo_noche_mes_pasado, 0), 2) as ratio_dia_noche_mes_pasado,
  mc.consumo_suma_mes_anio_pasado,
  mc.consumo_media_mes_anio_pasado,
  mc.consumo_max_mes_anio_pasado,
  mc.consumo_min_mes_anio_pasado,
  mc.consumo_stddev_mes_anio_pasado,
  mc.consumo_var_mes_anio_pasado,
  mc.skewness_mes_anio_pasado,
  mc.consumo_zero_mes_anio_pasado,
  mc.consumo_umbral_mes_anio_pasado,
  ROUND(((mc.consumo_suma_mes_actual - mc.consumo_suma_mes_anio_pasado) / mc.consumo_suma_mes_anio_pasado) * 100, 2) as pct_change_cons_mismo_mes_anio_pasado,
  ROUND(mc.consumo_dia_mes_anio_pasado/NULLIF(mc.consumo_noche_mes_anio_pasado, 0), 2) as ratio_dia_noche_mes_anio_pasado,
  mc.consumo_suma_trimestre_actual,
  mc.consumo_media_trimestre_actual,
  mc.consumo_max_trimestre_actual,
  mc.consumo_min_trimestre_actual,
  mc.consumo_stddev_trimestre_actual,
  mc.consumo_var_trimestre_actual,
  mc.skewness_trimestre_actual,
  mc.consumo_zero_trimestre_actual,
  mc.consumo_umbral_trimestre_actual,
  ROUND(mc.consumo_dia_trimestre_actual/NULLIF(mc.consumo_noche_trimestre_actual, 0), 2) as ratio_dia_noche_trimestre_actual,
  mc.consumo_suma_trimestre_pasado,
  mc.consumo_media_trimestre_pasado,
  mc.consumo_max_trimestre_pasado,
  mc.consumo_min_trimestre_pasado,
  mc.consumo_stddev_trimestre_pasado,
  mc.consumo_var_trimestre_pasado,
  mc.skewness_trimestre_pasado,
  mc.consumo_zero_trimestre_pasado,
  mc.consumo_umbral_trimestre_pasado,
  ROUND(((mc.consumo_suma_trimestre_actual - mc.consumo_suma_trimestre_pasado) / mc.consumo_suma_trimestre_pasado) * 100, 2) as pct_change_cons_trimestre_pasado,
  ROUND(mc.consumo_dia_trimestre_pasado/NULLIF(mc.consumo_noche_trimestre_pasado, 0), 2) as ratio_dia_noche_trimestre_pasado,
  mc.consumo_suma_trimestre_anio_pasado,
  mc.consumo_media_trimestre_anio_pasado,
  mc.consumo_max_trimestre_anio_pasado,
  mc.consumo_min_trimestre_anio_pasado,
  mc.consumo_stddev_trimestre_anio_pasado,
  mc.consumo_var_trimestre_anio_pasado,
  mc.skewness_trimestre_anio_pasado,
  mc.consumo_zero_trimestre_anio_pasado,
  mc.consumo_umbral_trimestre_anio_pasado,
  ROUND(((mc.consumo_suma_trimestre_actual - mc.consumo_suma_trimestre_anio_pasado) / mc.consumo_suma_trimestre_anio_pasado) * 100, 2) as pct_change_cons_mismo_trimestre_anio_pasado,
  ROUND(mc.consumo_dia_trimestre_anio_pasado/NULLIF(mc.consumo_noche_trimestre_anio_pasado, 0), 2) as ratio_dia_noche_trimestre_anio_pasado,
  mc.consumo_suma_anio_actual,
  mc.consumo_media_anio_actual,
  mc.consumo_max_anio_actual,
  mc.consumo_min_anio_actual,
  mc.consumo_stddev_anio_actual,
  mc.consumo_var_anio_actual,
  mc.skewness_anio_actual,
  mc.consumo_zero_anio_actual,
  mc.consumo_umbral_anio_actual,
  ROUND(mc.consumo_dia_anio_actual/NULLIF(mc.consumo_noche_anio_actual, 0), 2) as ratio_dia_noche_anio_actual,
  mc.consumo_suma_anio_pasado,
  mc.consumo_media_anio_pasado,
  mc.consumo_max_anio_pasado,
  mc.consumo_min_anio_pasado,
  mc.consumo_stddev_anio_pasado,
  mc.consumo_var_anio_pasado,
  mc.skewness_anio_pasado,
  mc.consumo_zero_anio_pasado,
  mc.consumo_umbral_anio_pasado,
  ROUND(((mc.consumo_suma_anio_actual - mc.consumo_suma_anio_pasado) / mc.consumo_suma_anio_pasado) * 100, 2) as pct_change_cons_anio_pasado,
  ROUND(mc.consumo_dia_anio_pasado/NULLIF(mc.consumo_noche_anio_pasado, 0), 2) as ratio_dia_noche_anio_pasado,
  
  -- MÉTRICAS DE EVENTOS (22 columnas)
  me.eventos_dias_sin_consumo_con_evento_semana_actual,
  me.eventos_dias_sin_consumo_con_evento_grupo4_semana_actual,
  me.count_eventos_semana_actual,
  me.eventos_dias_sin_consumo_con_evento_semana_pasada,
  me.eventos_dias_sin_consumo_con_evento_grupo4_semana_pasada,
  me.count_eventos_semana_pasada,
  me.eventos_dias_sin_consumo_con_evento_semana_anio_pasado,
  me.eventos_dias_sin_consumo_con_evento_grupo4_semana_anio_pasado,
  me.count_eventos_semana_anio_pasado,
  me.eventos_dias_sin_consumo_con_evento_mes_actual,
  me.eventos_dias_sin_consumo_con_evento_grupo4_mes_actual,
  me.count_eventos_mes_actual,
  me.eventos_dias_sin_consumo_con_evento_mes_pasado,
  me.eventos_dias_sin_consumo_con_evento_grupo4_mes_pasado,
  me.count_eventos_mes_pasado,
  me.eventos_dias_sin_consumo_con_evento_mes_anio_pasado,
  me.eventos_dias_sin_consumo_con_evento_grupo4_mes_anio_pasado,
  me.count_eventos_mes_anio_pasado,
  me.eventos_dias_sin_consumo_con_evento_trimestre_actual,
  me.eventos_dias_sin_consumo_con_evento_grupo4_trimestre_actual,
  me.count_eventos_trimestre_actual,
  me.eventos_dias_sin_consumo_con_evento_trimestre_pasado,
  me.eventos_dias_sin_consumo_con_evento_grupo4_trimestre_pasado,
  me.count_eventos_trimestre_pasado,
  me.eventos_dias_sin_consumo_con_evento_trimestre_anio_pasado,
  me.eventos_dias_sin_consumo_con_evento_grupo4_trimestre_anio_pasado,
  me.count_eventos_trimestre_anio_pasado,
  me.eventos_dias_sin_consumo_con_evento_anio_actual,
  me.eventos_dias_sin_consumo_con_evento_grupo4_anio_actual,
  me.count_eventos_anio_actual,
  me.eventos_dias_sin_consumo_con_evento_anio_pasado,
  me.eventos_dias_sin_consumo_con_evento_grupo4_anio_pasado,
  me.count_eventos_anio_pasado,
--
  -- MÉTRICAS DE EVENTOS GRUPO 4 POR TIPO (126 columnas)
  -- Semana actual
  meg4.eventos_grupo4_tipo1_semana_actual,
  meg4.eventos_grupo4_tipo2_semana_actual,
  meg4.eventos_grupo4_tipo3_semana_actual,
  meg4.eventos_grupo4_tipo4_semana_actual,
  meg4.eventos_grupo4_tipo5_semana_actual,
  meg4.eventos_grupo4_tipo6_semana_actual,
  meg4.eventos_grupo4_tipo7_semana_actual,
  meg4.eventos_grupo4_tipo8_semana_actual,
  meg4.eventos_grupo4_tipo9_semana_actual,
  meg4.eventos_grupo4_tipo10_semana_actual,
  meg4.eventos_grupo4_tipo11_semana_actual,
  meg4.eventos_grupo4_tipo12_semana_actual,
  meg4.eventos_grupo4_tipo13_semana_actual,
  meg4.eventos_grupo4_tipo14_semana_actual,
  
  -- Semana pasada
  meg4.eventos_grupo4_tipo1_semana_pasada,
  meg4.eventos_grupo4_tipo2_semana_pasada,
  meg4.eventos_grupo4_tipo3_semana_pasada,
  meg4.eventos_grupo4_tipo4_semana_pasada,
  meg4.eventos_grupo4_tipo5_semana_pasada,
  meg4.eventos_grupo4_tipo6_semana_pasada,
  meg4.eventos_grupo4_tipo7_semana_pasada,
  meg4.eventos_grupo4_tipo8_semana_pasada,
  meg4.eventos_grupo4_tipo9_semana_pasada,
  meg4.eventos_grupo4_tipo10_semana_pasada,
  meg4.eventos_grupo4_tipo11_semana_pasada,
  meg4.eventos_grupo4_tipo12_semana_pasada,
  meg4.eventos_grupo4_tipo13_semana_pasada,
  meg4.eventos_grupo4_tipo14_semana_pasada,
  
  -- Semana año pasado
  meg4.eventos_grupo4_tipo1_semana_anio_pasado,
  meg4.eventos_grupo4_tipo2_semana_anio_pasado,
  meg4.eventos_grupo4_tipo3_semana_anio_pasado,
  meg4.eventos_grupo4_tipo4_semana_anio_pasado,
  meg4.eventos_grupo4_tipo5_semana_anio_pasado,
  meg4.eventos_grupo4_tipo6_semana_anio_pasado,
  meg4.eventos_grupo4_tipo7_semana_anio_pasado,
  meg4.eventos_grupo4_tipo8_semana_anio_pasado,
  meg4.eventos_grupo4_tipo9_semana_anio_pasado,
  meg4.eventos_grupo4_tipo10_semana_anio_pasado,
  meg4.eventos_grupo4_tipo11_semana_anio_pasado,
  meg4.eventos_grupo4_tipo12_semana_anio_pasado,
  meg4.eventos_grupo4_tipo13_semana_anio_pasado,
  meg4.eventos_grupo4_tipo14_semana_anio_pasado,
  
  -- Mes actual
  meg4.eventos_grupo4_tipo1_mes_actual,
  meg4.eventos_grupo4_tipo2_mes_actual,
  meg4.eventos_grupo4_tipo3_mes_actual,
  meg4.eventos_grupo4_tipo4_mes_actual,
  meg4.eventos_grupo4_tipo5_mes_actual,
  meg4.eventos_grupo4_tipo6_mes_actual,
  meg4.eventos_grupo4_tipo7_mes_actual,
  meg4.eventos_grupo4_tipo8_mes_actual,
  meg4.eventos_grupo4_tipo9_mes_actual,
  meg4.eventos_grupo4_tipo10_mes_actual,
  meg4.eventos_grupo4_tipo11_mes_actual,
  meg4.eventos_grupo4_tipo12_mes_actual,
  meg4.eventos_grupo4_tipo13_mes_actual,
  meg4.eventos_grupo4_tipo14_mes_actual,
  
  -- Mes pasado
  meg4.eventos_grupo4_tipo1_mes_pasado,
  meg4.eventos_grupo4_tipo2_mes_pasado,
  meg4.eventos_grupo4_tipo3_mes_pasado,
  meg4.eventos_grupo4_tipo4_mes_pasado,
  meg4.eventos_grupo4_tipo5_mes_pasado,
  meg4.eventos_grupo4_tipo6_mes_pasado,
  meg4.eventos_grupo4_tipo7_mes_pasado,
  meg4.eventos_grupo4_tipo8_mes_pasado,
  meg4.eventos_grupo4_tipo9_mes_pasado,
  meg4.eventos_grupo4_tipo10_mes_pasado,
  meg4.eventos_grupo4_tipo11_mes_pasado,
  meg4.eventos_grupo4_tipo12_mes_pasado,
  meg4.eventos_grupo4_tipo13_mes_pasado,
  meg4.eventos_grupo4_tipo14_mes_pasado,
  
  -- Mes año pasado
  meg4.eventos_grupo4_tipo1_mes_anio_pasado,
  meg4.eventos_grupo4_tipo2_mes_anio_pasado,
  meg4.eventos_grupo4_tipo3_mes_anio_pasado,
  meg4.eventos_grupo4_tipo4_mes_anio_pasado,
  meg4.eventos_grupo4_tipo5_mes_anio_pasado,
  meg4.eventos_grupo4_tipo6_mes_anio_pasado,
  meg4.eventos_grupo4_tipo7_mes_anio_pasado,
  meg4.eventos_grupo4_tipo8_mes_anio_pasado,
  meg4.eventos_grupo4_tipo9_mes_anio_pasado,
  meg4.eventos_grupo4_tipo10_mes_anio_pasado,
  meg4.eventos_grupo4_tipo11_mes_anio_pasado,
  meg4.eventos_grupo4_tipo12_mes_anio_pasado,
  meg4.eventos_grupo4_tipo13_mes_anio_pasado,
  meg4.eventos_grupo4_tipo14_mes_anio_pasado,
  
  -- Trimestre actual
  meg4.eventos_grupo4_tipo1_trimestre_actual,
  meg4.eventos_grupo4_tipo2_trimestre_actual,
  meg4.eventos_grupo4_tipo3_trimestre_actual,
  meg4.eventos_grupo4_tipo4_trimestre_actual,
  meg4.eventos_grupo4_tipo5_trimestre_actual,
  meg4.eventos_grupo4_tipo6_trimestre_actual,
  meg4.eventos_grupo4_tipo7_trimestre_actual,
  meg4.eventos_grupo4_tipo8_trimestre_actual,
  meg4.eventos_grupo4_tipo9_trimestre_actual,
  meg4.eventos_grupo4_tipo10_trimestre_actual,
  meg4.eventos_grupo4_tipo11_trimestre_actual,
  meg4.eventos_grupo4_tipo12_trimestre_actual,
  meg4.eventos_grupo4_tipo13_trimestre_actual,
  meg4.eventos_grupo4_tipo14_trimestre_actual,
  
  -- Trimestre pasado
  meg4.eventos_grupo4_tipo1_trimestre_pasado,
  meg4.eventos_grupo4_tipo2_trimestre_pasado,
  meg4.eventos_grupo4_tipo3_trimestre_pasado,
  meg4.eventos_grupo4_tipo4_trimestre_pasado,
  meg4.eventos_grupo4_tipo5_trimestre_pasado,
  meg4.eventos_grupo4_tipo6_trimestre_pasado,
  meg4.eventos_grupo4_tipo7_trimestre_pasado,
  meg4.eventos_grupo4_tipo8_trimestre_pasado,
  meg4.eventos_grupo4_tipo9_trimestre_pasado,
  meg4.eventos_grupo4_tipo10_trimestre_pasado,
  meg4.eventos_grupo4_tipo11_trimestre_pasado,
  meg4.eventos_grupo4_tipo12_trimestre_pasado,
  meg4.eventos_grupo4_tipo13_trimestre_pasado,
  meg4.eventos_grupo4_tipo14_trimestre_pasado,
  
  -- Trimestre año pasado
  meg4.eventos_grupo4_tipo1_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo2_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo3_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo4_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo5_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo6_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo7_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo8_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo9_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo10_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo11_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo12_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo13_trimestre_anio_pasado,
  meg4.eventos_grupo4_tipo14_trimestre_anio_pasado,
  
  -- Año actual
  meg4.eventos_grupo4_tipo1_anio_actual,
  meg4.eventos_grupo4_tipo2_anio_actual,
  meg4.eventos_grupo4_tipo3_anio_actual,
  meg4.eventos_grupo4_tipo4_anio_actual,
  meg4.eventos_grupo4_tipo5_anio_actual,
  meg4.eventos_grupo4_tipo6_anio_actual,
  meg4.eventos_grupo4_tipo7_anio_actual,
  meg4.eventos_grupo4_tipo8_anio_actual,
  meg4.eventos_grupo4_tipo9_anio_actual,
  meg4.eventos_grupo4_tipo10_anio_actual,
  meg4.eventos_grupo4_tipo11_anio_actual,
  meg4.eventos_grupo4_tipo12_anio_actual,
  meg4.eventos_grupo4_tipo13_anio_actual,
  meg4.eventos_grupo4_tipo14_anio_actual,
  
  -- Año pasado
  meg4.eventos_grupo4_tipo1_anio_pasado,
  meg4.eventos_grupo4_tipo2_anio_pasado,
  meg4.eventos_grupo4_tipo3_anio_pasado,
  meg4.eventos_grupo4_tipo4_anio_pasado,
  meg4.eventos_grupo4_tipo5_anio_pasado,
  meg4.eventos_grupo4_tipo6_anio_pasado,
  meg4.eventos_grupo4_tipo7_anio_pasado,
  meg4.eventos_grupo4_tipo8_anio_pasado,
  meg4.eventos_grupo4_tipo9_anio_pasado,
  meg4.eventos_grupo4_tipo10_anio_pasado,
  meg4.eventos_grupo4_tipo11_anio_pasado,
  meg4.eventos_grupo4_tipo12_anio_pasado,
  meg4.eventos_grupo4_tipo13_anio_pasado,
  meg4.eventos_grupo4_tipo14_anio_pasado
  
FROM metricas_consumos mc
JOIN metricas_eventos me ON mc.cups_sgc = me.cups_sgc
LEFT JOIN metricas_eventos_grupo4_por_tipo meg4 ON mc.cups_sgc = meg4.cups_sgc				 
ORDER BY mc.cups_sgc;