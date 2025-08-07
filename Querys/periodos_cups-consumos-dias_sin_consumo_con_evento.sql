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
        AND b.provincia_sgc = 'MADRID' 
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
    SUM(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END) AS consumo_suma_semana_actual,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END), 2) AS consumo_media_semana_actual,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END) AS consumo_max_semana_actual,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END) AS consumo_min_semana_actual,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END), 2) AS consumo_stddev_semana_actual,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual THEN df.ai END), 2) AS consumo_var_semana_actual,
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual) AS consumo_zero_semana_actual,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_semana_actual AND f.fin_semana_actual) AS consumo_umbral_semana_actual,

    -- Semana pasada
    SUM(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END) AS consumo_suma_semana_pasada,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END), 2) AS consumo_media_semana_pasada,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END) AS consumo_max_semana_pasada,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END) AS consumo_min_semana_pasada,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END), 2) AS consumo_stddev_semana_pasada,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada THEN df.ai END), 2) AS consumo_var_semana_pasada,
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada) AS consumo_zero_semana_pasada,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_semana_pasada AND f.fin_semana_pasada) AS consumo_umbral_semana_pasada,
    

    -- Misma semana año pasado
    SUM(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END) AS consumo_suma_semana_anio_pasado,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END), 2) AS consumo_media_semana_anio_pasado,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END) AS consumo_max_semana_anio_pasado,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END) AS consumo_min_semana_anio_pasado,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END), 2) AS consumo_stddev_semana_anio_pasado,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado THEN df.ai END), 2) AS consumo_var_semana_anio_pasado,
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado) AS consumo_zero_semana_anio_pasado,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_misma_semana_anio_pasado AND f.fin_misma_semana_anio_pasado) AS consumo_umbral_semana_anio_pasado,

    -- Mes actual
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END) AS consumo_suma_mes_actual,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END), 2) AS consumo_media_mes_actual,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END) AS consumo_max_mes_actual,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END) AS consumo_min_mes_actual,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END), 2) AS consumo_stddev_mes_actual,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual THEN df.ai END), 2) AS consumo_var_mes_actual,
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual) AS consumo_zero_mes_actual,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mes_actual AND f.fin_mes_actual) AS consumo_umbral_mes_actual,

    -- Mes pasado
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END) AS consumo_suma_mes_pasado,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END), 2) AS consumo_media_mes_pasado,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END) AS consumo_max_mes_pasado,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END) AS consumo_min_mes_pasado,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END), 2) AS consumo_stddev_mes_pasado,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado THEN df.ai END), 2) AS consumo_var_mes_pasado,
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado) AS consumo_zero_mes_pasado,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mes_pasado AND f.fin_mes_pasado) AS consumo_umbral_mes_pasado,

    -- Mismo mes año pasado
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END) AS consumo_suma_mes_anio_pasado,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END), 2) AS consumo_media_mes_anio_pasado,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END) AS consumo_max_mes_anio_pasado,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END) AS consumo_min_mes_anio_pasado,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END), 2) AS consumo_stddev_mes_anio_pasado,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado THEN df.ai END), 2) AS consumo_var_mes_anio_pasado,
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado) AS consumo_zero_mes_anio_pasado,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mismo_mes_anio_pasado AND f.fin_mismo_mes_anio_pasado) AS consumo_umbral_mes_anio_pasado,

    -- Trimestre actual
    SUM(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END) AS consumo_suma_trimestre_actual,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END), 2) AS consumo_media_trimestre_actual,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END) AS consumo_max_trimestre_actual,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END) AS consumo_min_trimestre_actual,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END), 2) AS consumo_stddev_trimestre_actual,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual THEN df.ai END), 2) AS consumo_var_trimestre_actual,
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual) AS consumo_zero_trimestre_actual,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_trimestre_actual AND f.fin_trimestre_actual) AS consumo_umbral_trimestre_actual,

    -- Trimestre pasado
    SUM(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END) AS consumo_suma_trimestre_pasado,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END), 2) AS consumo_media_trimestre_pasado,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END) AS consumo_max_trimestre_pasado,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END) AS consumo_min_trimestre_pasado,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END), 2) AS consumo_stddev_trimestre_pasado,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado THEN df.ai END), 2) AS consumo_var_trimestre_pasado,
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado) AS consumo_zero_trimestre_pasado,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_trimestre_pasado AND f.fin_trimestre_pasado) AS consumo_umbral_trimestre_pasado,

    -- Mismo trimestre año pasado
    SUM(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END) AS consumo_suma_trimestre_anio_pasado,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END), 2) AS consumo_media_trimestre_anio_pasado,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END) AS consumo_max_trimestre_anio_pasado,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END) AS consumo_min_trimestre_anio_pasado,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END), 2) AS consumo_stddev_trimestre_anio_pasado,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado THEN df.ai END), 2) AS consumo_var_trimestre_anio_pasado,
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado) AS consumo_zero_trimestre_anio_pasado,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_mismo_trimestre_anio_pasado AND f.fin_mismo_trimestre_anio_pasado) AS consumo_umbral_trimestre_anio_pasado,

    -- Año actual
    SUM(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END) AS consumo_suma_anio_actual,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END), 2) AS consumo_media_anio_actual,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END) AS consumo_max_anio_actual,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END) AS consumo_min_anio_actual,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END), 2) AS consumo_stddev_anio_actual,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual THEN df.ai END), 2) AS consumo_var_anio_actual,
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual) AS consumo_zero_anio_actual,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_anio_actual AND f.fin_anio_actual) AS consumo_umbral_anio_actual,

    -- Año pasado
    SUM(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END) AS consumo_suma_anio_pasado,
    ROUND(AVG(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END), 2) AS consumo_media_anio_pasado,
    MAX(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END) AS consumo_max_anio_pasado,
    MIN(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END) AS consumo_min_anio_pasado,
    ROUND(STDDEV(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END), 2) AS consumo_stddev_anio_pasado,
    ROUND(VAR_SAMP(CASE WHEN df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado THEN df.ai END), 2) AS consumo_var_anio_pasado,
    COUNT(*) FILTER (WHERE df.ai = 0 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado) AS consumo_zero_anio_pasado,
    COUNT(*) FILTER (WHERE df.ai > 100 AND df.bc_decimal < 80 AND df.fh BETWEEN f.inicio_anio_pasado AND f.fin_anio_pasado) AS consumo_umbral_anio_pasado

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
  mc.consumo_zero_semana_actual,
  mc.consumo_umbral_semana_actual,
  mc.consumo_suma_semana_pasada,
  mc.consumo_media_semana_pasada,
  mc.consumo_max_semana_pasada,
  mc.consumo_min_semana_pasada,
  mc.consumo_stddev_semana_pasada,
  mc.consumo_var_semana_pasada,
  mc.consumo_zero_semana_pasada,
  mc.consumo_umbral_semana_pasada,
  mc.consumo_suma_semana_anio_pasado,
  mc.consumo_media_semana_anio_pasado,
  mc.consumo_max_semana_anio_pasado,
  mc.consumo_min_semana_anio_pasado,
  mc.consumo_stddev_semana_anio_pasado,
  mc.consumo_var_semana_anio_pasado,
  mc.consumo_zero_semana_anio_pasado,
  mc.consumo_umbral_semana_anio_pasado,
  mc.consumo_suma_mes_actual,
  mc.consumo_media_mes_actual,
  mc.consumo_max_mes_actual,
  mc.consumo_min_mes_actual,
  mc.consumo_stddev_mes_actual,
  mc.consumo_var_mes_actual,
  mc.consumo_zero_mes_actual,
  mc.consumo_umbral_mes_actual,
  mc.consumo_suma_mes_pasado,
  mc.consumo_media_mes_pasado,
  mc.consumo_max_mes_pasado,
  mc.consumo_min_mes_pasado,
  mc.consumo_stddev_mes_pasado,
  mc.consumo_var_mes_pasado,
  mc.consumo_zero_mes_pasado,
  mc.consumo_umbral_mes_pasado,
  mc.consumo_suma_mes_anio_pasado,
  mc.consumo_media_mes_anio_pasado,
  mc.consumo_max_mes_anio_pasado,
  mc.consumo_min_mes_anio_pasado,
  mc.consumo_stddev_mes_anio_pasado,
  mc.consumo_var_mes_anio_pasado,
  mc.consumo_zero_mes_anio_pasado,
  mc.consumo_umbral_mes_anio_pasado,
  mc.consumo_suma_trimestre_actual,
  mc.consumo_media_trimestre_actual,
  mc.consumo_max_trimestre_actual,
  mc.consumo_min_trimestre_actual,
  mc.consumo_stddev_trimestre_actual,
  mc.consumo_var_trimestre_actual,
  mc.consumo_zero_trimestre_actual,
  mc.consumo_umbral_trimestre_actual,
  mc.consumo_suma_trimestre_pasado,
  mc.consumo_media_trimestre_pasado,
  mc.consumo_max_trimestre_pasado,
  mc.consumo_min_trimestre_pasado,
  mc.consumo_stddev_trimestre_pasado,
  mc.consumo_var_trimestre_pasado,
  mc.consumo_zero_trimestre_pasado,
  mc.consumo_umbral_trimestre_pasado,
  mc.consumo_suma_trimestre_anio_pasado,
  mc.consumo_media_trimestre_anio_pasado,
  mc.consumo_max_trimestre_anio_pasado,
  mc.consumo_min_trimestre_anio_pasado,
  mc.consumo_stddev_trimestre_anio_pasado,
  mc.consumo_var_trimestre_anio_pasado,
  mc.consumo_zero_trimestre_anio_pasado,
  mc.consumo_umbral_trimestre_anio_pasado,
  mc.consumo_suma_anio_actual,
  mc.consumo_media_anio_actual,
  mc.consumo_max_anio_actual,
  mc.consumo_min_anio_actual,
  mc.consumo_stddev_anio_actual,
  mc.consumo_var_anio_actual,
  mc.consumo_zero_anio_actual,
  mc.consumo_umbral_anio_actual,
  mc.consumo_suma_anio_pasado,
  mc.consumo_media_anio_pasado,
  mc.consumo_max_anio_pasado,
  mc.consumo_min_anio_pasado,
  mc.consumo_stddev_anio_pasado,
  mc.consumo_var_anio_pasado,
  mc.consumo_zero_anio_pasado,
  mc.consumo_umbral_anio_pasado,
  
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
  me.count_eventos_anio_pasado

FROM metricas_consumos mc
LEFT JOIN metricas_eventos me ON mc.cups_sgc = me.cups_sgc
ORDER BY mc.cups_sgc;