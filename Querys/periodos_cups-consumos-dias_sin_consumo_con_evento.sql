-- ========================================
-- VERSIÓN CORREGIDA PARA ATHENA - SIN ERRORES DE AGREGACIÓN
-- ========================================
-- Solucionando: EXPRESSION_NOT_AGGREGATE error
-- Eliminando CROSS JOIN problemático y optimizando agregaciones

-- ========================================
-- PASO 1: VARIABLES DE FECHA SIMPLES (SIN CTE)
-- ========================================

-- ========================================
-- PASO 2: GRID OPTIMIZADO CON FILTROS TEMPRANOS
-- ========================================
WITH grid_optimizado AS (
  SELECT 
    grd.cnt_sgc,
    grd.cups_sgc,
    grd.ct_bdi,
    grd.trafo_bdi,
    grd.salida_bt_bdi,
    grd.clave_acometida_sgc,
    CAST(SUBSTR(grd.cups_sgc, 12, 7) AS INTEGER) AS nis_rad,
    grd.pot_ctto_sgc,
    
    -- Información de expedientes (simplificada)
    CASE WHEN exp.cups IS NOT NULL THEN 'SI' ELSE 'NO' END as tuvo_fraude_previo,
    CASE 
      WHEN exp.fecha_fin_anomalia IS NOT NULL 
        AND TRY(date_parse(exp.fecha_fin_anomalia, '%d/%m/%Y %H:%i:%s')) >= CURRENT_DATE - INTERVAL '90' DAY 
        THEN 'SI'
      ELSE 'NO'
    END as tuvo_fraude_reciente,
    exp.fecha_fin_anomalia
    
  FROM master_esir_scada.grid_contadores grd 
  LEFT JOIN master_irregularidades_fraudes.expedientes exp
    ON grd.cups_sgc = exp.cups
  WHERE grd.origen = 'ZEUS'
    AND grd.provincia_sgc IN ('TOLEDO','CIUDAD REAL')
    AND grd.estado_contrato_sgc = 'SRVSTAT001'
),

-- ========================================
-- PASO 3: MÉTRICAS DE CONSUMO CON FECHAS CALCULADAS DIRECTAMENTE
-- ========================================
metricas_consumo_completas AS (
  SELECT 
    s02.cnt_id,
    grd.cups_sgc,
    grd.ct_bdi,
    grd.trafo_bdi,
    grd.salida_bt_bdi,
    grd.clave_acometida_sgc,
    grd.tuvo_fraude_previo,
    grd.tuvo_fraude_reciente,
    
    -- ========================================
    -- MÉTRICAS DE CONSUMO CON FECHAS CALCULADAS EN LÍNEA
    -- ========================================
    
    -- SEMANA ACTUAL
    SUM(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('week', CURRENT_DATE) 
                      AND DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '6' DAY 
      THEN s02.ai 
    END) AS consumo_suma_semana_actual,
    
    AVG(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('week', CURRENT_DATE) 
                      AND DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '6' DAY 
      THEN s02.ai 
    END) AS consumo_media_semana_actual,
    
    MAX(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('week', CURRENT_DATE) 
                      AND DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '6' DAY 
      THEN s02.ai 
    END) AS consumo_max_semana_actual,
    
    MIN(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('week', CURRENT_DATE) 
                      AND DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '6' DAY 
      THEN s02.ai 
    END) AS consumo_min_semana_actual,
    
    -- SEMANA PASADA
    SUM(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '7' DAY 
                      AND DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_suma_semana_pasada,
    
    AVG(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '7' DAY 
                      AND DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_media_semana_pasada,
    
    MAX(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '7' DAY 
                      AND DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_max_semana_pasada,
    
    MIN(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '7' DAY 
                      AND DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_min_semana_pasada,
    
    -- SEMANA AÑO PASADO
    SUM(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1' YEAR) 
                      AND DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '6' DAY 
      THEN s02.ai 
    END) AS consumo_suma_semana_anio_pasado,
    
    AVG(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1' YEAR) 
                      AND DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '6' DAY 
      THEN s02.ai 
    END) AS consumo_media_semana_anio_pasado,
    
    MAX(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1' YEAR) 
                      AND DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '6' DAY 
      THEN s02.ai 
    END) AS consumo_max_semana_anio_pasado,
    
    -- MES ACTUAL
    SUM(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('month', CURRENT_DATE) 
                      AND DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1' MONTH - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_suma_mes_actual,
    
    AVG(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('month', CURRENT_DATE) 
                      AND DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1' MONTH - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_media_mes_actual,
    
    MAX(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('month', CURRENT_DATE) 
                      AND DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1' MONTH - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_max_mes_actual,
    
    MIN(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('month', CURRENT_DATE) 
                      AND DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1' MONTH - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_min_mes_actual,
    
    -- MES PASADO
    SUM(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH) 
                      AND DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_suma_mes_pasado,
    
    AVG(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH) 
                      AND DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_media_mes_pasado,
    
    MAX(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH) 
                      AND DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_max_mes_pasado,
    
    MIN(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' MONTH) 
                      AND DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_min_mes_pasado,
    
    -- MES AÑO PASADO
    SUM(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' YEAR) 
                      AND DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '1' MONTH - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_suma_mes_anio_pasado,
    
    AVG(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' YEAR) 
                      AND DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '1' MONTH - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_media_mes_anio_pasado,
    
    MAX(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' YEAR) 
                      AND DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '1' MONTH - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_max_mes_anio_pasado,
    
    -- AÑO ACTUAL
    SUM(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('year', CURRENT_DATE) 
                      AND DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1' YEAR - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_suma_anio_actual,
    
    AVG(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('year', CURRENT_DATE) 
                      AND DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1' YEAR - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_media_anio_actual,
    
    MAX(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('year', CURRENT_DATE) 
                      AND DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1' YEAR - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_max_anio_actual,
    
    MIN(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('year', CURRENT_DATE) 
                      AND DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1' YEAR - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_min_anio_actual,
    
    -- AÑO PASADO
    SUM(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1' YEAR) 
                      AND DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '1' YEAR - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_suma_anio_pasado,
    
    AVG(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1' YEAR) 
                      AND DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '1' YEAR - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_media_anio_pasado,
    
    MAX(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1' YEAR) 
                      AND DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '1' YEAR - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_max_anio_pasado,
    
    MIN(CASE 
      WHEN s02.fh BETWEEN DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1' YEAR) 
                      AND DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1' YEAR) + INTERVAL '1' YEAR - INTERVAL '1' DAY 
      THEN s02.ai 
    END) AS consumo_min_anio_pasado
    
  FROM transformation_esir.s02 s02
  INNER JOIN grid_optimizado grd ON s02.cnt_id = grd.cnt_sgc
  WHERE s02.partition_0 IN (
      CAST(YEAR(CURRENT_DATE) AS VARCHAR),
      CAST(YEAR(CURRENT_DATE - INTERVAL '1' YEAR) AS VARCHAR)
    )
    AND s02.fh >= CURRENT_DATE - INTERVAL '15' MONTH
    AND FROM_BASE(s02.bc, 16) < 80
    AND s02.ai BETWEEN 0 AND (grd.pot_ctto_sgc * 1.5)
    -- Filtro de exclusión optimizado
    AND (
      grd.fecha_fin_anomalia IS NULL 
      OR TRY(s02.fh > date_parse(grd.fecha_fin_anomalia, '%d/%m/%Y %H:%i:%s'))
      OR TRY(date_parse(grd.fecha_fin_anomalia, '%d/%m/%Y %H:%i:%s')) IS NULL
    )
  GROUP BY 
    s02.cnt_id, 
    grd.cups_sgc, 
    grd.ct_bdi, 
    grd.trafo_bdi, 
    grd.salida_bt_bdi, 
    grd.clave_acometida_sgc, 
    grd.tuvo_fraude_previo, 
    grd.tuvo_fraude_reciente
),

-- ========================================
-- PASO 4: PERCENTILES SIMPLIFICADOS SOLO PARA CT
-- ========================================
percentiles_ct AS (
  SELECT
    grd.ct_bdi,
    
    -- Solo percentiles críticos para reducir complejidad
    APPROX_PERCENTILE(
      CASE 
        WHEN s02.fh BETWEEN DATE_TRUNC('week', CURRENT_DATE) 
                        AND DATE_TRUNC('week', CURRENT_DATE) + INTERVAL '6' DAY 
        THEN s02.ai 
      END, 0.99
    ) AS percentil99_semana_actual_ct,
    
    APPROX_PERCENTILE(
      CASE 
        WHEN s02.fh BETWEEN DATE_TRUNC('month', CURRENT_DATE) 
                        AND DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1' MONTH - INTERVAL '1' DAY 
        THEN s02.ai 
      END, 0.99
    ) AS percentil99_mes_actual_ct,
    
    APPROX_PERCENTILE(
      CASE 
        WHEN s02.fh BETWEEN DATE_TRUNC('year', CURRENT_DATE) 
                        AND DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '1' YEAR - INTERVAL '1' DAY 
        THEN s02.ai 
      END, 0.99
    ) AS percentil99_anio_actual_ct
    
  FROM transformation_esir.s02 s02
  INNER JOIN grid_optimizado grd ON s02.cnt_id = grd.cnt_sgc
  WHERE s02.partition_0 IN (
      CAST(YEAR(CURRENT_DATE) AS VARCHAR),
      CAST(YEAR(CURRENT_DATE - INTERVAL '1' YEAR) AS VARCHAR)
    )
    AND s02.fh >= CURRENT_DATE - INTERVAL '15' MONTH
    AND FROM_BASE(s02.bc, 16) < 80
    AND s02.ai BETWEEN 0 AND (grd.pot_ctto_sgc * 1.5)
    AND (
      grd.fecha_fin_anomalia IS NULL 
      OR TRY(s02.fh > date_parse(grd.fecha_fin_anomalia, '%d/%m/%Y %H:%i:%s'))
      OR TRY(date_parse(grd.fecha_fin_anomalia, '%d/%m/%Y %H:%i:%s')) IS NULL
    )
  GROUP BY grd.ct_bdi
)

-- ========================================
-- SELECT FINAL OPTIMIZADO Y CORREGIDO
-- ========================================
SELECT 
  mc.cups_sgc,
  
  -- Información de expedientes
  mc.tuvo_fraude_previo,
  mc.tuvo_fraude_reciente,
  
  -- Métricas de consumo principales
  ROUND(COALESCE(mc.consumo_suma_semana_actual, 0), 2) AS consumo_suma_semana_actual,
  ROUND(COALESCE(mc.consumo_media_semana_actual, 0), 2) AS consumo_media_semana_actual,
  ROUND(COALESCE(mc.consumo_max_semana_actual, 0), 2) AS consumo_max_semana_actual,
  ROUND(COALESCE(mc.consumo_min_semana_actual, 0), 2) AS consumo_min_semana_actual,
  
  ROUND(COALESCE(mc.consumo_suma_semana_pasada, 0), 2) AS consumo_suma_semana_pasada,
  ROUND(COALESCE(mc.consumo_media_semana_pasada, 0), 2) AS consumo_media_semana_pasada,
  ROUND(COALESCE(mc.consumo_max_semana_pasada, 0), 2) AS consumo_max_semana_pasada,
  ROUND(COALESCE(mc.consumo_min_semana_pasada, 0), 2) AS consumo_min_semana_pasada,
  
  ROUND(COALESCE(mc.consumo_suma_mes_actual, 0), 2) AS consumo_suma_mes_actual,
  ROUND(COALESCE(mc.consumo_media_mes_actual, 0), 2) AS consumo_media_mes_actual,
  ROUND(COALESCE(mc.consumo_max_mes_actual, 0), 2) AS consumo_max_mes_actual,
  ROUND(COALESCE(mc.consumo_min_mes_actual, 0), 2) AS consumo_min_mes_actual,
  
  ROUND(COALESCE(mc.consumo_suma_mes_pasado, 0), 2) AS consumo_suma_mes_pasado,
  ROUND(COALESCE(mc.consumo_media_mes_pasado, 0), 2) AS consumo_media_mes_pasado,
  ROUND(COALESCE(mc.consumo_max_mes_pasado, 0), 2) AS consumo_max_mes_pasado,
  ROUND(COALESCE(mc.consumo_min_mes_pasado, 0), 2) AS consumo_min_mes_pasado,
  
  ROUND(COALESCE(mc.consumo_suma_anio_actual, 0), 2) AS consumo_suma_anio_actual,
  ROUND(COALESCE(mc.consumo_media_anio_actual, 0), 2) AS consumo_media_anio_actual,
  ROUND(COALESCE(mc.consumo_max_anio_actual, 0), 2) AS consumo_max_anio_actual,
  ROUND(COALESCE(mc.consumo_min_anio_actual, 0), 2) AS consumo_min_anio_actual,
  
  ROUND(COALESCE(mc.consumo_suma_anio_pasado, 0), 2) AS consumo_suma_anio_pasado,
  ROUND(COALESCE(mc.consumo_media_anio_pasado, 0), 2) AS consumo_media_anio_pasado,
  ROUND(COALESCE(mc.consumo_max_anio_pasado, 0), 2) AS consumo_max_anio_pasado,
  ROUND(COALESCE(mc.consumo_min_anio_pasado, 0), 2) AS consumo_min_anio_pasado,
  
  -- Picos anómalos (solo los más importantes)
  CASE 
    WHEN mc.consumo_max_semana_actual > COALESCE(per_ct.percentil99_semana_actual_ct, 999999) 
    THEN true 
    ELSE false 
  END AS pico_anomalo_semana_actual,
  
  CASE 
    WHEN mc.consumo_max_mes_actual > COALESCE(per_ct.percentil99_mes_actual_ct, 999999) 
    THEN true 
    ELSE false 
  END AS pico_anomalo_mes_actual,
  
  CASE 
    WHEN mc.consumo_max_anio_actual > COALESCE(per_ct.percentil99_anio_actual_ct, 999999) 
    THEN true 
    ELSE false 
  END AS pico_anomalo_anio_actual,
  
  -- Ratios de cambio importantes
  ROUND(
    COALESCE(
      (mc.consumo_suma_semana_actual - mc.consumo_suma_semana_pasada) * 100.0 / 
      NULLIF(mc.consumo_suma_semana_pasada, 0), 
      0
    ), 2
  ) as pct_change_semana,
  
  ROUND(
    COALESCE(
      (mc.consumo_suma_mes_actual - mc.consumo_suma_mes_pasado) * 100.0 / 
      NULLIF(mc.consumo_suma_mes_pasado, 0), 
      0
    ), 2
  ) as pct_change_mes,
  
  ROUND(
    COALESCE(
      (mc.consumo_suma_anio_actual - mc.consumo_suma_anio_pasado) * 100.0 / 
      NULLIF(mc.consumo_suma_anio_pasado, 0), 
      0
    ), 2
  ) as pct_change_anio,
  
  -- Clasificación de expedientes
  CASE
    WHEN mc.tuvo_fraude_reciente = 'SI' THEN 'REINCIDENTE_RECIENTE'
    WHEN mc.tuvo_fraude_previo = 'SI' THEN 'REINCIDENTE_RESUELTO'
    ELSE 'LIMPIO'
  END as clasificacion_expedientes

FROM metricas_consumo_completas mc
LEFT JOIN percentiles_ct per_ct 
  ON mc.ct_bdi = per_ct.ct_bdi
WHERE mc.cups_sgc IS NOT NULL
ORDER BY 
  mc.tuvo_fraude_previo DESC,
  mc.cups_sgc;