WITH 
-- ✅ PASO 1: GRID BASE CON FILTROS CRÍTICOS TEMPRANOS
grid_base AS (
  SELECT 
    grd.cnt_sgc,
    grd.cups_sgc,
    CAST(SUBSTR(grd.cups_sgc, 12, 7) AS INTEGER) AS nis_rad,
    grd.pot_ctto_sgc,
    
    -- Información de expedientes simplificada
    exp.fecha_inicio_anomalia,
    exp.fecha_fin_anomalia,
    exp.tipo_anomalia,
    exp.estado,
    
    CASE WHEN exp.cups IS NOT NULL THEN 'SI' ELSE 'NO' END as tuvo_fraude_previo,
    
    -- Fecha límite optimizada con TRY para evitar errores
    CASE 
      WHEN exp.fecha_fin_anomalia IS NOT NULL THEN 
        TRY(date_parse(exp.fecha_fin_anomalia, '%d/%m/%Y %H:%i:%s'))
      ELSE NULL
    END as fecha_limite_timestamp,
    
    -- Estado de fraude simplificado
    CASE 
      WHEN exp.cups IS NULL THEN 'LIMPIO'
      WHEN exp.fecha_fin_anomalia IS NULL THEN 'FRAUDE_ACTIVO'
      WHEN TRY(date_parse(exp.fecha_fin_anomalia, '%d/%m/%Y %H:%i:%s')) < CURRENT_TIMESTAMP THEN 'FRAUDE_RESUELTO'
      ELSE 'FRAUDE_PENDIENTE'
    END as estado_fraude
    
  FROM master_esir_scada.grid_contadores grd 
  LEFT JOIN master_irregularidades_fraudes.expedientes exp
    ON grd.cups_sgc = exp.cups
  WHERE grd.origen = 'ZEUS'
    AND grd.provincia_sgc IN ('TOLEDO','CIUDAD REAL')
    AND grd.estado_contrato_sgc = 'SRVSTAT001'
),

-- ✅ PASO 2: EVENTOS GRUPO 4 CON FILTROS OPTIMIZADOS
eventos_validos AS (
  SELECT 
    s09.cnt_id,
    s09.fh as fecha_hora_evento,
    s09.c as tipo_evento,
    grd.cups_sgc,
    grd.nis_rad,
    grd.tuvo_fraude_previo,
    grd.estado_fraude,
    grd.fecha_limite_timestamp,
    
    -- Cálculo de días desde anomalía previa
    CASE 
      WHEN grd.fecha_limite_timestamp IS NOT NULL THEN
        date_diff('day', grd.fecha_limite_timestamp, s09.fh)
      ELSE NULL
    END as dias_desde_fin_anomalia_previa
    
  FROM transformation_esir.s09 s09
  INNER JOIN grid_base grd ON s09.cnt_id = grd.cnt_sgc
  WHERE s09.et = 4  -- Solo eventos grupo 4
    AND s09.partition_0 IN (
        CAST(YEAR(CURRENT_DATE) AS VARCHAR),
        CAST(YEAR(CURRENT_DATE - INTERVAL '1' YEAR) AS VARCHAR)
    )
    AND s09.fh >= CURRENT_DATE - INTERVAL '15' MONTH
    AND s09.fh >= CURRENT_DATE - INTERVAL '12' MONTH  -- Reducir ventana para optimizar
    -- Filtro de exclusión optimizado
    AND (
      grd.fecha_limite_timestamp IS NULL 
      OR s09.fh > grd.fecha_limite_timestamp
    )
),

-- ✅ PASO 3: CONSUMOS BASE CON FILTROS TEMPRANOS
consumos_base AS (
  SELECT
    s02.cnt_id,
    s02.fh,
    s02.ai,
    grd.cups_sgc,
    grd.fecha_limite_timestamp
  FROM transformation_esir.s02 s02 
  INNER JOIN grid_base grd ON s02.cnt_id = grd.cnt_sgc
  WHERE s02.partition_0 IN (
        CAST(YEAR(CURRENT_DATE) AS VARCHAR),
        CAST(YEAR(CURRENT_DATE - INTERVAL '1' YEAR) AS VARCHAR)
    )
    AND s02.fh >= CURRENT_DATE - INTERVAL '15' MONTH
    AND FROM_BASE(s02.bc, 16) < 80
    AND s02.ai BETWEEN 0 AND (grd.pot_ctto_sgc * 1.5)
    AND s02.ai > 0  -- Solo consumos positivos
    -- Filtro de exclusión
    AND (
      grd.fecha_limite_timestamp IS NULL 
      OR s02.fh > grd.fecha_limite_timestamp
    )
),

-- ✅ PASO 4: INSPECCIONES SIMPLIFICADAS
inspecciones_activas AS (
  SELECT DISTINCT
    insp.nis_rad,
    insp.fecha_ini_os,
    insp.fuce
  FROM transformation_esir.ooss01 insp
  INNER JOIN grid_base grd ON insp.nis_rad = grd.nis_rad
  WHERE insp.partition_0 IN (
        CAST(YEAR(CURRENT_DATE) AS VARCHAR),
        CAST(YEAR(CURRENT_DATE - INTERVAL '1' YEAR) AS VARCHAR)
    )
    AND insp.fecha_ini_os >= CURRENT_DATE - INTERVAL '15' MONTH
    AND insp.cer = 'ACTSTA0014'
),

-- ✅ PASO 5: EVENTOS SIN INSPECCIÓN (OPTIMIZADO)
eventos_limpios AS (
  SELECT 
    ev.*
  FROM eventos_validos ev
  LEFT JOIN inspecciones_activas insp
    ON ev.nis_rad = insp.nis_rad
    AND DATE(ev.fecha_hora_evento) BETWEEN insp.fecha_ini_os AND insp.fuce
  WHERE insp.nis_rad IS NULL
),

-- ✅ PASO 6: ANÁLISIS PRE/POST EN UNA SOLA PASADA
analisis_consumo_por_evento AS (
  SELECT 
    el.cnt_id,
    el.cups_sgc,
    el.nis_rad,
    DATE_TRUNC('hour', el.fecha_hora_evento) as fecha_hora_evento,
    el.tipo_evento,
    el.tuvo_fraude_previo,
    el.estado_fraude,
    el.dias_desde_fin_anomalia_previa,
    
    -- ========================================
    -- MÉTRICAS PRE-EVENTO (90 días antes)
    -- ========================================
    COUNT(CASE 
      WHEN cb.fh >= el.fecha_hora_evento - INTERVAL '90' DAY 
       AND cb.fh < el.fecha_hora_evento 
      THEN cb.ai 
    END) as registros_pre_90d,
    
    AVG(CASE 
      WHEN cb.fh >= el.fecha_hora_evento - INTERVAL '90' DAY 
       AND cb.fh < el.fecha_hora_evento 
      THEN cb.ai 
    END) as promedio_pre_90d,
    
    APPROX_PERCENTILE(CASE 
      WHEN cb.fh >= el.fecha_hora_evento - INTERVAL '90' DAY 
       AND cb.fh < el.fecha_hora_evento 
      THEN cb.ai 
    END, 0.5) as mediana_pre_90d,
    
    MIN(CASE 
      WHEN cb.fh >= el.fecha_hora_evento - INTERVAL '90' DAY 
       AND cb.fh < el.fecha_hora_evento 
      THEN cb.fh 
    END) as fecha_min_pre,
    
    MAX(CASE 
      WHEN cb.fh >= el.fecha_hora_evento - INTERVAL '90' DAY 
       AND cb.fh < el.fecha_hora_evento 
      THEN cb.fh 
    END) as fecha_max_pre,
    
    -- ========================================
    -- MÉTRICAS POST-EVENTO (90 días después)
    -- ========================================
    COUNT(CASE 
      WHEN cb.fh > el.fecha_hora_evento 
       AND cb.fh <= el.fecha_hora_evento + INTERVAL '90' DAY 
      THEN cb.ai 
    END) as registros_post_90d,
    
    AVG(CASE 
      WHEN cb.fh > el.fecha_hora_evento 
       AND cb.fh <= el.fecha_hora_evento + INTERVAL '90' DAY 
      THEN cb.ai 
    END) as promedio_post_90d,
    
    APPROX_PERCENTILE(CASE 
      WHEN cb.fh > el.fecha_hora_evento 
       AND cb.fh <= el.fecha_hora_evento + INTERVAL '90' DAY 
      THEN cb.ai 
    END, 0.5) as mediana_post_90d,
    
    MIN(CASE 
      WHEN cb.fh > el.fecha_hora_evento 
       AND cb.fh <= el.fecha_hora_evento + INTERVAL '90' DAY 
      THEN cb.fh 
    END) as fecha_min_post,
    
    MAX(CASE 
      WHEN cb.fh > el.fecha_hora_evento 
       AND cb.fh <= el.fecha_hora_evento + INTERVAL '90' DAY 
      THEN cb.fh 
    END) as fecha_max_post
    
  FROM eventos_limpios el
  INNER JOIN consumos_base cb
    ON el.cnt_id = cb.cnt_id
    AND cb.fh BETWEEN el.fecha_hora_evento - INTERVAL '90' DAY 
                  AND el.fecha_hora_evento + INTERVAL '90' DAY
  GROUP BY 
    el.cnt_id, el.cups_sgc, el.nis_rad, 
    DATE_TRUNC('hour', el.fecha_hora_evento), el.tipo_evento,
    el.tuvo_fraude_previo, el.estado_fraude, el.dias_desde_fin_anomalia_previa
),

-- ✅ PASO 7: CÁLCULOS DE FRAUDE Y RANKING
analisis_fraude_final AS (
  SELECT 
    *,
    
    -- Reducciones
    CASE 
      WHEN promedio_pre_90d > 0 THEN 
        ROUND(((promedio_pre_90d - promedio_post_90d) / promedio_pre_90d) * 100, 2)
      ELSE NULL 
    END as reduccion_promedio_pct,
    
    CASE 
      WHEN mediana_pre_90d > 0 THEN 
        ROUND(((mediana_pre_90d - mediana_post_90d) / mediana_pre_90d) * 100, 2)
      ELSE NULL 
    END as reduccion_mediana_pct,
    
    -- Score con penalización por reincidencia
    CASE 
      WHEN promedio_pre_90d > 0 AND mediana_pre_90d > 0 THEN
        ROUND((
          (COALESCE(((promedio_pre_90d - promedio_post_90d) / promedio_pre_90d) * 100, 0)) * 0.5 +
          (COALESCE(((mediana_pre_90d - mediana_post_90d) / mediana_pre_90d) * 100, 0)) * 0.5
        ) * 
        CASE WHEN tuvo_fraude_previo = 'SI' THEN 1.3 ELSE 1.0 END, 2)
      ELSE NULL
    END as score_fraude_combinado,
    
    -- Consistencia
    CASE 
      WHEN ABS(
        COALESCE(((promedio_pre_90d - promedio_post_90d) / promedio_pre_90d) * 100, 0) -
        COALESCE(((mediana_pre_90d - mediana_post_90d) / mediana_pre_90d) * 100, 0)
      ) <= 10 THEN 'CONSISTENTE'
      WHEN ABS(
        COALESCE(((promedio_pre_90d - promedio_post_90d) / promedio_pre_90d) * 100, 0) -
        COALESCE(((mediana_pre_90d - mediana_post_90d) / mediana_pre_90d) * 100, 0)
      ) <= 25 THEN 'MODERADA'
      ELSE 'DIVERGENTE'
    END as consistencia_metricas
    
  FROM analisis_consumo_por_evento
  WHERE registros_pre_90d >= 30  -- Mínimo de datos para análisis confiable
    AND registros_post_90d >= 30
),

-- ✅ PASO 8: RANKING OPTIMIZADO
ranking_final AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY cnt_id 
      ORDER BY 
        score_fraude_combinado DESC,
        reduccion_mediana_pct DESC,
        fecha_hora_evento DESC
    ) as ranking
  FROM analisis_fraude_final
  WHERE score_fraude_combinado IS NOT NULL
    AND score_fraude_combinado >= 5  -- Filtrar casos con score muy bajo
)

-- ========================================
-- RESULTADO FINAL OPTIMIZADO
-- ========================================
SELECT 
  cnt_id,
  cups_sgc,
  nis_rad,
  fecha_hora_evento,
  tipo_evento,
  
  -- Contexto de expedientes
  tuvo_fraude_previo,
  estado_fraude,
  dias_desde_fin_anomalia_previa,
  
  -- Métricas PRE (redondeadas)
  registros_pre_90d,
  ROUND(COALESCE(promedio_pre_90d, 0), 2) as promedio_pre,
  ROUND(COALESCE(mediana_pre_90d, 0), 2) as mediana_pre,
  
  -- Métricas POST (redondeadas)
  registros_post_90d,
  ROUND(COALESCE(promedio_post_90d, 0), 2) as promedio_post,
  ROUND(COALESCE(mediana_post_90d, 0), 2) as mediana_post,
  
  -- Análisis de fraude
  reduccion_promedio_pct,
  reduccion_mediana_pct,
  score_fraude_combinado,
  consistencia_metricas,
  
  -- Clasificación optimizada
  CASE 
    WHEN tuvo_fraude_previo = 'SI' AND score_fraude_combinado >= 30 THEN 'REINCIDENTE_ALTO'
    WHEN tuvo_fraude_previo = 'SI' AND score_fraude_combinado >= 20 THEN 'REINCIDENTE_MEDIO'
    WHEN tuvo_fraude_previo = 'SI' THEN 'REINCIDENTE_BAJO'
    WHEN score_fraude_combinado >= 50 AND consistencia_metricas = 'CONSISTENTE' THEN 'FRAUDE_MUY_PROBABLE'
    WHEN score_fraude_combinado >= 30 AND consistencia_metricas IN ('CONSISTENTE', 'MODERADA') THEN 'FRAUDE_PROBABLE'
    WHEN score_fraude_combinado >= 20 THEN 'SOSPECHOSO'
    WHEN score_fraude_combinado >= 10 THEN 'REVISAR'
    ELSE 'BAJO_RIESGO'
  END as clasificacion_final,
  
  -- Fechas de contexto
  fecha_min_pre,
  fecha_max_pre,
  fecha_min_post,
  fecha_max_post,
  
  -- Información adicional
  ranking
  
FROM ranking_final
WHERE ranking = 1  -- Solo el mejor evento por contador
ORDER BY 
  CASE WHEN tuvo_fraude_previo = 'SI' THEN 0 ELSE 1 END,  -- Reincidentes primero
  score_fraude_combinado DESC,
  reduccion_mediana_pct DESC;