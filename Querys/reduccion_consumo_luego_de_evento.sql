WITH 
-- ✅ SECCIÓN 1: GRID DE CONTADORES CON FILTROS Y EXPEDIENTES
grid_contadores_filtrados AS (
  SELECT 
    grd.cnt_sgc,
    grd.cups_sgc,
    CAST(SUBSTR(grd.cups_sgc, 12, 7) AS INTEGER) AS nis_rad,
    grd.pot_ctto_sgc,
    
    -- ✅ INFORMACIÓN DE EXPEDIENTES PREVIOS
    exp.fecha_inicio_anomalia,
    exp.fecha_fin_anomalia,
    exp.tipo_anomalia,
    exp.estado,  -- ✅ CAMPO CORRECTO
    
    -- ✅ ETIQUETAS DE FRAUDE PREVIO
    CASE 
      WHEN exp.cups IS NOT NULL THEN 'SI'
      ELSE 'NO'
    END as tuvo_fraude_previo,
    
    CASE 
      WHEN exp.fecha_fin_anomalia IS NOT NULL THEN exp.fecha_fin_anomalia
      ELSE NULL
    END as fecha_limite_exclusion,
    
    -- ✅ CLASIFICACIÓN DEL CONTADOR
    CASE 
      WHEN exp.cups IS NULL THEN 'LIMPIO'
      WHEN exp.fecha_fin_anomalia IS NULL THEN 'FRAUDE_ACTIVO'
      WHEN date_parse(exp.fecha_fin_anomalia, '%d/%m/%Y %H:%i:%s') < CURRENT_TIMESTAMP THEN 'FRAUDE_RESUELTO'
      ELSE 'FRAUDE_PENDIENTE'
    END as estado_fraude
    
  FROM master_esir_scada.grid_contadores grd 
  LEFT JOIN master_irregularidades_fraudes.expedientes exp
    ON grd.cups_sgc = exp.cups
  WHERE grd.origen = 'ZEUS'
    AND grd.provincia_sgc IN ('TOLEDO','CIUDAD REAL')
    AND grd.estado_contrato_sgc = 'SRVSTAT001' -- estado del contrato vigente
),

-- ✅ SECCIÓN 2: CONSUMOS DE ESOS CONTADORES (CON FILTRO DE EXPEDIENTES)
consumos_contadores AS (
  SELECT
    s02.cnt_id,
    s02.fh,
    s02.ai,
    grd.cups_sgc,
    grd.nis_rad,
    grd.tuvo_fraude_previo,
    grd.estado_fraude,
    grd.fecha_limite_exclusion
  FROM transformation_esir.s02 s02 
  INNER JOIN grid_contadores_filtrados grd 
    ON s02.cnt_id = grd.cnt_sgc
  WHERE s02.partition_0 IN (
            CAST(YEAR(CURRENT_DATE) AS VARCHAR),
            CAST(YEAR(CURRENT_DATE - INTERVAL '1' YEAR) AS VARCHAR)
        )
        AND s02.fh >= CURRENT_DATE - INTERVAL '15' MONTH
        AND FROM_BASE(s02.bc, 16) < 80
        AND s02.ai BETWEEN 0 AND (grd.pot_ctto_sgc * 1.5)
        -- ✅ FILTRO CLAVE: EXCLUIR CONSUMOS DURANTE ANOMALÍA CONOCIDA
        AND (
          grd.fecha_limite_exclusion IS NULL  -- Sin expedientes previos
          OR s02.fh > date_parse(grd.fecha_limite_exclusion, '%d/%m/%Y %H:%i:%s')  -- O consumos posteriores al fin de anomalía
        )
),

-- ✅ SECCIÓN 3: EVENTOS GRUPO 4 (CON FILTRO DE EXPEDIENTES)
eventos_grupo4_base AS (
  SELECT DISTINCT
    s09.cnt_id,
    s09.fh as fecha_hora_evento,
    s09.et as grupo_evento,
    s09.c as tipo_evento
  FROM transformation_esir.s09 s09
  INNER JOIN grid_contadores_filtrados grd
    ON s09.cnt_id = grd.cnt_sgc
  WHERE s09.et = 4
    AND s09.partition_0 IN (
        CAST(YEAR(CURRENT_DATE) AS VARCHAR),
        CAST(YEAR(CURRENT_DATE - INTERVAL '1' YEAR) AS VARCHAR)
    )
    AND s09.fh >= CURRENT_DATE - INTERVAL '15' MONTH
    -- ✅ FILTRO CLAVE: EXCLUIR EVENTOS DURANTE ANOMALÍA CONOCIDA
    AND (
      grd.fecha_limite_exclusion IS NULL  -- Sin expedientes previos
      OR s09.fh > date_parse(grd.fecha_limite_exclusion, '%d/%m/%Y %H:%i:%s')  -- O eventos posteriores al fin de anomalía
    )
),

-- ✅ SECCIÓN 4: EVENTOS CON INFO DE CONTADORES (INCLUYENDO EXPEDIENTES)
eventos_con_contador_info AS (
  SELECT 
    eg4.*,
    grd.cups_sgc,
    grd.nis_rad,
    grd.tuvo_fraude_previo,
    grd.estado_fraude,
    grd.fecha_limite_exclusion,
    
    -- ✅ INFORMACIÓN CONTEXTUAL
    CASE 
      WHEN grd.fecha_limite_exclusion IS NOT NULL THEN
        date_diff('day', date_parse(grd.fecha_limite_exclusion, '%d/%m/%Y %H:%i:%s'), eg4.fecha_hora_evento)
      ELSE NULL
    END as dias_desde_fin_anomalia_previa
    
  FROM eventos_grupo4_base eg4
  INNER JOIN grid_contadores_filtrados grd
    ON eg4.cnt_id = grd.cnt_sgc
),

-- ✅ SECCIÓN 5: INSPECCIONES (USANDO GRID_CONTADORES YA FILTRADOS)
inspecciones_contadores AS (
  SELECT 
    insp.nis_rad,
    insp.fecha_ini_os,
    insp.fuce,
    insp.tip_os,
    insp.descripcion_os
  FROM transformation_esir.ooss01 insp
  INNER JOIN grid_contadores_filtrados grd
    ON insp.nis_rad = grd.nis_rad  -- ✅ USANDO GRID YA FILTRADO
  WHERE insp.partition_0 IN (
        CAST(YEAR(CURRENT_DATE) AS VARCHAR),
        CAST(YEAR(CURRENT_DATE - INTERVAL '1' YEAR) AS VARCHAR)
    )
    AND insp.fecha_ini_os >= CURRENT_DATE - INTERVAL '15' MONTH
    AND insp.cer = 'ACTSTA0014'
),

-- ✅ SECCIÓN 6: EVENTOS FILTRADOS (SIN INSPECCIONES)
eventos_sin_inspeccion AS (
  SELECT 
    eci.*
  FROM eventos_con_contador_info eci
  LEFT JOIN inspecciones_contadores insp
    ON eci.nis_rad = insp.nis_rad
    AND DATE(eci.fecha_hora_evento) BETWEEN insp.fecha_ini_os AND insp.fuce
  WHERE insp.nis_rad IS NULL  -- ✅ EXCLUYE EVENTOS CON INSPECCIONES
),

-- ✅ SECCIÓN 7: CONSUMOS PRE-EVENTO (CON INFO DE EXPEDIENTES)
consumos_pre_por_evento AS (
  SELECT 
    es.cnt_id,
    es.cups_sgc,
    es.nis_rad,
    DATE_TRUNC('hour', es.fecha_hora_evento) as fecha_hora_evento,
    es.tipo_evento,
    es.tuvo_fraude_previo,
    es.estado_fraude,
    es.dias_desde_fin_anomalia_previa,
    
    COUNT(cc.ai) as registros_pre_90d,
    AVG(cc.ai) as promedio_pre_90d,
    approx_percentile(cc.ai, 0.5) as mediana_pre_90d,
    MIN(cc.fh) as fecha_min_pre,
    MAX(cc.fh) as fecha_max_pre
    
  FROM eventos_sin_inspeccion es
  INNER JOIN consumos_contadores cc
    ON es.cnt_id = cc.cnt_id
    AND cc.fh >= es.fecha_hora_evento - INTERVAL '90' DAY
    AND cc.fh < es.fecha_hora_evento
    AND cc.ai > 0
  GROUP BY es.cnt_id, es.cups_sgc, es.nis_rad, DATE_TRUNC('hour', es.fecha_hora_evento), 
           es.tipo_evento, es.tuvo_fraude_previo, es.estado_fraude, es.dias_desde_fin_anomalia_previa
),

-- ✅ SECCIÓN 8: CONSUMOS POST-EVENTO
consumos_post_por_evento AS (
  SELECT 
    es.cnt_id,
    es.cups_sgc,
    es.nis_rad,
    DATE_TRUNC('hour', es.fecha_hora_evento) as fecha_hora_evento,
    es.tipo_evento,
    
    COUNT(cc.ai) as registros_post_90d,
    AVG(cc.ai) as promedio_post_90d,
    approx_percentile(cc.ai, 0.5) as mediana_post_90d,
    MIN(cc.fh) as fecha_min_post,
    MAX(cc.fh) as fecha_max_post
    
  FROM eventos_sin_inspeccion es
  INNER JOIN consumos_contadores cc
    ON es.cnt_id = cc.cnt_id
    AND cc.fh > es.fecha_hora_evento
    AND cc.fh <= es.fecha_hora_evento + INTERVAL '90' DAY
    AND cc.ai > 0
  GROUP BY es.cnt_id, es.cups_sgc, es.nis_rad, DATE_TRUNC('hour', es.fecha_hora_evento), es.tipo_evento
),

-- ✅ SECCIÓN 9: ANÁLISIS DE FRAUDE (CON CONTEXTO DE EXPEDIENTES)
analisis_fraude AS (
  SELECT 
    pre.cnt_id,
    pre.cups_sgc,
    pre.nis_rad,
    pre.fecha_hora_evento,
    pre.tipo_evento,
    
    -- ✅ CONTEXTO DE EXPEDIENTES
    pre.tuvo_fraude_previo,
    pre.estado_fraude,
    pre.dias_desde_fin_anomalia_previa,
    
    -- Datos PRE
    pre.registros_pre_90d,
    pre.promedio_pre_90d,
    pre.mediana_pre_90d,
    pre.fecha_min_pre,
    pre.fecha_max_pre,
    
    -- Datos POST
    post.registros_post_90d,
    post.promedio_post_90d,
    post.mediana_post_90d,
    post.fecha_min_post,
    post.fecha_max_post,
    
    -- Reducciones
    CASE 
      WHEN pre.promedio_pre_90d > 0 THEN 
        ROUND(((pre.promedio_pre_90d - post.promedio_post_90d) / pre.promedio_pre_90d) * 100, 2)
      ELSE NULL 
    END as reduccion_promedio_pct,
    
    CASE 
      WHEN pre.mediana_pre_90d > 0 THEN 
        ROUND(((pre.mediana_pre_90d - post.mediana_post_90d) / pre.mediana_pre_90d) * 100, 2)
      ELSE NULL 
    END as reduccion_mediana_pct,
    
    -- ✅ Score con penalización por reincidencia
    CASE 
      WHEN pre.promedio_pre_90d > 0 AND pre.mediana_pre_90d > 0 THEN
        ROUND((
          (COALESCE(((pre.promedio_pre_90d - post.promedio_post_90d) / pre.promedio_pre_90d) * 100, 0)) * 0.5 +
          (COALESCE(((pre.mediana_pre_90d - post.mediana_post_90d) / pre.mediana_pre_90d) * 100, 0)) * 0.5
        ) * 
        -- ✅ MULTIPLICADOR POR REINCIDENCIA
        CASE 
          WHEN pre.tuvo_fraude_previo = 'SI' THEN 1.3  -- 30% más grave si ya tuvo fraude
          ELSE 1.0
        END, 2)
      ELSE NULL
    END as score_fraude_combinado,
    
    -- Consistencia
    CASE 
      WHEN ABS(
        COALESCE(((pre.promedio_pre_90d - post.promedio_post_90d) / pre.promedio_pre_90d) * 100, 0) -
        COALESCE(((pre.mediana_pre_90d - post.mediana_post_90d) / pre.mediana_pre_90d) * 100, 0)
      ) <= 10 THEN 'CONSISTENTE'
      WHEN ABS(
        COALESCE(((pre.promedio_pre_90d - post.promedio_post_90d) / pre.promedio_pre_90d) * 100, 0) -
        COALESCE(((pre.mediana_pre_90d - post.mediana_post_90d) / pre.mediana_pre_90d) * 100, 0)
      ) <= 25 THEN 'MODERADA'
      ELSE 'DIVERGENTE'
    END as consistencia_metricas
    
  FROM consumos_pre_por_evento pre
  INNER JOIN consumos_post_por_evento post 
    ON pre.cnt_id = post.cnt_id 
    AND pre.fecha_hora_evento = post.fecha_hora_evento
),

-- ✅ SECCIÓN 10: RANKING POR CONTADOR
ranking_por_contador AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY cnt_id 
      ORDER BY 
        score_fraude_combinado DESC,
        reduccion_mediana_pct DESC,
        fecha_hora_evento DESC
    ) as ranking
  FROM analisis_fraude
  WHERE score_fraude_combinado IS NOT NULL
)

-- ========================================
-- RESULTADO FINAL CON EXPEDIENTES
-- ========================================
SELECT 
  cnt_id,
  cups_sgc,
  nis_rad,
  fecha_hora_evento,
  tipo_evento,
  
  -- ✅ CONTEXTO DE EXPEDIENTES PREVIOS
  tuvo_fraude_previo,
  estado_fraude,
  dias_desde_fin_anomalia_previa,
  
  -- Métricas PRE
  registros_pre_90d,
  ROUND(promedio_pre_90d, 2) as promedio_pre,
  ROUND(mediana_pre_90d, 2) as mediana_pre,
  
  -- Métricas POST
  registros_post_90d,
  ROUND(promedio_post_90d, 2) as promedio_post,
  ROUND(mediana_post_90d, 2) as mediana_post,
  
  -- Análisis de fraude
  reduccion_promedio_pct,
  reduccion_mediana_pct,
  score_fraude_combinado,
  consistencia_metricas,
  
  -- ✅ Clasificación especial para reincidentes
  CASE 
    WHEN tuvo_fraude_previo = 'SI' AND score_fraude_combinado >= 30 THEN 'REINCIDENTE_ALTO'
    WHEN tuvo_fraude_previo = 'SI' AND score_fraude_combinado >= 20 THEN 'REINCIDENTE_MEDIO'
    WHEN score_fraude_combinado >= 50 AND consistencia_metricas = 'CONSISTENTE' THEN 'FRAUDE_MUY_PROBABLE'
    WHEN score_fraude_combinado >= 30 AND consistencia_metricas IN ('CONSISTENTE', 'MODERADA') THEN 'FRAUDE_PROBABLE'
    WHEN score_fraude_combinado >= 20 THEN 'SOSPECHOSO'
    WHEN score_fraude_combinado >= 10 THEN 'REVISAR'
    ELSE 'BAJO_RIESGO'
  END as clasificacion_final,
  
  -- Fechas
  fecha_min_pre,
  fecha_max_pre,
  fecha_min_post,
  fecha_max_post
  
FROM ranking_por_contador
WHERE ranking = 1
ORDER BY 
  tuvo_fraude_previo DESC,  -- ✅ Primero los reincidentes
  score_fraude_combinado DESC,
  reduccion_mediana_pct DESC;