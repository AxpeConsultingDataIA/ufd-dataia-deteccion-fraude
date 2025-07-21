// ========================================
// CONSULTAS DE EJEMPLO - SISTEMA CONTADORES ELÉCTRICOS
// ========================================

// ========================================
// CONSULTAS BÁSICAS DE CONTEO Y ESTADÍSTICAS
// ========================================

// Contar todos los nodos por tipo
MATCH (n)
RETURN labels(n)[0] as tipo_nodo, count(*) as total
ORDER BY total DESC;

// Contadores activos por estado
MATCH (c:CONTADOR)
RETURN c.estado_contrato, count(*) as total_contadores
ORDER BY total_contadores DESC;

// Distribución de contadores por marca
MATCH (c:CONTADOR)
RETURN c.marca_contador, count(*) as total, 
       collect(DISTINCT c.modelo_contador) as modelos
ORDER BY total DESC;

// ========================================
// CONSULTAS DE FRAUDES Y ANOMALÍAS
// ========================================

// Top 10 expedientes de fraude por valoración
MATCH (e:EXPEDIENTE_FRAUDE)
RETURN e.nis_expediente, e.tipo_anomalia, e.clasificacion_fraude, 
       e.valoracion_total, e.energia_liquidable
ORDER BY e.valoracion_total DESC
LIMIT 10;

// Fraudes agrupados por tipo de anomalía
MATCH (e:EXPEDIENTE_FRAUDE)
RETURN e.tipo_anomalia, 
       count(*) as total_expedientes,
       sum(e.valoracion_total) as valoracion_total,
       avg(e.valoracion_total) as valoracion_promedio,
       sum(e.energia_liquidable) as energia_total
ORDER BY total_expedientes DESC;

// Contadores involucrados en fraudes
MATCH (c:CONTADOR)-[:INVOLUCRADO_EN_FRAUDE]->(e:EXPEDIENTE_FRAUDE)
RETURN c.nis_rad, c.marca_contador, c.modelo_contador,
       count(e) as total_fraudes,
       sum(e.valoracion_total) as valoracion_total_fraudes
ORDER BY total_fraudes DESC;

// Fraudes por mes (últimos 12 meses)
MATCH (e:EXPEDIENTE_FRAUDE)
WHERE e.fecha_acta >= date() - duration('P12M')
RETURN date.truncate('month', e.fecha_acta) as mes,
       count(*) as total_fraudes,
       sum(e.valoracion_total) as valoracion_mes
ORDER BY mes DESC;

// ========================================
// CONSULTAS DE MEDICIONES Y CONSUMO
// ========================================

// Últimas mediciones por concentrador
MATCH (con:CONCENTRADOR)-[:GENERA_MEDICION]->(m:MEDICION)
WITH con, m
ORDER BY m.timestamp_medicion DESC
WITH con, collect(m)[0] as ultima_medicion
RETURN con.concentrador_id, 
       ultima_medicion.timestamp_medicion as ultima_lectura,
       ultima_medicion.energia_activa
ORDER BY ultima_lectura DESC;

// Mediciones por temporada
MATCH (m:MEDICION)
WHERE m.timestamp_medicion >= datetime() - duration('P30D')
RETURN m.temporada, 
       count(*) as total_mediciones,
       avg(m.energia_activa) as energia_promedio,
       sum(m.energia_activa) as energia_total
ORDER BY total_mediciones DESC;

// Contadores con mayor consumo (últimas 24h)
MATCH (c:CONTADOR)-[:GENERA_MEDICION]->(m:MEDICION)
WHERE m.timestamp_medicion >= datetime() - duration('PT24H')
WITH c, sum(m.energia_activa) as consumo_total
RETURN c.nis_rad, c.marca_contador, consumo_total
ORDER BY consumo_total DESC
LIMIT 20;

// ========================================
// CONSULTAS GEOGRÁFICAS Y UBICACIONES
// ========================================

// Contadores por área de ejecución
MATCH (c:CONTADOR)-[:INSTALADO_EN]->(u:UBICACION)
RETURN u.area_ejecucion, 
       count(c) as total_contadores,
       collect(DISTINCT c.estado_contrato) as estados
ORDER BY total_contadores DESC;

// Contadores en un radio específico (ejemplo: coordenadas aprox.)
MATCH (c:CONTADOR)-[:INSTALADO_EN]->(u:UBICACION)
WHERE u.coordenada_x BETWEEN 100 AND 200 
  AND u.coordenada_y BETWEEN 100 AND 200
RETURN c.nis_rad, u.coordenada_x, u.coordenada_y, u.codigo_postal;

// Inspecciones por ubicación
MATCH (i:INSPECCION)
WHERE i.coordenada_x IS NOT NULL AND i.coordenada_y IS NOT NULL
RETURN i.coordenada_x, i.coordenada_y, 
       count(*) as total_inspecciones,
       collect(DISTINCT i.estado_os) as estados
ORDER BY total_inspecciones DESC;

// ========================================
// CONSULTAS DE CONCENTRADORES Y COMUNICACIONES
// ========================================

// Estado de comunicaciones de concentradores
MATCH (con:CONCENTRADOR)
RETURN con.estado_comunicacion, 
       count(*) as total_concentradores,
       collect(con.version_concentrador) as versiones
ORDER BY total_concentradores DESC;

// Concentradores con más contadores conectados
MATCH (con:CONCENTRADOR)<-[:CONECTADO_A]-(c:CONTADOR)
RETURN con.concentrador_id, con.estado_comunicacion,
       count(c) as contadores_conectados
ORDER BY contadores_conectados DESC;

// Eventos generados por concentrador (última semana)
MATCH (con:CONCENTRADOR)-[:GENERA_EVENTO]->(e:EVENTO)
WHERE e.timestamp_evento >= datetime() - duration('P7D')
RETURN con.concentrador_id, 
       count(e) as total_eventos,
       collect(DISTINCT e.tipo_reporte) as tipos_reporte
ORDER BY total_eventos DESC;

// ========================================
// CONSULTAS DE COMERCIALIZADORAS
// ========================================

// Suministros por comercializadora
MATCH (s:SUMINISTRO)
WHERE s.comercializadora_codigo IS NOT NULL
RETURN s.comercializadora_codigo, 
       count(*) as total_suministros,
       collect(DISTINCT s.estado_contrato) as estados,
       sum(s.potencia_contratada) as potencia_total
ORDER BY total_suministros DESC;

// Comercializadoras afectadas por fraudes
MATCH (e:EXPEDIENTE_FRAUDE)-[:AFECTA_A]->(com:COMERCIALIZADORA)
RETURN com.nombre_comercializadora, com.codigo_comercializadora,
       count(e) as total_expedientes_fraude,
       sum(e.valoracion_total) as valoracion_total_fraudes
ORDER BY total_expedientes_fraude DESC;

// ========================================
// CONSULTAS DE INSPECCIONES Y ÓRDENES DE TRABAJO
// ========================================

// Inspecciones por estado
MATCH (i:INSPECCION)
RETURN i.estado_os, 
       count(*) as total_inspecciones,
       collect(DISTINCT i.empresa_contratista) as empresas
ORDER BY total_inspecciones DESC;

// Inspecciones pendientes más antiguas
MATCH (i:INSPECCION)
WHERE i.estado_os IN ['PENDIENTE', 'EN_PROCESO', 'ASIGNADA']
RETURN i.numero_os, i.motivo, i.fecha_alta, i.estado_os, i.empresa_contratista
ORDER BY i.fecha_alta ASC
LIMIT 20;

// Rendimiento por empresa contratista
MATCH (i:INSPECCION)
WHERE i.empresa_contratista IS NOT NULL
RETURN i.empresa_contratista,
       count(*) as total_inspecciones,
       count(CASE WHEN i.estado_os = 'FINALIZADA' THEN 1 END) as finalizadas,
       round(100.0 * count(CASE WHEN i.estado_os = 'FINALIZADA' THEN 1 END) / count(*), 2) as porcentaje_finalizadas
ORDER BY total_inspecciones DESC;

// ========================================
// CONSULTAS COMPLEJAS Y ANÁLISIS AVANZADO
// ========================================

// Análisis completo por contador (incluye mediciones, fraudes, ubicación)
MATCH (c:CONTADOR)
OPTIONAL MATCH (c)-[:MIDE_CONSUMO_DE]->(s:SUMINISTRO)
OPTIONAL MATCH (c)-[:GENERA_MEDICION]->(m:MEDICION)
WHERE m.timestamp_medicion >= datetime() - duration('P30D')
OPTIONAL MATCH (c)-[:INVOLUCRADO_EN_FRAUDE]->(e:EXPEDIENTE_FRAUDE)
OPTIONAL MATCH (c)-[:INSTALADO_EN]->(u:UBICACION)
RETURN c.nis_rad, c.marca_contador, c.estado_contrato,
       s.tarifa_activa, u.area_ejecucion,
       count(DISTINCT m) as mediciones_ultimo_mes,
       count(DISTINCT e) as total_fraudes,
       sum(e.valoracion_total) as valoracion_fraudes
ORDER BY valoracion_fraudes DESC NULLS LAST;

// Correlación entre tipo de contador y fraudes
MATCH (c:CONTADOR)-[:INVOLUCRADO_EN_FRAUDE]->(e:EXPEDIENTE_FRAUDE)
WITH c.marca_contador as marca, c.modelo_contador as modelo,
     count(e) as total_fraudes,
     sum(e.valoracion_total) as valoracion_total
MATCH (c2:CONTADOR)
WHERE c2.marca_contador = marca AND c2.modelo_contador = modelo
WITH marca, modelo, total_fraudes, valoracion_total, count(c2) as total_contadores
RETURN marca, modelo, total_contadores, total_fraudes,
       round(100.0 * total_fraudes / total_contadores, 2) as porcentaje_fraude,
       valoracion_total
ORDER BY porcentaje_fraude DESC;

// Patrones de consumo anómalos (desviaciones significativas)
MATCH (c:CONTADOR)-[:GENERA_MEDICION]->(m:MEDICION)
WHERE m.timestamp_medicion >= datetime() - duration('P30D')
WITH c, avg(m.energia_activa) as consumo_promedio, 
     stdev(m.energia_activa) as desviacion_estandar,
     count(m) as total_mediciones
WHERE total_mediciones > 10 AND desviacion_estandar > consumo_promedio * 0.5
RETURN c.nis_rad, c.marca_contador, 
       round(consumo_promedio, 2) as consumo_promedio,
       round(desviacion_estandar, 2) as desviacion,
       total_mediciones
ORDER BY desviacion DESC
LIMIT 20;

// ========================================
// CONSULTAS DE MANTENIMIENTO Y LIMPIEZA
// ========================================

// Identificar nodos huérfanos (sin relaciones)
MATCH (n)
WHERE NOT (n)--()
RETURN labels(n)[0] as tipo_nodo, count(*) as nodos_huerfanos
ORDER BY nodos_huerfanos DESC;

// Verificar integridad referencial básica
MATCH (c:CONTADOR)
WHERE c.nis_rad IS NULL OR c.numero_contador IS NULL
RETURN "Contadores con datos faltantes" as problema, count(*) as total
UNION
MATCH (s:SUMINISTRO)
WHERE s.nis_rad IS NULL
RETURN "Suministros sin NIS" as problema, count(*) as total;

// ========================================
// CONSULTAS DE RENDIMIENTO Y MONITOREO
// ========================================

// Tamaño de la base de datos por tipo de nodo
CALL apoc.meta.stats() YIELD labels, relTypesCount
RETURN labels, relTypesCount;

// Últimas actividades en la base de datos
MATCH (m:MEDICION)
WITH max(m.timestamp_medicion) as ultima_medicion
MATCH (e:EVENTO)
WITH ultima_medicion, max(e.timestamp_evento) as ultimo_evento
RETURN ultima_medicion, ultimo_evento,
       duration.between(ultima_medicion, datetime()) as tiempo_desde_ultima_medicion,
       duration.between(ultimo_evento, datetime()) as tiempo_desde_ultimo_evento;