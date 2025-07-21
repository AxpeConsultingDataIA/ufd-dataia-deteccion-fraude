// Neo4j Database Schema
// Estructura de la base de datos de contadores eléctricos

// ========================================
// CONSTRAINTS AND INDEXES
// ========================================

// Unique constraints
CREATE CONSTRAINT contador_nis_unique IF NOT EXISTS FOR (c:CONTADOR) REQUIRE c.nis_rad IS UNIQUE;
CREATE CONSTRAINT suministro_nis_unique IF NOT EXISTS FOR (s:SUMINISTRO) REQUIRE s.nis_rad IS UNIQUE;
CREATE CONSTRAINT comercializadora_codigo_unique IF NOT EXISTS FOR (c:COMERCIALIZADORA) REQUIRE c.codigo_comercializadora IS UNIQUE;
CREATE CONSTRAINT concentrador_id_unique IF NOT EXISTS FOR (c:CONCENTRADOR) REQUIRE c.concentrador_id IS UNIQUE;
CREATE CONSTRAINT expediente_nis_unique IF NOT EXISTS FOR (e:EXPEDIENTE_FRAUDE) REQUIRE e.nis_expediente IS UNIQUE;
CREATE CONSTRAINT expediente_dg_unique IF NOT EXISTS FOR (e:EXPEDIENTE_FRAUDE) REQUIRE e.dg IS UNIQUE;
CREATE CONSTRAINT inspeccion_os_unique IF NOT EXISTS FOR (i:INSPECCION) REQUIRE i.numero_os IS UNIQUE;

// Indexes for performance
CREATE INDEX contador_marca_idx IF NOT EXISTS FOR (c:CONTADOR) ON (c.marca_contador);
CREATE INDEX contador_modelo_idx IF NOT EXISTS FOR (c:CONTADOR) ON (c.modelo_contador);
CREATE INDEX suministro_estado_idx IF NOT EXISTS FOR (s:SUMINISTRO) ON (s.estado_contrato);
CREATE INDEX expediente_tipo_idx IF NOT EXISTS FOR (e:EXPEDIENTE_FRAUDE) ON (e.tipo_anomalia);
CREATE INDEX expediente_clasificacion_idx IF NOT EXISTS FOR (e:EXPEDIENTE_FRAUDE) ON (e.clasificacion_fraude);
CREATE INDEX ubicacion_x_idx IF NOT EXISTS FOR (u:UBICACION) ON (u.coordenada_x);
CREATE INDEX ubicacion_y_idx IF NOT EXISTS FOR (u:UBICACION) ON (u.coordenada_y);
CREATE INDEX inspeccion_fecha_idx IF NOT EXISTS FOR (i:INSPECCION) ON (i.fecha_estado);
CREATE INDEX medicion_timestamp_idx IF NOT EXISTS FOR (m:MEDICION) ON (m.timestamp_medicion);
CREATE INDEX evento_timestamp_idx IF NOT EXISTS FOR (e:EVENTO) ON (e.timestamp_evento);

// ========================================
// NODE LABELS STRUCTURE
// ========================================

// CONTADOR - Contadores eléctricos
// Properties: potencia_maxima, fecha_instalacion, marca_contador, estado_contrato, 
//            fases_contador, estado_tg, modelo_contador, ct_gmo, nis_rad, 
//            numero_contador, ct_odi, tipo_aparato, telegest_activo, tension, version_firmware

// SUMINISTRO - Puntos de suministro eléctrico
// Properties: tarifa_activa, cnae, fecha_alta_suministro, comercializadora_codigo, 
//            nis_rad, potencia_maxima, tension_suministro, tipo_punto, 
//            potencia_contratada, fases_suministro, estado_contrato

// COMERCIALIZADORA - Empresas comercializadoras de energía
// Properties: codigo_comercializadora, nombre_comercializadora

// CONCENTRADOR - Dispositivos concentradores de datos
// Properties: estado_comunicacion, concentrador_id, version_concentrador, tipo_reporte

// EXPEDIENTE_FRAUDE - Expedientes de fraudes detectados
// Properties: metodo_estimacion, nis_expediente, numero_factura, codigo_comercializadora,
//            estado_expediente, tipo_anomalia, fecha_acta, fecha_inicio_anomalia,
//            clasificacion_fraude, energia_liquidable, porcentaje_liquidable, dg,
//            valoracion_total, dias_liquidables, irregularidades_detectadas,
//            energia_facturada, fecha_fin_anomalia

// UBICACION - Ubicaciones geográficas
// Properties: codigo_postal, coordenada_x, coordenada_y, area_ejecucion

// INSPECCION - Órdenes de trabajo e inspecciones
// Properties: fecha_alta, tipo_os, numero_os, coordenada_x, precio_os, coordenada_y,
//            motivo, nis_inspeccion, empresa_contratista, codigo_operario, proceso,
//            estado_os, fecha_estado

// MEDICION - Mediciones de consumo eléctrico
// Properties: timestamp_medicion, resistencia_r2, energia_inductiva, concentrador_id,
//            resistencia_r1, temporada, resistencia_r4, resistencia_r3, energia_activa

// EVENTO - Eventos del sistema
// Properties: timestamp_evento, concentrador_id, temporada, tipo_reporte,
//            version_evento, id_peticion

// ========================================
// RELATIONSHIPS
// ========================================

// CONTADOR relationships:
// (:CONTADOR)-[:MIDE_CONSUMO_DE]->(:SUMINISTRO)
// (:CONTADOR)-[:CONECTADO_A]->(:CONCENTRADOR)
// (:CONTADOR)-[:GENERA_MEDICION]->(:MEDICION)
// (:CONTADOR)-[:INVOLUCRADO_EN_FRAUDE]->(:EXPEDIENTE_FRAUDE)
// (:CONTADOR)-[:INSTALADO_EN]->(:UBICACION)

// CONCENTRADOR relationships:
// (:CONCENTRADOR)-[:GENERA_MEDICION]->(:MEDICION)
// (:CONCENTRADOR)-[:GENERA_EVENTO]->(:EVENTO)

// EXPEDIENTE_FRAUDE relationships:
// (:EXPEDIENTE_FRAUDE)-[:RELACIONADO_CON]->(:EXPEDIENTE_FRAUDE)
// (:EXPEDIENTE_FRAUDE)-[:AFECTA_A]->(:COMERCIALIZADORA)

// ========================================
// EXAMPLE DATA STRUCTURE QUERIES
// ========================================

// Create sample nodes structure (commented out - use as reference)
/*
// Example CONTADOR
CREATE (c:CONTADOR {
    nis_rad: "ES0000000000000001",
    numero_contador: "CTR001",
    marca_contador: "CIRCUTOR",
    modelo_contador: "CVM-1D",
    potencia_maxima: 15000,
    tension: 400,
    fases_contador: "TRIFASICO",
    telegest_activo: true,
    estado_contrato: "ACTIVO",
    estado_tg: "ACTIVO",
    fecha_instalacion: date("2023-01-15"),
    version_firmware: "1.2.3"
});

// Example SUMINISTRO
CREATE (s:SUMINISTRO {
    nis_rad: "ES0000000000000001",
    tarifa_activa: "3.0A",
    potencia_contratada: 15000,
    potencia_maxima: 15000,
    tension_suministro: 400,
    fases_suministro: "TRIFASICO",
    tipo_punto: "INDUSTRIAL",
    estado_contrato: "ACTIVO",
    fecha_alta_suministro: date("2023-01-10"),
    comercializadora_codigo: "COM001",
    cnae: "2511"
});

// Create relationship
MATCH (c:CONTADOR {nis_rad: "ES0000000000000001"})
MATCH (s:SUMINISTRO {nis_rad: "ES0000000000000001"})
CREATE (c)-[:MIDE_CONSUMO_DE]->(s);
*/