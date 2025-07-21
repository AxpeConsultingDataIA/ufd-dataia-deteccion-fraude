# Base de Datos Neo4j - Sistema de Contadores Eléctricos

## Descripción General

Esta base de datos Neo4j está diseñada para gestionar un sistema completo de contadores eléctricos, incluyendo mediciones, eventos, fraudes e inspecciones. El modelo de datos utiliza un enfoque de grafos que permite relaciones complejas entre las diferentes entidades del sistema.

## Estructura del Modelo de Datos

### Nodos Principales

#### 🔌 CONTADOR
Representa los contadores eléctricos instalados en el sistema.

**Propiedades clave:**
- `nis_rad` (único): Identificador único del contador
- `numero_contador`: Número físico del contador
- `marca_contador`, `modelo_contador`: Información del fabricante
- `potencia_maxima`, `tension`: Características técnicas
- `telegest_activo`: Estado de telegestión
- `estado_contrato`: Estado actual del contrato

#### ⚡ SUMINISTRO
Puntos de suministro eléctrico asociados a los contadores.

**Propiedades clave:**
- `nis_rad` (único): Identificador único del suministro
- `tarifa_activa`: Tarifa eléctrica aplicada
- `potencia_contratada`: Potencia contratada por el cliente
- `estado_contrato`: Estado del contrato de suministro
- `comercializadora_codigo`: Código de la empresa comercializadora

#### 🏢 COMERCIALIZADORA
Empresas comercializadoras de energía eléctrica.

**Propiedades:**
- `codigo_comercializadora` (único): Código identificativo
- `nombre_comercializadora`: Nombre de la empresa

#### 📡 CONCENTRADOR
Dispositivos que concentran y gestionan las comunicaciones con los contadores.

**Propiedades:**
- `concentrador_id` (único): Identificador del concentrador
- `estado_comunicacion`: Estado de las comunicaciones
- `version_concentrador`: Versión del firmware

#### 🚨 EXPEDIENTE_FRAUDE
Expedientes de investigación de fraudes detectados en el sistema.

**Propiedades importantes:**
- `nis_expediente` (único): Identificador del expediente
- `tipo_anomalia`: Tipo de anomalía detectada
- `clasificacion_fraude`: Clasificación del fraude
- `energia_liquidable`: Energía a liquidar por el fraude
- `valoracion_total`: Valoración económica total

#### 📍 UBICACION
Ubicaciones geográficas de los contadores e instalaciones.

**Propiedades:**
- `coordenada_x`, `coordenada_y`: Coordenadas geográficas
- `codigo_postal`: Código postal de la ubicación
- `area_ejecucion`: Área de ejecución asignada

#### 🔍 INSPECCION
Órdenes de trabajo e inspecciones realizadas en el sistema.

**Propiedades:**
- `numero_os` (único): Número de orden de servicio
- `fecha_alta`, `fecha_estado`: Fechas de gestión
- `empresa_contratista`: Empresa que realiza la inspección
- `estado_os`: Estado actual de la orden

#### 📊 MEDICION
Mediciones de consumo eléctrico registradas por los contadores.

**Propiedades:**
- `timestamp_medicion`: Timestamp de la medición
- `energia_activa`, `energia_inductiva`: Valores de energía
- `resistencia_r1`, `resistencia_r2`, `resistencia_r3`, `resistencia_r4`: Resistencias medidas

#### 📝 EVENTO
Eventos del sistema generados por los concentradores.

**Propiedades:**
- `timestamp_evento`: Timestamp del evento
- `tipo_reporte`: Tipo de reporte generado
- `version_evento`: Versión del evento

### Relaciones Principales

```cypher
// Relaciones de CONTADOR
(:CONTADOR)-[:MIDE_CONSUMO_DE]->(:SUMINISTRO)
(:CONTADOR)-[:CONECTADO_A]->(:CONCENTRADOR)
(:CONTADOR)-[:GENERA_MEDICION]->(:MEDICION)
(:CONTADOR)-[:INVOLUCRADO_EN_FRAUDE]->(:EXPEDIENTE_FRAUDE)
(:CONTADOR)-[:INSTALADO_EN]->(:UBICACION)

// Relaciones de CONCENTRADOR
(:CONCENTRADOR)-[:GENERA_MEDICION]->(:MEDICION)
(:CONCENTRADOR)-[:GENERA_EVENTO]->(:EVENTO)

// Relaciones de EXPEDIENTE_FRAUDE
(:EXPEDIENTE_FRAUDE)-[:RELACIONADO_CON]->(:EXPEDIENTE_FRAUDE)
(:EXPEDIENTE_FRAUDE)-[:AFECTA_A]->(:COMERCIALIZADORA)
```

## Archivos del Repositorio

- `schema.cypher`: Definición completa del esquema de la base de datos
- `sample_queries.cypher`: Consultas de ejemplo para operaciones comunes
- `README.md`: Esta documentación

## Instalación y Configuración

### Prerrequisitos
- Neo4j Community/Enterprise Edition 4.4+
- Acceso a la base de datos con permisos de escritura

### Creación del Esquema

```bash
# Ejecutar el archivo de esquema
cypher-shell -u <usuario> -p <contraseña> -f schema.cypher
```

### Verificación de la Instalación

```cypher
// Verificar que las constraints se han creado correctamente
SHOW CONSTRAINTS;

// Verificar que los índices se han creado correctamente
SHOW INDEXES;
```

## Consultas Típicas

### Contadores Activos por Comercializadora
```cypher
MATCH (c:CONTADOR)-[:MIDE_CONSUMO_DE]->(s:SUMINISTRO)
WHERE s.estado_contrato = "ACTIVO"
RETURN s.comercializadora_codigo, count(c) as total_contadores
ORDER BY total_contadores DESC;
```

### Fraudes por Tipo de Anomalía
```cypher
MATCH (e:EXPEDIENTE_FRAUDE)
RETURN e.tipo_anomalia, count(*) as total_expedientes, 
       sum(e.valoracion_total) as valoracion_total
ORDER BY total_expedientes DESC;
```

### Mediciones Recientes por Concentrador
```cypher
MATCH (con:CONCENTRADOR)-[:GENERA_MEDICION]->(m:MEDICION)
WHERE m.timestamp_medicion >= datetime() - duration('P7D')
RETURN con.concentrador_id, count(m) as total_mediciones
ORDER BY total_mediciones DESC;
```

## Consideraciones de Rendimiento

- Los índices están optimizados para consultas frecuentes por fecha, estado y ubicación
- Las consultas geográficas utilizan índices en `coordenada_x` y `coordenada_y`
- Para consultas complejas con múltiples relaciones, considere usar `PROFILE` para optimizar

## Mantenimiento

### Limpieza de Datos Antiguos
```cypher
// Eliminar mediciones anteriores a 1 año (ejecutar con precaución)
MATCH (m:MEDICION)
WHERE m.timestamp_medicion < datetime() - duration('P365D')
DELETE m;
```

### Estadísticas de la Base de Datos
```cypher
// Contar nodos por tipo
MATCH (n)
RETURN labels(n) as tipo_nodo, count(*) as total
ORDER BY total DESC;
```

## Contacto y Soporte

Para preguntas sobre la estructura de datos o consultas específicas, contacte al equipo de desarrollo.

---

**Nota**: Esta documentación debe actualizarse cuando se realicen cambios en el esquema de la base de datos.