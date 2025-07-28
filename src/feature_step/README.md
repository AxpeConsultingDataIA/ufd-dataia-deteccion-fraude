# Sistema de Carga UFD a Neo4j

Este proyecto permite cargar datos UFD desde archivos CSV a una base de datos Neo4j de manera optimizada.

## 🚀 Características

- **Carga optimizada**: Procesamiento en lotes con gestión inteligente de memoria
- **Manejo robusto de errores**: Logging detallado y recuperación ante fallos
- **Configuración flexible**: Esquemas y configuraciones centralizadas
- **Constraints e índices automáticos**: Creación inteligente de estructura en Neo4j
- **Múltiples formatos de fecha**: Soporte para diferentes formatos timestamp

## 📋 Prerequisitos

- Python 3.8+
- Apache Spark 3.4+
- Neo4j 5.x
- Java 8 o 11 (requerido por Spark)

## 🔧 Instalación

1. **Clonar el repositorio**:
```bash
git clone <tu-repositorio>
cd ufd-neo4j-loader
```

2. **Instalar dependencias**:
```bash
pip install -r requirements.txt
```

3. **Configurar Neo4j**:
   - Edita `config.py` y actualiza `NEO4J_CONFIG`:
   ```python
   NEO4J_CONFIG = {
       "uri": "bolt://localhost:7687",
       "user": "neo4j", 
       "password": "tu_password_aqui"
   }
   ```

## 📁 Estructura de Archivos

```
src
├── feature_step/
    ├── main.py                     # Script principal
    ├── functional_code.py          # Funciones de carga y procesamiento
    ├── neo4j_manager.py            # Clase para gestión de Neo4j
    ├── config.py                   # Configuraciones y esquemas
    ├── requirements.txt            # Dependencias
    └── carga_tablas.log            # Log de ejecución (se genera automáticamente)
```

## 📊 Archivos CSV Esperados

El sistema espera encontrar estos archivos en la carpeta de entrada:

- `s09.csv` - Eventos
- `s02.csv` - Curva horaria  
- `ooss01.csv` - Inspecciones
- `grid_contadores.csv` - Grid de contadores
- `denuncias.csv` - Denuncias
- `expedientes.csv` - Expedientes
- `grupo_eventos_codigos.csv` - Códigos de eventos

## 🏃‍♂️ Uso

### ✅ **Comando básico (Solo carga datos):**
```bash
# Solo carga datos a Neo4j (mantiene BD existente)
python main.py -i /ruta/a/tus/csvs
```

### 🔥 **Comando para borrar + cargar:**
```bash
# Borra BD completamente y carga datos frescos
python main.py -i /ruta/a/tus/csvs --borrar-neo4j

python main.py -i C:/Users/gmtorrealbac/Documents/AXPE_2025/UFD/src/feature_step/datasets -m "train" -l "DEBUG" --borrar-neo4j
```

### 🔧 **Opciones avanzadas:**
```bash
# Con logging detallado y batch size personalizado
python main.py -i /ruta/a/tus/csvs -l DEBUG --batch-size 1000

# Borrar + cargar con configuración personalizada
python main.py -i /ruta/a/tus/csvs --borrar-neo4j -l DEBUG --batch-size 500

# Ver ayuda completa
python main.py --help
```

### 📋 **Parámetros disponibles:**

| Parámetro | Descripción | Ejemplo |
|-----------|-------------|---------|
| `-i, --input` | 📂 Carpeta con archivos CSV (obligatorio) | `-i /datos/csv/` |
| `-m, --mode` | 🎯 Modo de ejecución | `-m train` (por defecto) |
| `-l, --loglevel` | 📊 Nivel de logging | `-l DEBUG` |
| `--batch-size` | 📦 Tamaño de lote para Neo4j | `--batch-size 1000` |
| `--borrar-neo4j` | 🔥 Borrar BD antes de cargar | `--borrar-neo4j` |

## 🔄 **Comportamiento según parámetros:**

| Comando | Comportamiento |
|---------|---------------|
| `python main.py -i /datos` | 📊 **Solo carga** datos (mantiene BD) |
| `python main.py -i /datos --borrar-neo4j` | 🔥 **Borra BD** + 📊 **carga datos** |

## 🚀 **Tus comandos específicos:**

```bash
# OPCIÓN 1: Solo cargar datos (mantener BD existente)
python main.py -i C:/Users/gmtorrealbac/Documents/AXPE_2025/UFD/src/feature_step/datasets -m train -l DEBUG

# OPCIÓN 2: Borrar todo + cargar datos frescos
python main.py -i C:/Users/gmtorrealbac/Documents/AXPE_2025/UFD/src/feature_step/datasets -m train -l DEBUG --borrar-neo4j
```

## 🔍 Logs y Monitoreo

El sistema genera logs detallados en:
- **Consola**: Información en tiempo real
- **Archivo**: `carga_tablas.log` (histórico completo)

### Interpretar los logs:
```
✅ - Operación exitosa
❌ - Error crítico
⚠️  - Advertencia
🔍 - Información de debug
📦 - Procesamiento de lotes
🎉 - Proceso completado
```

## 🗃️ Estructura en Neo4j

### Nodos creados:
- `Evento` - Eventos del sistema
- `Curva_horaria` - Datos de curva horaria
- `Inspecciones` - Inspecciones realizadas
- `Grid_contadores` - Grid de contadores
- `Denuncias` - Denuncias registradas
- `Expedientes` - Expedientes
- `Grupo_eventos_codigos` - Códigos de eventos

### Características automáticas:
- **Constraints únicos**: Evitan duplicados
- **Índices optimizados**: Mejoran el rendimiento de consultas
- **Timestamps automáticos**: `created_at` y `updated_at` en cada nodo

## 🛠️ Configuración Avanzada

### Personalizar esquemas:
Edita `config.py` en la sección `DATA_CONFIG` para:
- Cambiar tipos de datos
- Añadir nuevas columnas
- Modificar formatos de fecha

### Ajustar configuración Neo4j:
Modifica las configuraciones `*_CONFIG` en `config.py` para:
- Cambiar campos de constraint
- Añadir nuevos índices
- Personalizar nombres de nodos

## 🚨 Troubleshooting

### Error: "No se pudo conectar a Neo4j"
- ✅ Verificar que Neo4j esté ejecutándose
- ✅ Comprobar URI, usuario y contraseña en `config.py`
- ✅ Verificar conectividad de red

### Error: "DataFrame no encontrado"
- ✅ Verificar que los archivos CSV existan en la carpeta indicada
- ✅ Comprobar nombres de archivos (distingue mayúsculas/minúsculas)
- ✅ Verificar permisos de lectura

### Error: "Out of Memory"
- ✅ Reducir `--batch-size` (probar con 250 o 100)
- ✅ Aumentar memoria JVM: `export SPARK_DRIVER_MEMORY=4g`
- ✅ Procesar archivos más pequeños

### Performance lenta:
- ✅ Aumentar `--batch-size` si hay suficiente memoria
- ✅ Verificar índices en Neo4j
- ✅ Usar SSD para archivos de datos

## 📝 Ejemplos de Uso

### ✅ **Carga normal (mantener BD):**
```bash
# Solo carga datos nuevos/actualizados
python main.py -i /datos/ufd -l INFO

# Con batch size optimizado
python main.py -i /datos/ufd --batch-size 1000
```

### 🔥 **Carga completa (BD limpia):**
```bash
# Borra todo + carga datos frescos
python main.py -i /datos/ufd --borrar-neo4j -l DEBUG

# Para producción con batch grande
python main.py -i /datos/ufd --borrar-neo4j --batch-size 2000
```

### 🧪 **Para desarrollo/testing:**
```bash
# Testing con datos pequeños
python main.py -i /datos/ufd_test --batch-size 100 -l DEBUG

# Verificar estructura sin borrar
python main.py -i /datos/ufd -l INFO
```

### 📊 **Monitoreo:**
```bash
# En una terminal: ejecutar proceso
python main.py -i /datos/ufd --borrar-neo4j -l DEBUG

# En otra terminal: seguir logs
tail -f carga_tablas.log
```

## 🤝 Contribuir

1. Hacer fork del proyecto
2. Crear branch para feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit cambios (`git commit -m 'Añadir nueva funcionalidad'`)
4. Push al branch (`git push origin feature/nueva-funcionalidad`)
5. Crear Pull Request

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver archivo `LICENSE` para detalles.

---

**⚠️ Importante**: Siempre hacer backup de tu base de datos Neo4j antes de usar `--borrar-neo4j`.


# 🔗 Ejemplos de Uso con Relaciones

## 🚀 Comandos Principales

### ✅ **Carga completa (datos + relaciones):**
```bash
# Borrar BD + cargar datos + crear relaciones
python main.py -i /ruta/datos --borrar-neo4j --crear-relaciones

python main.py -i C:/Users/gmtorrealbac/Documents/AXPE_2025/UFD/src/feature_step/datasets -m "train" -l "DEBUG" --borrar-neo4j --crear-relaciones

# Con validaciones completas
python main.py -i /ruta/datos --borrar-neo4j --crear-relaciones --validar-nodos -l DEBUG
```

### 📊 **Solo cargar datos (sin relaciones):**
```bash
# Mantener BD existente + solo agregar datos
python main.py -i /ruta/datos -l INFO

# Borrar BD + solo cargar datos (sin relaciones)
python main.py -i /ruta/datos --borrar-neo4j
```

### 🔗 **Solo crear/actualizar relaciones:**
```bash
# Solo crear relaciones (datos ya existen)
python main.py -i /ruta/datos --solo-relaciones

# Limpiar relaciones existentes + crear nuevas
python main.py -i /ruta/datos --solo-relaciones --limpiar-relaciones

# Solo relaciones específicas
python main.py -i /ruta/datos --solo-relaciones --relaciones-especificas INSPECCION_EN_GRID EVENTO_EN_CONTADOR
```

### 🧪 **Para desarrollo/testing:**
```bash
# Validar que existen todos los nodos necesarios
python main.py -i /ruta/datos --solo-relaciones --validar-nodos

# Crear relaciones con logging detallado
python main.py -i /ruta/datos --solo-relaciones -l DEBUG --validar-nodos
```

## 📋 **Nuevos Parámetros Disponibles**

| Parámetro | Descripción | Ejemplo |
|-----------|-------------|---------|
| `--crear-relaciones` | 🔗 Crear relaciones después de cargar datos | `--crear-relaciones` |
| `--solo-relaciones` | 🔗 Solo crear relaciones (sin cargar datos) | `--solo-relaciones` |
| `--limpiar-relaciones` | 🧹 Limpiar relaciones antes de crear | `--limpiar-relaciones` |
| `--relaciones-especificas` | 🎯 Crear solo relaciones específicas | `--relaciones-especificas EVENTO_EN_CONTADOR` |
| `--validar-nodos` | ✅ Validar nodos antes de crear relaciones | `--validar-nodos` |

## 🔄 **Flujos de Trabajo Recomendados**

### 🏁 **Primera vez / Producción:**
```bash
# 1. Carga completa desde cero
python main.py -i /ruta/datos --borrar-neo4j --crear-relaciones --validar-nodos -l INFO

# Resultado: BD limpia + datos + relaciones + validaciones
```

### 🔄 **Actualización de datos:**
```bash
# 1. Solo actualizar datos (mantener relaciones)
python main.py -i /ruta/datos -l INFO

# 2. Recrear solo las relaciones si es necesario
python main.py -i /ruta/datos --solo-relaciones --limpiar-relaciones
```

### 🐛 **Debug / Desarrollo:**
```bash
# 1. Verificar qué datos y relaciones existen
python main.py -i /ruta/datos --solo-relaciones --validar-nodos -l DEBUG

# 2. Probar relaciones específicas
python main.py -i /ruta/datos --solo-relaciones --relaciones-especificas INSPECCION_EN_GRID -l DEBUG

# 3. Limpiar y recrear todo en modo debug
python main.py -i /ruta/datos --borrar-neo4j --crear-relaciones -l DEBUG
```

### 🔧 **Mantenimiento:**
```bash
# Limpiar solo relaciones (mantener datos)
python main.py -i /ruta/datos --solo-relaciones --limpiar-relaciones

# Recrear todas las relaciones
python main.py -i /ruta/datos --solo-relaciones --limpiar-relaciones --validar-nodos
```

## 🔗 **Tipos de Relaciones Disponibles**

| Relación | Descripción | Conecta |
|----------|-------------|---------|
| `INSPECCION_EN_GRID` | Inspecciones por NIS | Inspecciones ↔ Grid_contadores |
| `EVENTO_EN_CONTADOR` | Eventos por CNC_ID | Eventos ↔ Grid_contadores |
| `CURVA_EN_CONTADOR` | Curva horaria por CNC_ID | Curva_horaria ↔ Grid_contadores |
| `DENUNCIA_EN_CUPS` | Denuncias por CUPS | Denuncias ↔ Grid_contadores |
| `EXPEDIENTE_EN_CUPS` | Expedientes por CUPS | Expedientes ↔ Grid_contadores |
| `EVENTO_CON_CODIGO` | Eventos con códigos | Eventos ↔ Grupo_eventos_codigos |
| `INSPECCION_SIMULTANEA` | Inspecciones mismo día | Inspecciones ↔ Inspecciones |
| `EVENTO_CONSECUTIVO` | Eventos consecutivos | Eventos ↔ Eventos |

## 🎯 **Ejemplos Específicos por Caso de Uso**

### 📊 **Análisis de datos (solo lectura):**
```bash
# Verificar estado actual sin cambios
python main.py -i /ruta/datos --solo-relaciones --validar-nodos
```

### 🔄 **Actualización incremental:**
```bash
# 1. Cargar nuevos datos
python main.py -i /ruta/datos_nuevos -l INFO

# 2. Actualizar solo relaciones afectadas
python main.py -i /ruta/datos --solo-relaciones --relaciones-especificas EVENTO_EN_CONTADOR CURVA_EN_CONTADOR
```

### 🧪 **Testing específico:**
```bash
# Probar solo relaciones de inspecciones
python main.py -i /ruta/datos --solo-relaciones --relaciones-especificas INSPECCION_EN_GRID INSPECCION_SIMULTANEA -l DEBUG

# Verificar eventos y códigos
python main.py -i /ruta/datos --solo-relaciones --relaciones-especificas EVENTO_CON_CODIGO EVENTO_CONSECUTIVO -l DEBUG
```

### 🚨 **Recuperación de errores:**
```bash
# Si falló la carga de relaciones, solo reintentarlas
python main.py -i /ruta/datos --solo-relaciones --limpiar-relaciones --validar-nodos -l DEBUG

# Si falló todo, empezar desde cero
python main.py -i /ruta/datos --borrar-neo4j --crear-relaciones --validar-nodos -l DEBUG
```

## 📈 **Interpretación de Logs de Relaciones**

```
🔗 Creando relación: EVENTO_EN_CONTADOR
📝 Descripción: Relaciona eventos con contadores por CNC_ID
✅ EVENTO_EN_CONTADOR completada:
   🔗 Relaciones creadas: 15,432
   📋 Propiedades establecidas: 30,864
   ⏱️ Tiempo: 12.3s
```

- **🔗 Relaciones creadas**: Número de conexiones establecidas entre nodos
- **📋 Propiedades establecidas**: Incluye metadatos como `created_at`, `tipo_relacion`, etc.
- **⏱️ Tiempo**: Duración de la operación

### ⚠️ **Casos de advertencia:**
```
⚠️ EVENTO_CON_CODIGO: No se crearon relaciones
   Esto puede ser normal si no hay datos que coincidan
```
- Es normal si los datos no tienen campos coincidentes
- Verificar que los nodos de origen y destino existan

### ❌ **Casos de error:**
```
❌ Error creando relación INSPECCION_EN_GRID: Syntax error
```
- Revisar la query en `config.py`
- Verificar que los nombres de campos sean correctos

## 🔍 **Validación y Troubleshooting**

### **Verificar estado antes de actuar:**
```bash
# Ver qué nodos existen
python main.py -i /ruta/datos --solo-relaciones --validar-nodos -l INFO

# Ver qué relaciones ya existen
python main.py -i /ruta/datos --solo-relaciones -l INFO
```

### **Problemas comunes y soluciones:**

#### 🚫 **Error: "Faltan algunos tipos de nodo"**
```bash
# Problema: Intentas crear relaciones sin tener los datos cargados
# Solución: Cargar datos primero
python main.py -i /ruta/datos -m train
# Luego crear relaciones
python main.py -i /ruta/datos --solo-relaciones
```

#### 🐌 **Relaciones muy lentas**
```bash
# Problema: Queries complejas sin índices
# Solución: Los índices se crean automáticamente, pero puedes verificar:
python main.py -i /ruta/datos --validar-nodos -l DEBUG
```

#### 📭 **"No se crearon relaciones"**
```bash
# Problema: Datos no coinciden entre tablas
# Solución: Verificar datos específicos
python main.py -i /ruta/datos --solo-relaciones --relaciones-especificas NOMBRE_RELACION -l DEBUG
```

#### 🔄 **Relaciones duplicadas**
```bash
# Problema: Ejecutar varias veces sin limpiar
# Solución: Limpiar antes de recrear
python main.py -i /ruta/datos --solo-relaciones --limpiar-relaciones
```

## 🎛️ **Personalización Avanzada**

### **Crear relaciones personalizadas:**
Edita `config.py` y añade a `RELACIONES_CONFIG`:

```python
{
    "nombre": "MI_RELACION_CUSTOM",
    "descripcion": "Descripción de mi relación",
    "query": """
    MATCH (a:TipoA), (b:TipoB)
    WHERE a.campo = b.campo
    CREATE (a)-[:MI_RELACION {
        created_at: datetime(),
        tipo_relacion: 'custom'
    }]->(b)
    """
}
```

### **Ejecutar solo tu relación:**
```bash
python main.py -i /ruta/datos --solo-relaciones --relaciones-especificas MI_RELACION_CUSTOM
```

## 📊 **Monitoreo de Performance**

### **Logs detallados:**
```bash
# Ver todo el proceso paso a paso
python main.py -i /ruta/datos --crear-relaciones -l DEBUG

# Seguir logs en tiempo real
tail -f carga_tablas.log | grep "🔗\|✅\|❌"
```

### **Métricas importantes:**
- **Tiempo por relación**: Normal 5-30s, revisar si >60s
- **Relaciones creadas**: Debe ser >0 si hay datos coincidentes
- **Memoria**: Vigilar uso si hay muchas relaciones

## 🚀 **Comandos de Producción Recomendados**

### **Setup inicial completo:**
```bash
python main.py \
  -i /ruta/datos \
  --borrar-neo4j \
  --crear-relaciones \
  --validar-nodos \
  --batch-size 1000 \
  -l INFO
```

### **Actualización de datos:**
```bash
# 1. Actualizar datos
python main.py -i /ruta/datos_nuevos --batch-size 1000 -l INFO

# 2. Recrear relaciones afectadas
python main.py \
  -i /ruta/datos \
  --solo-relaciones \
  --limpiar-relaciones \
  --validar-nodos \
  -l INFO
```

### **Mantenimiento semanal:**
```bash
# Verificar integridad y recrear relaciones
python main.py \
  -i /ruta/datos \
  --solo-relaciones \
  --limpiar-relaciones \
  --validar-nodos \
  -l INFO
```

## 🔗 **Consultas Neo4j Útiles Post-Carga**

Una vez creadas las relaciones, puedes usar estas consultas en Neo4j Browser:

### **Ver resumen de relaciones:**
```cypher
MATCH ()-[r]->()
RETURN type(r) as TipoRelacion, count(r) as Cantidad
ORDER BY Cantidad DESC
```

### **Encontrar nodos más conectados:**
```cypher
MATCH (n)-[r]-()
RETURN labels(n) as TipoNodo, count(r) as TotalConexiones, n
ORDER BY TotalConexiones DESC
LIMIT 10
```

### **Verificar integridad de datos:**
```cypher
// Nodos sin relaciones
MATCH (n)
WHERE NOT (n)-[]-()
RETURN labels(n) as TipoNodo, count(n) as NodosSinRelaciones
```

### **Análisis específico por contador:**
```cypher
MATCH (g:Grid_contadores)-[r]-(other)
WHERE g.nis_rad_sgc = "123456"
RETURN g, r, other
```

---

💡 **Tip**: Usa `--validar-nodos` siempre en producción para detectar problemas antes de crear relaciones.

🚨 **Importante**: Siempre haz backup antes de usar `--borrar-neo4j` o `--limpiar-relaciones`.