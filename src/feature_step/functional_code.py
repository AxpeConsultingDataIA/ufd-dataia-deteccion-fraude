import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, to_date, col, from_unixtime
from config import DATA_CONFIG
from neo4j import GraphDatabase
import time
from neo4j_manager import Neo4jTableManager
from neo4j_relations_manager import Neo4jRelationsManager
from config import RELACIONES_CONFIG, VALIDACIONES_RELACIONES

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("carga_tablas.log"),
        logging.StreamHandler()
    ]
)

def validar_neo4j_config(config):
    required_keys = {"uri", "user", "password"}
    missing = required_keys - set(config.keys())
    if missing:
        logging.error(f"Faltan claves en NEO4J_CONFIG: {missing}")
        return False
    if not all(config[k] for k in required_keys):
        logging.error("Algún valor de NEO4J_CONFIG está vacío.")
        return False
    return True

def borrar_toda_bd_neo4j(uri, usuario, password, confirmar=False, borrar_props=False):
    """
    Borra COMPLETAMENTE todos los datos de la base de datos Neo4j.
    ADVERTENCIA: Esta operación es IRREVERSIBLE y eliminará:
    - Todos los nodos
    - Todas las relaciones
    - Todos los índices
    - Todas las constraints

    Args:
        uri (str): URI de conexión a Neo4j (ej: "bolt://localhost:7687")
        usuario (str): Usuario de Neo4j
        password (str): Contraseña de Neo4j
        confirmar (bool): DEBE ser True para ejecutar el borrado

    Returns:
        dict: Resumen de la operación
    """
    if not confirmar:
        logging.error("OPERACIÓN CANCELADA. Usa confirmar=True para ejecutar el borrado.")
        return {"status": "cancelado", "mensaje": "Operación no confirmada"}

    logging.info("INICIANDO BORRADO COMPLETO DE BASE DE DATOS NEO4J")
    resultados = {
        "nodos_eliminados": 0,
        "relaciones_eliminadas": 0,
        "indices_eliminados": 0,
        "constraints_eliminadas": 0,
        "tiempo_total": 0,
        "errores": []
    }

    driver = None
    try:
        driver = GraphDatabase.driver(uri, auth=(usuario, password))
        inicio = time.time()
        with driver.session() as session:

            # OPCIONAL: Borrar solo propiedades si se solicita
            if borrar_props:
                try:
                    eliminadas_nodos = session.run("""
                        MATCH (n)
                        WITH n
                        CALL {
                            WITH n
                            WITH keys(n) AS props, n
                            UNWIND props AS p
                            REMOVE n[p]
                        }
                        RETURN count(n) as nodos_afectados
                    """).single()["nodos_afectados"]

                    eliminadas_rels = session.run("""
                        MATCH ()-[r]->()
                        WITH r
                        CALL {
                            WITH r
                            WITH keys(r) AS props, r
                            UNWIND props AS p
                            REMOVE r[p]
                        }
                        RETURN count(r) as rels_afectados
                    """).single()["rels_afectados"]

                    logging.info(f"Propiedades eliminadas: {eliminadas_nodos} nodos y {eliminadas_rels} relaciones")

                except Exception as e:
                    error_msg = f"Error borrando propiedades: {str(e)}"
                    resultados["errores"].append(error_msg)
                    logging.error(error_msg)

            # 1. Eliminar todas las relaciones
            try:
                resultado = session.run("MATCH ()-[r]-() DELETE r RETURN count(r) as eliminadas")
                relaciones = resultado.single()["eliminadas"]
                resultados["relaciones_eliminadas"] = relaciones
                logging.info(f" {relaciones:,} relaciones eliminadas")
            except Exception as e:
                error_msg = f"Error eliminando relaciones: {str(e)}"
                resultados["errores"].append(error_msg)
                logging.error(error_msg)

            # 2. Eliminar todos los nodos en lotes
            try:
                batch_size = 10000
                total_nodos = 0
                while True:
                    resultado = session.run(f"""
                        MATCH (n)
                        WITH n LIMIT {batch_size}
                        DELETE n
                        RETURN count(n) as eliminados
                    """)
                    batch_eliminados = resultado.single()["eliminados"]
                    total_nodos += batch_eliminados
                    if batch_eliminados == 0:
                        break
                    logging.info(f"Lote: {batch_eliminados:,} nodos eliminados (Total: {total_nodos:,})")
                resultados["nodos_eliminados"] = total_nodos
                logging.info(f"Total: {total_nodos:,} nodos eliminados")
            except Exception as e:
                error_msg = f"Error eliminando nodos: {str(e)}"
                resultados["errores"].append(error_msg)
                logging.error(error_msg)

            # 3. Eliminar todos los índices
            try:
                indices = session.run("SHOW INDEXES").data()
                indices_eliminados = 0
                for indice in indices:
                    nombre_indice = indice.get('name', 'unknown')
                    try:
                        session.run(f"DROP INDEX {nombre_indice}")
                        indices_eliminados += 1
                        logging.info(f"Índice eliminado: {nombre_indice}")
                    except Exception as e:
                        logging.warning(f"No se pudo eliminar índice {nombre_indice}: {str(e)}")
                resultados["indices_eliminados"] = indices_eliminados
                logging.info(f" {indices_eliminados} índices eliminados")
            except Exception as e:
                error_msg = f"Error eliminando índices: {str(e)}"
                resultados["errores"].append(error_msg)
                logging.error(error_msg)

            # 4. Eliminar todas las constraints
            try:
                constraints = session.run("SHOW CONSTRAINTS").data()
                constraints_eliminadas = 0
                for constraint in constraints:
                    nombre_constraint = constraint.get('name', 'unknown')
                    try:
                        session.run(f"DROP CONSTRAINT {nombre_constraint}")
                        constraints_eliminadas += 1
                        logging.info(f"Constraint eliminada: {nombre_constraint}")
                    except Exception as e:
                        logging.warning(f"No se pudo eliminar constraint {nombre_constraint}: {str(e)}")
                resultados["constraints_eliminadas"] = constraints_eliminadas
                logging.info(f"{constraints_eliminadas} constraints eliminadas")
            except Exception as e:
                error_msg = f"Error eliminando constraints: {str(e)}"
                resultados["errores"].append(error_msg)
                logging.error(error_msg)

            # 5. Verificación final
            try:
                resultado_nodos = session.run("MATCH (n) RETURN count(n) as total")
                nodos_restantes = resultado_nodos.single()["total"]
                resultado_rels = session.run("MATCH ()-[r]-() RETURN count(r) as total")
                rels_restantes = resultado_rels.single()["total"]
                logging.info(f"Nodos restantes: {nodos_restantes}")
                logging.info(f"Relaciones restantes: {rels_restantes}")
                if nodos_restantes == 0 and rels_restantes == 0:
                    logging.info("BASE DE DATOS COMPLETAMENTE LIMPIA")
                else:
                    logging.warning("Algunos elementos no se pudieron eliminar")
            except Exception as e:
                error_msg = f"Error en verificación final: {str(e)}"
                resultados["errores"].append(error_msg)
                logging.error(error_msg)

        fin = time.time()
        resultados["tiempo_total"] = round(fin - inicio, 2)
    except Exception as e:
        error_msg = f"Error de conexión: {str(e)}"
        resultados["errores"].append(error_msg)
        logging.error(error_msg)
    finally:
        if driver:
            driver.close()
            logging.info("Conexión cerrada")

    return resultados

from pyspark.sql.types import StringType, LongType
from pyspark.sql.functions import col, to_date, to_timestamp, from_unixtime

def load_raw_training_data(input_config=DATA_CONFIG):
    spark = SparkSession.builder.getOrCreate()
    for name, cfg in input_config.items():
        try:
            logging.info(f"Cargando tabla: {name} desde {cfg['path']}")
            df = spark.read.csv(
                cfg["path"],
                header=True,
                sep=";",
                schema=cfg["schema"],
                encoding="utf-8"
            )

            logging.info(f"Tabla {name} cargada correctamente con {df.count()} filas y {len(df.columns)} columnas")

            # Procesar columnas de fecha
            date_columns = cfg.get("date_columns", [])
            date_format = cfg.get("date_format", None)

            for date_col in date_columns:
                if date_col in df.columns:
                    # Buscar el tipo declarado en el schema
                    schema_field = next((f for f in cfg["schema"].fields if f.name == date_col), None)
                    if schema_field:
                        field_type = type(schema_field.dataType).__name__

                        # Si es LongType, convertir desde epoch a timestamp
                        if field_type == "LongType":
                            df = df.withColumn(date_col, to_timestamp(from_unixtime(col(date_col))))
                            logging.info(f"Columna {date_col} convertida desde epoch a timestamp")

                        # Si es StringType, convertir usando date_format
                        elif field_type == "StringType":
                            if date_format:
                                if any(x in date_format for x in ["H", "m", "s"]):
                                    df = df.withColumn(date_col, to_timestamp(col(date_col), date_format))
                                    logging.info(f"Columna {date_col} convertida a timestamp con formato {date_format}")
                                else:
                                    df = df.withColumn(date_col, to_date(col(date_col), date_format))
                                    logging.info(f"Columna {date_col} convertida a date con formato {date_format}")
                            else:
                                df = df.withColumn(date_col, to_date(col(date_col)))
                                logging.info(f"Columna {date_col} convertida a date con formato por defecto")

            # Guardar en variable global
            globals()[f"df_{name}"] = df
            logging.info(f"Esquema de df_{name}:")
            df.printSchema()

        except Exception as e:
            logging.error(f"Error al cargar tabla {name}: {e}", exc_info=True)

    return {
        f"df_{name}": globals()[f"df_{name}"]
        for name in input_config.keys()
        if f"df_{name}" in globals()
    }

def procesar_tablas_neo4j(tablas_neo4j, dfs, neo4j_config):
    """
    Procesa las tablas usando la configuración tablas_neo4j
    
    Args:
        tablas_neo4j: Diccionario con la configuración de tablas
        dfs: Diccionario con los DataFrames de Spark
        neo4j_config: Configuración de conexión a Neo4j
    """
    # Importar las configuraciones desde config
    from config import (
        DENUNCIAS_CONFIG, EVENTOS_CONFIG, EXPEDIENTES_CONFIG, 
        INSPECCIONES_CONFIG, GRID_CONTADORES_CONFIG, 
        GRUPO_EVENTOS_CODIGOS_CONFIG, CURVA_HORARIA_CONFIG
    )
    
    # Diccionario para resolver nombres de configuración
    config_map = {
        "DENUNCIAS_CONFIG": DENUNCIAS_CONFIG,
        "EVENTOS_CONFIG": EVENTOS_CONFIG,
        "EXPEDIENTES_CONFIG": EXPEDIENTES_CONFIG,
        "INSPECCIONES_CONFIG": INSPECCIONES_CONFIG,
        "GRID_CONTADORES_CONFIG": GRID_CONTADORES_CONFIG,
        "GRUPO_EVENTOS_CODIGOS_CONFIG": GRUPO_EVENTOS_CODIGOS_CONFIG,
        "CURVA_HORARIA_CONFIG": CURVA_HORARIA_CONFIG
    }
    
    resultados = {}
    
    logging.info(f"Iniciando procesamiento de {len(tablas_neo4j)} tablas...")
    logging.info("=" * 60)
    
    for tabla_key, tabla_info in tablas_neo4j.items():
        dataframe_name = tabla_info["dataframe"]
        nombre_neo4j = tabla_info["nombre_neo4j"] 
        config_name = tabla_info["config"]
        
        logging.info(f"Procesando: {tabla_key}")
        logging.info(f"DataFrame: {dataframe_name}")
        logging.info(f"Nodo Neo4j: {nombre_neo4j}")
        logging.info(f"Config: {config_name}")
        
        try:
            # Obtener el DataFrame
            df = dfs.get(dataframe_name)
            if df is None:
                logging.error(f"DataFrame {dataframe_name} no encontrado en dfs")
                resultados[tabla_key] = {
                    "exito": False,
                    "error": f"DataFrame {dataframe_name} no encontrado",
                    "nombre_neo4j": nombre_neo4j
                }
                continue
            
            # Obtener la configuración usando el diccionario
            config = config_map.get(config_name)
            if config is None:
                logging.error(f"Configuración {config_name} no encontrada en config_map")
                resultados[tabla_key] = {
                    "exito": False,
                    "error": f"Configuración {config_name} no encontrada", 
                    "nombre_neo4j": nombre_neo4j
                }
                continue
            
            # Verificar filas del DataFrame
            filas = df.count()
            logging.info(f"Filas: {filas:,}")
            
            if filas == 0:
                logging.warning(f"DataFrame vacío, saltando...")
                resultados[tabla_key] = {
                    "exito": False,
                    "error": "DataFrame vacío",
                    "nombre_neo4j": nombre_neo4j,
                    "filas": 0
                }
                continue
            
            # Crear instancia del manager con NEO4J_CONFIG
            manager = Neo4jTableManager(neo4j_config)
            
            # Procesar la tabla
            logging.info(f"Iniciando carga a Neo4j...")
            resultado = manager.cargar_dataframe_a_neo4j_optimizado(df, config)
            
            resultados[tabla_key] = {
                "exito": True,
                "resultado": resultado,
                "nombre_neo4j": nombre_neo4j,
                "filas_originales": filas,
                "dataframe_name": dataframe_name
            }
            
            logging.info(f"{tabla_key} procesado exitosamente")
            logging.info(f"Nodos creados: {resultado.get('total_creados', 0):,}")
            
        except Exception as e:
            logging.error(f"Error procesando {tabla_key}: {e}")
            resultados[tabla_key] = {
                "exito": False,
                "error": str(e),
                "nombre_neo4j": nombre_neo4j,
                "dataframe_name": dataframe_name
            }
    
    # Resumen final
    logging.info("\n" + "=" * 60)
    logging.info("RESUMEN FINAL")
    logging.info("=" * 60)
    
    exitosos = sum(1 for r in resultados.values() if r["exito"])
    fallidos = len(resultados) - exitosos
    total_nodos = sum(r.get("resultado", {}).get("total_creados", 0) for r in resultados.values() if r["exito"])
    
    logging.info(f"Tablas procesadas: {len(resultados)}")
    logging.info(f"Exitosas: {exitosos}")
    logging.info(f"Fallidas: {fallidos}")
    logging.info(f"Total nodos creados: {total_nodos:,}")
    
    if fallidos > 0:
        logging.warning(f"Tablas con errores:")
        for tabla_key, resultado in resultados.items():
            if not resultado["exito"]:
                logging.error(f"{tabla_key}: {resultado['error']}")
    
    return resultados

# Función principal para crear todas las relaciones
def crear_todas_las_relaciones(neo4j_config, relaciones_a_crear=None, validar_antes=True):
    """
    Crear todas las relaciones entre nodos en Neo4j
    
    Args:
        neo4j_config: Configuración de conexión a Neo4j
        relaciones_a_crear: Lista específica de relaciones a crear. Si es None, crea todas.
        validar_antes: Si verificar nodos existentes antes de crear relaciones
        
    Returns:
        Dict con resumen de la operación
    """
    logging.info("INICIANDO PROCESO DE CREACIÓN DE RELACIONES")
    logging.info("="*60)
    
    # Crear instancia del manager de relaciones
    relations_manager = Neo4jRelationsManager(neo4j_config)
    
    # Validaciones previas si está habilitado
    if validar_antes and VALIDACIONES_RELACIONES.get("verificar_nodos_antes_relaciones", True):
        logging.info("Verificando relaciones existentes antes de crear nuevas...")
        estado_previo = relations_manager.verificar_relaciones_existentes()
        
        if "error" in estado_previo:
            logging.error(f"Error verificando estado previo: {estado_previo['error']}")
            return {"error": "No se pudo verificar estado previo", "detalles": estado_previo}
        
        if VALIDACIONES_RELACIONES.get("mostrar_estadisticas_previas", True):
            logging.info("Estado previo de relaciones:")
            for rel in estado_previo.get('relaciones_existentes', []):
                logging.info(f"  {rel['tipo']}: {rel['cantidad']} relaciones")
    
    # Determinar qué relaciones crear
    if relaciones_a_crear is None:
        relaciones_config = RELACIONES_CONFIG
        logging.info(f"Creando TODAS las relaciones ({len(relaciones_config)} tipos)")
    else:
        # Filtrar solo las relaciones solicitadas
        relaciones_config = [r for r in RELACIONES_CONFIG if r['nombre'] in relaciones_a_crear]
        logging.info(f"Creando relaciones específicas: {relaciones_a_crear}")
        
        if not relaciones_config:
            logging.error("No se encontraron relaciones válidas para crear")
            return {"error": "Relaciones no encontradas", "solicitadas": relaciones_a_crear}
    
    # Crear las relaciones
    resultado = relations_manager.crear_todas_las_relaciones(relaciones_config)
    
    # Verificación posterior
    if resultado.get('exitosas', 0) > 0:
        logging.info("Verificando estado posterior...")
        estado_posterior = relations_manager.verificar_relaciones_existentes()
        
        if "error" not in estado_posterior:
            logging.info("Estado posterior de relaciones:")
            for rel in estado_posterior.get('relaciones_existentes', []):
                logging.info(f"  {rel['tipo']}: {rel['cantidad']} relaciones")
            
            resultado['estado_posterior'] = estado_posterior
    
    return resultado

def limpiar_relaciones_especificas(neo4j_config, tipos_relacion=None):
    """
    Limpiar relaciones específicas de Neo4j
    
    Args:
        neo4j_config: Configuración de conexión a Neo4j
        tipos_relacion: Lista de tipos de relación a eliminar. Si es None, elimina todas.
        
    Returns:
        Dict con resultado de la operación
    """
    logging.info("INICIANDO LIMPIEZA DE RELACIONES")
    logging.info("="*40)
    
    relations_manager = Neo4jRelationsManager(neo4j_config)
    
    # Verificar estado antes
    estado_previo = relations_manager.verificar_relaciones_existentes()
    
    if "error" not in estado_previo:
        logging.info("Relaciones antes de la limpieza:")
        for rel in estado_previo.get('relaciones_existentes', []):
            logging.info(f"  {rel['tipo']}: {rel['cantidad']} relaciones")
    
    # Realizar limpieza
    resultado_limpieza = relations_manager.limpiar_relaciones(tipos_relacion)
    
    # Verificar estado después
    if resultado_limpieza.get('exito', False):
        estado_posterior = relations_manager.verificar_relaciones_existentes()
        
        if "error" not in estado_posterior:
            logging.info("Relaciones después de la limpieza:")
            for rel in estado_posterior.get('relaciones_existentes', []):
                logging.info(f"  {rel['tipo']}: {rel['cantidad']} relaciones")
            
            resultado_limpieza['estado_posterior'] = estado_posterior
    
    return resultado_limpieza

def validar_nodos_para_relaciones(neo4j_config, tipos_nodo_requeridos):
    """
    Validar que existan los nodos necesarios antes de crear relaciones
    
    Args:
        neo4j_config: Configuración de conexión a Neo4j
        tipos_nodo_requeridos: Lista de tipos de nodo que deben existir
        
    Returns:
        Dict con resultado de la validación
    """
    relations_manager = Neo4jRelationsManager(neo4j_config)
    driver = relations_manager.conectar_neo4j()
    
    if not driver:
        return {"error": "No se pudo conectar a Neo4j"}
    
    try:
        with driver.session() as session:
            nodos_existentes = {}
            nodos_faltantes = []
            
            for tipo_nodo in tipos_nodo_requeridos:
                query = f"MATCH (n:{tipo_nodo}) RETURN count(n) as cantidad"
                resultado = session.run(query)
                cantidad = resultado.single()['cantidad']
                
                nodos_existentes[tipo_nodo] = cantidad
                
                if cantidad == 0:
                    nodos_faltantes.append(tipo_nodo)
                
                logging.info(f"Nodos {tipo_nodo}: {cantidad}")
            
            if nodos_faltantes:
                logging.warning(f"Tipos de nodo faltantes: {nodos_faltantes}")
                return {
                    "valido": False,
                    "nodos_existentes": nodos_existentes,
                    "nodos_faltantes": nodos_faltantes,
                    "mensaje": "Faltan algunos tipos de nodo"
                }
            else:
                logging.info("Todos los tipos de nodo requeridos están presentes")
                return {
                    "valido": True,
                    "nodos_existentes": nodos_existentes,
                    "nodos_faltantes": [],
                    "mensaje": "Validación exitosa"
                }
                
    except Exception as e:
        logging.error(f"Error validando nodos: {e}")
        return {"error": str(e)}
    finally:
        driver.close()

def obtener_relaciones_por_tipo(tipos_nodo: list):
    """
    Filtra relaciones que involucren ciertos tipos de nodo
    
    Args:
        tipos_nodo: Lista de tipos de nodo (ej: ['Inspecciones', 'Grid_contadores'])
    
    Returns:
        Lista de configuraciones de relaciones filtradas
    """
    relaciones_filtradas = []
    
    for relacion in RELACIONES_CONFIG:
        query = relacion['query']
        # Verificar si algún tipo de nodo está en la query
        if any(tipo in query for tipo in tipos_nodo):
            relaciones_filtradas.append(relacion)
    
    return relaciones_filtradas
