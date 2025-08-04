"""
Neo4j Table Manager - Adaptado del notebook cargar_tablas_UFD_v2.ipynb
"""

from neo4j import GraphDatabase
import pandas as pd
from typing import Dict, List, Any, Optional
import time
import logging

logger = logging.getLogger(__name__)

class Neo4jTableManager:
    """Clase para manejar cualquier tabla en Neo4j de forma modular"""
    
    def __init__(self, neo4j_config: Dict[str, str]):
        """
        Inicializar el manager con configuración de Neo4j
        
        Args:
            neo4j_config: Dict con 'uri', 'user', 'password'
        """
        self.config = neo4j_config
        
    def conectar_neo4j(self):
        """Crear conexión a Neo4j"""
        try:
            driver = GraphDatabase.driver(
                self.config["uri"], 
                auth=(self.config["user"], self.config["password"])
            )
            # Probar conexión
            with driver.session() as session:
                session.run("RETURN 1")
            return driver
        except Exception as e:
            logger.error(f"Error conectando a Neo4j: {e}")
            return None

    def obtener_constraints_existentes(self, driver):
        """Obtener lista de constraints existentes"""
        try:
            with driver.session() as session:
                result = session.run("SHOW CONSTRAINTS")
                constraints = {}
                for record in result:
                    name = record["name"]
                    labels = record["labelsOrTypes"]
                    properties = record["properties"]
                    constraints[name] = {
                        'labels': labels,
                        'properties': properties,
                        'type': record["type"]
                    }
                return constraints
        except Exception as e:
            logger.warning(f"Error obteniendo constraints: {e}")
            return {}
    
    def obtener_indices_existentes(self, driver):
        """Obtener lista de índices existentes"""
        try:
            with driver.session() as session:
                result = session.run("SHOW INDEXES")
                indices = {}
                for record in result:
                    # Solo índices que no sean de constraints
                    if record.get("owningConstraint") is None:
                        name = record["name"]
                        labels = record["labelsOrTypes"]
                        properties = record["properties"]
                        indices[name] = {
                            'labels': labels,
                            'properties': properties,
                            'type': record["type"],
                            'state': record["state"]
                        }
                return indices
        except Exception as e:
            logger.warning(f"Error obteniendo índices: {e}")
            return {}
    
    def crear_constraints_indices_tabla_inteligente(self, 
                                                  tabla_config: Dict[str, Any],
                                                  forzar_recreacion: bool = False,
                                                  silencioso: bool = False):
        """VERSIÓN INTELIGENTE - Crear constraints e índices solo si no existen"""
        driver = self.conectar_neo4j()
        if not driver:
            return False
        
        nodo_label = tabla_config['nodo_label']
        campos_constraint = tabla_config.get('campos_constraint', [])
        campos_indices = tabla_config.get('campos_indices', [])
        
        try:
            # 1. Obtener estado actual
            if not silencioso:
                logger.info(f"Verificando estructura existente para {nodo_label}...")
                
            constraints_existentes = self.obtener_constraints_existentes(driver)
            indices_existentes = self.obtener_indices_existentes(driver)
            
            if not silencioso:
                logger.info(f"Constraints existentes: {len(constraints_existentes)}")
                logger.info(f"Índices existentes: {len(indices_existentes)}")
            
            with driver.session() as session:
                # 2. Manejar constraint único
                constraint_name = f"{nodo_label.lower()}_unique"
                constraint_existe = constraint_name in constraints_existentes
                
                if campos_constraint:
                    if forzar_recreacion and constraint_existe:
                        if not silencioso:
                            logger.info(f"Eliminando constraint existente: {constraint_name}")
                        session.run(f"DROP CONSTRAINT {constraint_name}")
                        constraint_existe = False
                    
                    if not constraint_existe:
                        campos_str = ", ".join([f"e.{campo}" for campo in campos_constraint])
                        constraint_query = f"""
                        CREATE CONSTRAINT {constraint_name} IF NOT EXISTS 
                        FOR (e:{nodo_label}) 
                        REQUIRE ({campos_str}) IS UNIQUE
                        """
                        session.run(constraint_query)
                        if not silencioso:
                            logger.info(f"Constraint creado: {constraint_name}")
                    else:
                        if not silencioso:
                            logger.info(f"Constraint ya existe: {constraint_name}")
                
                # 3. Manejar índices individuales
                indices_creados = 0
                indices_ya_existian = 0
                
                for campo in campos_indices:
                    index_name = f"{nodo_label.lower()}_{campo}"
                    index_existe = index_name in indices_existentes
                    
                    if forzar_recreacion and index_existe:
                        if not silencioso:
                            logger.info(f"Eliminando índice existente: {index_name}")
                        session.run(f"DROP INDEX {index_name}")
                        index_existe = False
                    
                    if not index_existe:
                        index_query = f"""
                        CREATE INDEX {index_name} IF NOT EXISTS 
                        FOR (e:{nodo_label}) ON (e.{campo})
                        """
                        session.run(index_query)
                        if not silencioso:
                            logger.info(f"Índice creado: {index_name}")
                        indices_creados += 1
                    else:
                        if not silencioso:
                            logger.info(f"Índice ya existe: {index_name}")
                        indices_ya_existian += 1
                
                # 4. Resumen final
                if not silencioso:
                    logger.info(f"Resumen para {nodo_label}:")
                    logger.info(f"Constraint: {'Creado' if not constraint_existe and campos_constraint else 'Ya existía' if constraint_existe else 'No requerido'}")
                    logger.info(f"Índices creados: {indices_creados}")
                    logger.info(f"Índices que ya existían: {indices_ya_existian}")
                
                return True
                
        except Exception as e:
            logger.error(f"Error creando estructura para {nodo_label}: {e}")
            return False
        finally:
            driver.close()

    def preparar_fila_generica(self, row_dict: Dict[str, Any], campos_tabla: List[str]) -> Dict[str, Any]:
        """Preparar fila para inserción en Neo4j"""
        def limpiar_valor(valor):
            if valor is None or valor == "NULL" or (isinstance(valor, str) and valor.strip() == ""):
                return None
            # Convertir timestamps a string ISO
            if hasattr(valor, 'isoformat'):
                return valor.isoformat()
            return str(valor) if valor is not None else None
        
        # Preparar solo los campos especificados
        result = {}
        for campo in campos_tabla:
            result[campo] = limpiar_valor(row_dict.get(campo))
        
        return result

    def generar_query_insercion(self, tabla_config: Dict[str, Any]) -> str:
        """Generar query de inserción para Neo4j"""
        nodo_label = tabla_config['nodo_label']
        campos_constraint = tabla_config.get('campos_constraint', [])
        todos_los_campos = tabla_config['todos_los_campos']
        
        # Campos para MERGE (evitar duplicados)
        if campos_constraint:
            merge_campos = ",\n        ".join([f"{campo}: item.{campo}" for campo in campos_constraint])
            merge_clause = f"""
    MERGE (e:{nodo_label} {{
        {merge_campos}
    }})"""
        else:
            # Si no hay constraint, crear directamente
            merge_clause = f"CREATE (e:{nodo_label})"
        
        # Campos para SET (actualizar/establecer propiedades)
        set_campos = ",\n        ".join([f"e.{campo} = item.{campo}" for campo in todos_los_campos])
        
        query = f"""
    UNWIND $items as item
    {merge_clause}
    SET 
        {set_campos},
        e.created_at = datetime(),
        e.updated_at = datetime()
    """
        
        return query

    def insertar_lote_generico(self, datos_lote: List[Dict[str, Any]], tabla_config: Dict[str, Any]) -> Dict[str, Any]:
        """Insertar lote de datos en Neo4j"""
        driver = self.conectar_neo4j()
        if not driver:
            return {"error": "No se pudo conectar"}
        
        query = self.generar_query_insercion(tabla_config)
        
        try:
            with driver.session() as session:
                result = session.run(query, items=datos_lote)
                summary = result.consume()
                return {
                    'nodes_created': summary.counters.nodes_created,
                    'properties_set': summary.counters.properties_set,
                    'nodes_merged': len(datos_lote)
                }
        except Exception as e:
            logger.error(f"Error insertando lote: {e}")
            return {"error": str(e)}
        finally:
            driver.close()

    def cargar_dataframe_a_neo4j_optimizado(self, spark_df, tabla_config: Dict[str, Any], batch_size: int = 500) -> Dict[str, Any]:
        """
        VERSIÓN OPTIMIZADA - Procesa en lotes sin usar operaciones RDD complejas
        """
        nodo_label = tabla_config['nodo_label']
        todos_los_campos = tabla_config['todos_los_campos']
        
        # Primero crear constraints e índices
        logger.info(f"Configurando estructura para {nodo_label}...")
        self.crear_constraints_indices_tabla_inteligente(tabla_config, silencioso=True)
        
        # Obtener total de filas (usar cache para evitar recálculo)
        spark_df.cache()
        total_filas = spark_df.count()
        logger.info(f"INICIANDO CARGA OPTIMIZADA DE {total_filas} REGISTROS A {nodo_label}")
        logger.info("=" * 60)
        
        # Estadísticas de carga
        total_creados = 0
        total_propiedades = 0
        lotes_procesados = 0
        errores = 0
        
        # Crear una columna de índice consecutivo UNA SOLA VEZ
        from pyspark.sql.functions import row_number
        from pyspark.sql.window import Window
        
        # Crear ventana para row_number (genera índices consecutivos 1, 2, 3...)
        window_spec = Window.orderBy(spark_df.columns[0])  # Ordenar por primera columna
        df_con_indice = spark_df.withColumn("temp_row_id", row_number().over(window_spec))
        df_con_indice.cache()
        
        # Procesar en lotes usando row_number consecutivo
        offset = 1  # row_number empieza en 1
        
        while offset <= total_filas:
            # Calcular el tamaño del lote actual
            lote_actual = min(batch_size, total_filas - offset + 1)
            lotes_procesados += 1
            
            logger.info(f"Procesando lote {lotes_procesados}: registros {offset}-{offset+lote_actual-1} de {total_filas}")
            
            try:
                # Obtener el lote actual usando row_number consecutivo
                lote_df = df_con_indice.filter(
                    (df_con_indice.temp_row_id >= offset) & 
                    (df_con_indice.temp_row_id < offset + batch_size)
                ).drop("temp_row_id")  # Remover la columna temporal
                
                # Convertir solo este lote pequeño a pandas
                lote_pandas = lote_df.toPandas()
                
                if len(lote_pandas) == 0:
                    logger.info(f"Lote vacío en offset {offset}, terminando procesamiento")
                    break
                
                # Preparar datos del lote
                datos_lote = []
                for _, row in lote_pandas.iterrows():
                    row_dict = row.to_dict()
                    dato_preparado = self.preparar_fila_generica(row_dict, todos_los_campos)
                    datos_lote.append(dato_preparado)
                
                # Insertar lote
                resultado = self.insertar_lote_generico(datos_lote, tabla_config)
                
                if "error" in resultado:
                    logger.error(f"Error en lote {lotes_procesados}: {resultado['error']}")
                    errores += 1
                else:
                    # Actualizar estadísticas
                    total_creados += resultado['nodes_created']
                    total_propiedades += resultado['properties_set']
                
                # Mostrar progreso
                progreso = ((offset + len(datos_lote)) / total_filas) * 100
                logger.info(f"Lote {lotes_procesados} completado - Progreso: {progreso:.1f}%")
                
            except Exception as e:
                logger.error(f"Error procesando lote en offset {offset}: {str(e)}")
                errores += 1
            
            # Avanzar al siguiente lote
            offset += batch_size
        
        # Limpiar cache
        spark_df.unpersist()
        df_con_indice.unpersist()
        
        # Resumen final
        logger.info("\n" + "=" * 60)
        logger.info(f"CARGA COMPLETADA PARA {nodo_label}")
        logger.info(f"Estadísticas finales:")
        logger.info(f"Total nodos creados/actualizados: {total_creados}")
        logger.info(f"Total propiedades establecidas: {total_propiedades}")
        logger.info(f"Lotes procesados: {lotes_procesados}")
        logger.info(f"Errores: {errores}")

        return {
            'total_creados': total_creados,
            'total_propiedades': total_propiedades,
            'lotes_procesados': lotes_procesados,
            'errores': errores,
            'nodo_label': nodo_label
        }
    

"""
Extensión del Neo4j Manager para manejo de relaciones
"""

import logging
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

class Neo4jRelationsManager:
    """Clase para manejar relaciones entre nodos en Neo4j"""
    
    def __init__(self, neo4j_config: Dict[str, str]):
        """
        Inicializar el manager con configuración de Neo4j
        
        Args:
            neo4j_config: Dict con 'uri', 'user', 'password'
        """
        self.config = neo4j_config
        
    def conectar_neo4j(self):
        """Crear conexión a Neo4j"""
        try:
            driver = GraphDatabase.driver(
                self.config["uri"], 
                auth=(self.config["user"], self.config["password"])
            )
            # Probar conexión
            with driver.session() as session:
                session.run("RETURN 1")
            return driver
        except Exception as e:
            logger.error(f"Error conectando a Neo4j: {e}")
            return None

    def crear_relacion_por_lotes(self, 
                                relacion_config: Dict[str, Any], 
                                batch_size: int = 1000) -> Dict[str, Any]:
        """
        Crear relaciones por lotes entre nodos
        
        Args:
            relacion_config: Configuración de la relación a crear
            batch_size: Tamaño del lote para procesamiento
            
        Returns:
            Dict con estadísticas de la operación
        """
        driver = self.conectar_neo4j()
        if not driver:
            return {"error": "No se pudo conectar a Neo4j"}
        
        nombre_relacion = relacion_config['nombre']
        query_relacion = relacion_config['query']
        
        logger.info(f"Creando relación: {nombre_relacion}")
        logger.info(f"Query: {query_relacion}")
        
        try:
            with driver.session() as session:
                # Ejecutar la query de relación
                resultado = session.run(query_relacion)
                summary = resultado.consume()
                
                relaciones_creadas = summary.counters.relationships_created
                propiedades_set = summary.counters.properties_set
                
                logger.info(f"Relación {nombre_relacion} completada:")
                logger.info(f"  Relaciones creadas: {relaciones_creadas}")
                logger.info(f"  Propiedades establecidas: {propiedades_set}")
                
                return {
                    'nombre': nombre_relacion,
                    'relaciones_creadas': relaciones_creadas,
                    'propiedades_set': propiedades_set,
                    'exito': True
                }
                
        except Exception as e:
            logger.error(f"Error creando relación {nombre_relacion}: {e}")
            return {
                'nombre': nombre_relacion,
                'error': str(e),
                'exito': False
            }
        finally:
            driver.close()

    def crear_todas_las_relaciones(self, relaciones_config: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Crear todas las relaciones definidas en la configuración
        
        Args:
            relaciones_config: Lista de configuraciones de relaciones
            
        Returns:
            Dict con resumen de todas las operaciones
        """
        logger.info("="*60)
        logger.info("INICIANDO CREACIÓN DE RELACIONES")
        logger.info("="*60)
        
        resultados = {}
        total_relaciones_creadas = 0
        relaciones_exitosas = 0
        relaciones_fallidas = 0
        
        for relacion_config in relaciones_config:
            nombre = relacion_config['nombre']
            
            try:
                resultado = self.crear_relacion_por_lotes(relacion_config)
                resultados[nombre] = resultado
                
                if resultado.get('exito', False):
                    relaciones_exitosas += 1
                    total_relaciones_creadas += resultado.get('relaciones_creadas', 0)
                else:
                    relaciones_fallidas += 1
                    
            except Exception as e:
                logger.error(f"Error procesando relación {nombre}: {e}")
                resultados[nombre] = {
                    'nombre': nombre,
                    'error': str(e),
                    'exito': False
                }
                relaciones_fallidas += 1
        
        # Resumen final
        logger.info("\n" + "="*60)
        logger.info("RESUMEN CREACIÓN DE RELACIONES")
        logger.info("="*60)
        logger.info(f"Total relaciones procesadas: {len(relaciones_config)}")
        logger.info(f"Exitosas: {relaciones_exitosas}")
        logger.info(f"Fallidas: {relaciones_fallidas}")
        logger.info(f"Total relaciones creadas: {total_relaciones_creadas}")
        
        if relaciones_fallidas > 0:
            logger.warning("Relaciones con errores:")
            for nombre, resultado in resultados.items():
                if not resultado.get('exito', False):
                    logger.error(f"  {nombre}: {resultado.get('error', 'Error desconocido')}")
        
        return {
            'total_procesadas': len(relaciones_config),
            'exitosas': relaciones_exitosas,
            'fallidas': relaciones_fallidas,
            'total_relaciones_creadas': total_relaciones_creadas,
            'resultados_detallados': resultados
        }

    def verificar_relaciones_existentes(self) -> Dict[str, Any]:
        """
        Verificar qué relaciones ya existen en la base de datos
        
        Returns:
            Dict con información de relaciones existentes
        """
        driver = self.conectar_neo4j()
        if not driver:
            return {"error": "No se pudo conectar a Neo4j"}
        
        try:
            with driver.session() as session:
                # Obtener tipos de relaciones existentes
                query_tipos = """
                MATCH ()-[r]->()
                RETURN DISTINCT type(r) as tipo_relacion, count(r) as cantidad
                ORDER BY cantidad DESC
                """
                
                resultado = session.run(query_tipos)
                relaciones_existentes = []
                
                for record in resultado:
                    relaciones_existentes.append({
                        'tipo': record['tipo_relacion'],
                        'cantidad': record['cantidad']
                    })
                
                logger.info("Relaciones existentes en Neo4j:")
                for rel in relaciones_existentes:
                    logger.info(f"  {rel['tipo']}: {rel['cantidad']} relaciones")
                
                return {
                    'relaciones_existentes': relaciones_existentes,
                    'total_tipos': len(relaciones_existentes),
                    'total_relaciones': sum(rel['cantidad'] for rel in relaciones_existentes)
                }
                
        except Exception as e:
            logger.error(f"Error verificando relaciones: {e}")
            return {"error": str(e)}
        finally:
            driver.close()

    def limpiar_relaciones(self, tipos_relacion: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Limpiar relaciones específicas o todas las relaciones
        
        Args:
            tipos_relacion: Lista de tipos de relación a eliminar. Si es None, elimina todas.
            
        Returns:
            Dict con estadísticas de la limpieza
        """
        driver = self.conectar_neo4j()
        if not driver:
            return {"error": "No se pudo conectar a Neo4j"}
        
        try:
            with driver.session() as session:
                if tipos_relacion is None:
                    # Eliminar todas las relaciones
                    query = "MATCH ()-[r]-() DELETE r RETURN count(r) as eliminadas"
                    logger.info("Eliminando TODAS las relaciones...")
                else:
                    # Eliminar relaciones específicas
                    tipos_str = "', '".join(tipos_relacion)
                    query = f"""
                    MATCH ()-[r]->()
                    WHERE type(r) IN ['{tipos_str}']
                    DELETE r
                    RETURN count(r) as eliminadas
                    """
                    logger.info(f"Eliminando relaciones de tipos: {tipos_relacion}")
                
                resultado = session.run(query)
                eliminadas = resultado.single()['eliminadas']
                
                logger.info(f"Relaciones eliminadas: {eliminadas}")
                
                return {
                    'relaciones_eliminadas': eliminadas,
                    'tipos_eliminados': tipos_relacion,
                    'exito': True
                }
                
        except Exception as e:
            logger.error(f"Error limpiando relaciones: {e}")
            return {"error": str(e)}
        finally:
            driver.close()