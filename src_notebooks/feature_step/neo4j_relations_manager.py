"""
Neo4j Relations Manager - Gestión de relaciones entre nodos
Integrado con la arquitectura existente del proyecto UFD
"""

import logging
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase
import time

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
        descripcion = relacion_config.get('descripcion', 'Sin descripción')
        
        logger.info(f"Creando relación: {nombre_relacion}")
        logger.info(f"Descripción: {descripcion}")
        logger.debug(f"Query: {query_relacion}")
        
        inicio_tiempo = time.time()
        
        try:
            with driver.session() as session:
                # Ejecutar la query de relación
                resultado = session.run(query_relacion)
                summary = resultado.consume()
                
                relaciones_creadas = summary.counters.relationships_created
                propiedades_set = summary.counters.properties_set
                tiempo_transcurrido = round(time.time() - inicio_tiempo, 2)
                
                if relaciones_creadas > 0:
                    logger.info(f"{nombre_relacion} completada:")
                    logger.info(f"Relaciones creadas: {relaciones_creadas:,}")
                    logger.info(f"Propiedades establecidas: {propiedades_set:,}")
                    logger.info(f"Tiempo: {tiempo_transcurrido}s")
                else:
                    logger.warning(f"{nombre_relacion}: No se crearon relaciones")
                    logger.warning(f"   Esto puede ser normal si no hay datos que coincidan")
                
                return {
                    'nombre': nombre_relacion,
                    'relaciones_creadas': relaciones_creadas,
                    'propiedades_set': propiedades_set,
                    'tiempo_segundos': tiempo_transcurrido,
                    'exito': True
                }
                
        except Exception as e:
            tiempo_transcurrido = round(time.time() - inicio_tiempo, 2)
            logger.error(f"Error creando relación {nombre_relacion}: {e}")
            logger.error(f"Tiempo hasta error: {tiempo_transcurrido}s")
            return {
                'nombre': nombre_relacion,
                'error': str(e),
                'tiempo_segundos': tiempo_transcurrido,
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
        logger.info("INICIANDO CREACIÓN DE RELACIONES")
        logger.info("="*60)
        
        resultados = {}
        total_relaciones_creadas = 0
        relaciones_exitosas = 0
        relaciones_fallidas = 0
        tiempo_total_inicio = time.time()
        
        for i, relacion_config in enumerate(relaciones_config, 1):
            nombre = relacion_config['nombre']
            
            logger.info(f"\nProcesando {i}/{len(relaciones_config)}: {nombre}")
            logger.info("-" * 40)
            
            try:
                resultado = self.crear_relacion_por_lotes(relacion_config)
                resultados[nombre] = resultado
                
                if resultado.get('exito', False):
                    relaciones_exitosas += 1
                    total_relaciones_creadas += resultado.get('relaciones_creadas', 0)
                else:
                    relaciones_fallidas += 1
                    
            except Exception as e:
                logger.error(f"Error crítico procesando relación {nombre}: {e}")
                resultados[nombre] = {
                    'nombre': nombre,
                    'error': str(e),
                    'exito': False
                }
                relaciones_fallidas += 1
        
        tiempo_total = round(time.time() - tiempo_total_inicio, 2)
        
        # Resumen final
        logger.info("\n" + "RESUMEN CREACIÓN DE RELACIONES")
        logger.info("="*60)
        logger.info(f"Total tipos de relaciones procesadas: {len(relaciones_config)}")
        logger.info(f"Exitosas: {relaciones_exitosas}")
        logger.info(f"Fallidas: {relaciones_fallidas}")
        logger.info(f"Total relaciones creadas: {total_relaciones_creadas:,}")
        logger.info(f"Tiempo total: {tiempo_total}s")
        
        if relaciones_fallidas > 0:
            logger.warning(f"\nRELACIONES CON ERRORES ({relaciones_fallidas}):")
            for nombre, resultado in resultados.items():
                if not resultado.get('exito', False):
                    logger.error(f"{nombre}: {resultado.get('error', 'Error desconocido')}")
        
        if relaciones_exitosas > 0:
            logger.info(f"\nRELACIONES EXITOSAS ({relaciones_exitosas}):")
            for nombre, resultado in resultados.items():
                if resultado.get('exito', False):
                    cantidad = resultado.get('relaciones_creadas', 0)
                    tiempo = resultado.get('tiempo_segundos', 0)
                    logger.info(f"{nombre}: {cantidad:,} relaciones ({tiempo}s)")
        
        return {
            'total_procesadas': len(relaciones_config),
            'exitosas': relaciones_exitosas,
            'fallidas': relaciones_fallidas,
            'total_relaciones_creadas': total_relaciones_creadas,
            'tiempo_total_segundos': tiempo_total,
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
                total_relaciones = 0
                
                for record in resultado:
                    cantidad = record['cantidad']
                    relaciones_existentes.append({
                        'tipo': record['tipo_relacion'],
                        'cantidad': cantidad
                    })
                    total_relaciones += cantidad
                
                if relaciones_existentes:
                    logger.info(f"Relaciones existentes en Neo4j ({len(relaciones_existentes)} tipos):")
                    for rel in relaciones_existentes:
                        logger.info(f"{rel['tipo']}: {rel['cantidad']:,} relaciones")
                    logger.info(f"Total relaciones en BD: {total_relaciones:,}")
                else:
                    logger.info("No hay relaciones en la base de datos")
                
                return {
                    'relaciones_existentes': relaciones_existentes,
                    'total_tipos': len(relaciones_existentes),
                    'total_relaciones': total_relaciones
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
        
        inicio_tiempo = time.time()
        
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
                tiempo_transcurrido = round(time.time() - inicio_tiempo, 2)
                
                if eliminadas > 0:
                    logger.info(f"{eliminadas:,} relaciones eliminadas en {tiempo_transcurrido}s")
                else:
                    logger.info(f"No se encontraron relaciones para eliminar")
                
                return {
                    'relaciones_eliminadas': eliminadas,
                    'tipos_eliminados': tipos_relacion,
                    'tiempo_segundos': tiempo_transcurrido,
                    'exito': True
                }
                
        except Exception as e:
            tiempo_transcurrido = round(time.time() - inicio_tiempo, 2)
            logger.error(f"Error limpiando relaciones: {e}")
            return {
                "error": str(e),
                'tiempo_segundos': tiempo_transcurrido,
                'exito': False
            }
        finally:
            driver.close()

    def obtener_estadisticas_nodos(self) -> Dict[str, Any]:
        """
        Obtener estadísticas de nodos existentes en la base de datos
        
        Returns:
            Dict con estadísticas de nodos
        """
        driver = self.conectar_neo4j()
        if not driver:
            return {"error": "No se pudo conectar a Neo4j"}
        
        try:
            with driver.session() as session:
                # Obtener conteo de nodos por tipo
                query_nodos = """
                MATCH (n)
                RETURN DISTINCT labels(n) as etiquetas, count(n) as cantidad
                ORDER BY cantidad DESC
                """
                
                resultado = session.run(query_nodos)
                nodos_por_tipo = []
                total_nodos = 0
                
                for record in resultado:
                    etiquetas = record['etiquetas']
                    cantidad = record['cantidad']
                    # Tomar la primera etiqueta como tipo principal
                    tipo_principal = etiquetas[0] if etiquetas else 'Sin_etiqueta'
                    
                    nodos_por_tipo.append({
                        'tipo': tipo_principal,
                        'etiquetas': etiquetas,
                        'cantidad': cantidad
                    })
                    total_nodos += cantidad
                
                logger.info(f"Nodos existentes en Neo4j ({len(nodos_por_tipo)} tipos):")
                for nodo in nodos_por_tipo:
                    logger.info(f"{nodo['tipo']}: {nodo['cantidad']:,} nodos")
                logger.info(f"Total nodos en BD: {total_nodos:,}")
                
                return {
                    'nodos_por_tipo': nodos_por_tipo,
                    'total_tipos': len(nodos_por_tipo),
                    'total_nodos': total_nodos
                }
                
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas de nodos: {e}")
            return {"error": str(e)}
        finally:
            driver.close()

    def validar_conexiones_potenciales(self, relacion_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validar cuántas conexiones potenciales existen para una relación antes de crearla
        
        Args:
            relacion_config: Configuración de la relación a validar
            
        Returns:
            Dict con estadísticas de validación
        """
        driver = self.conectar_neo4j()
        if not driver:
            return {"error": "No se pudo conectar a Neo4j"}
        
        nombre_relacion = relacion_config['nombre']
        query_original = relacion_config['query']
        
        try:
            # Convertir la query de CREATE a MATCH para contar
            query_validacion = query_original.replace("CREATE ", "WITH ").replace("MERGE ", "WITH ")
            
            # Extraer la parte del MATCH para contar coincidencias
            lineas = query_validacion.split('\n')
            match_lines = [line.strip() for line in lineas if line.strip().startswith('MATCH')]
            where_lines = [line.strip() for line in lineas if line.strip().startswith('WHERE')]
            
            if match_lines:
                query_conteo = match_lines[0]
                if where_lines:
                    query_conteo += '\n' + where_lines[0]
                query_conteo += '\nRETURN count(*) as potenciales'
                
                with driver.session() as session:
                    resultado = session.run(query_conteo)
                    potenciales = resultado.single()['potenciales']
                    
                    logger.info(f"Validación {nombre_relacion}: {potenciales:,} conexiones potenciales")
                    
                    return {
                        'nombre': nombre_relacion,
                        'conexiones_potenciales': potenciales,
                        'recomendacion': 'Proceder' if potenciales > 0 else 'Sin datos coincidentes',
                        'exito': True
                    }
            else:
                logger.warning(f"No se pudo validar {nombre_relacion}: Query muy compleja")
                return {
                    'nombre': nombre_relacion,
                    'conexiones_potenciales': -1,
                    'recomendacion': 'No validable',
                    'exito': False
                }
                
        except Exception as e:
            logger.error(f"Error validando {nombre_relacion}: {e}")
            return {
                'nombre': nombre_relacion,
                'error': str(e),
                'exito': False
            }
        finally:
            driver.close()