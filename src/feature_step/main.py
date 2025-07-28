import argparse
import logging
import os
from config import DATA_CONFIG, NEO4J_CONFIG, tablas_neo4j
from functional_code import (
    load_raw_training_data, 
    borrar_toda_bd_neo4j, 
    validar_neo4j_config, 
    procesar_tablas_neo4j,
    crear_todas_las_relaciones,
    limpiar_relaciones_especificas,
    validar_nodos_para_relaciones
)
from pyspark.sql import SparkSession


def main():
    # Argumentos según documentación original + nuevos para relaciones
    parser = argparse.ArgumentParser(description="Sistema de Carga UFD a Neo4j con Relaciones")
    
    # Argumentos originales
    parser.add_argument("-i", "--input", help="Carpeta donde están los datasets (.csv)", required=True)
    parser.add_argument("-m", "--mode", help="Modo de ejecución", default="train")
    parser.add_argument("-l", "--loglevel", help="Nivel de log", default="INFO")
    parser.add_argument("--borrar-neo4j", action="store_true", help="Borrar BD Neo4j antes de cargar datos")
    parser.add_argument("--batch-size", type=int, default=500, help="Tamaño de lote para Neo4j")
    
    # NUEVOS ARGUMENTOS PARA RELACIONES
    parser.add_argument("--crear-relaciones", action="store_true", help="Crear relaciones después de cargar datos")
    parser.add_argument("--solo-relaciones", action="store_true", help="Solo crear relaciones (sin cargar datos)")
    parser.add_argument("--limpiar-relaciones", action="store_true", help="Limpiar relaciones antes de crear")
    parser.add_argument("--relaciones-especificas", nargs="+", help="Crear solo relaciones específicas (por nombre)")
    parser.add_argument("--validar-nodos", action="store_true", help="Validar nodos antes de crear relaciones")
    
    args = parser.parse_args()

    # Configurar logging según argumento
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Nivel de log inválido: {args.loglevel}')
    
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("carga_tablas.log"),
            logging.StreamHandler()
        ]
    )

    logging.info("==" * 30)
    logging.info("INICIANDO PROCESO DE CARGA UFD")
    logging.info("==" * 30)
    logging.info(f"Carpeta de entrada: {args.input}")
    logging.info(f"Modo: {args.mode}")
    logging.info(f"Nivel de log: {args.loglevel}")
    logging.info(f"Tamaño de lote: {args.batch_size}")
    
    if args.borrar_neo4j:
        logging.info("Se borrará la BD Neo4j antes de cargar")
    else:
        logging.info("Solo se cargarán datos (sin borrar BD)")

    # Mostrar configuración de relaciones
    if args.crear_relaciones:
        logging.info("Se crearán relaciones después de cargar datos")
    if args.solo_relaciones:
        logging.info("MODO: Solo creación de relaciones")
    if args.limpiar_relaciones:
        logging.info("Se limpiarán relaciones existentes")
    if args.relaciones_especificas:
        logging.info(f"Relaciones específicas: {args.relaciones_especificas}")
    if args.validar_nodos:
        logging.info("Se validarán nodos antes de crear relaciones")

    # Verificar que la carpeta existe (solo si no es modo solo-relaciones)
    if not args.solo_relaciones:
        if not os.path.exists(args.input):
            logging.error(f"La carpeta {args.input} no existe")
            return 1

        # Actualizar las rutas de cada tabla para que apunten a la carpeta indicada
        data_config_updated = {}
        archivos_faltantes = []
        
        for nombre_tabla, cfg in DATA_CONFIG.items():
            filename = os.path.basename(cfg["path"])
            ruta_completa = os.path.join(args.input, filename)
            
            # Verificar que el archivo existe
            if not os.path.exists(ruta_completa):
                archivos_faltantes.append(filename)
            
            # Crear nueva configuración con ruta actualizada
            cfg_actualizada = cfg.copy()
            cfg_actualizada["path"] = ruta_completa
            data_config_updated[nombre_tabla] = cfg_actualizada
        
        # Verificar archivos faltantes
        if archivos_faltantes:
            logging.warning(f"Archivos faltantes: {archivos_faltantes}")
            logging.info("Archivos disponibles en la carpeta:")
            for archivo in os.listdir(args.input):
                if archivo.endswith('.csv'):
                    logging.info(f"  {archivo}")
    
    # Validar configuración de Neo4j antes de proceder
    if not validar_neo4j_config(NEO4J_CONFIG):
        logging.error("Configuración de Neo4j inválida. Revisa NEO4J_CONFIG en config.py")
        return 1

    # NUEVA LÓGICA PARA MODO SOLO-RELACIONES
    if args.solo_relaciones:
        logging.info("MODO: Solo creación de relaciones")
        logging.info("="*50)
        
        # Validar nodos si se solicita
        if args.validar_nodos:
            tipos_nodo_requeridos = [
                "Inspecciones", "Grid_contadores", "Evento", "Curva_horaria", 
                "Denuncias", "Expedientes", "Grupo_eventos_codigos"
            ]
            
            logging.info("Validando existencia de nodos...")
            validacion = validar_nodos_para_relaciones(NEO4J_CONFIG, tipos_nodo_requeridos)
            
            if not validacion.get("valido", False):
                logging.error("Validación de nodos falló:")
                for nodo_faltante in validacion.get("nodos_faltantes", []):
                    logging.error(f"Falta tipo de nodo: {nodo_faltante}")
                logging.error("Solución: Carga primero los datos con --mode train")
                return 1
            else:
                logging.info("Validación de nodos exitosa")
                for tipo_nodo, cantidad in validacion.get("nodos_existentes", {}).items():
                    logging.info(f"{tipo_nodo}: {cantidad:,} nodos")
        
        # Limpiar relaciones si se solicita
        if args.limpiar_relaciones:
            logging.info("\nLimpiando relaciones existentes...")
            resultado_limpieza = limpiar_relaciones_especificas(NEO4J_CONFIG)
            
            if resultado_limpieza.get('exito', False):
                eliminadas = resultado_limpieza['relaciones_eliminadas']
                tiempo = resultado_limpieza.get('tiempo_segundos', 0)
                logging.info(f"{eliminadas:,} relaciones eliminadas en {tiempo}s")
            else:
                error_msg = resultado_limpieza.get('error', 'Error desconocido')
                logging.warning(f"Error limpiando relaciones: {error_msg}")
        
        # Crear relaciones
        logging.info("\nCreando relaciones...")
        resultado_relaciones = crear_todas_las_relaciones(
            NEO4J_CONFIG, 
            relaciones_a_crear=args.relaciones_especificas,
            validar_antes=not args.validar_nodos  
        )
        
        if resultado_relaciones.get('exitosas', 0) > 0:
            exitosas = resultado_relaciones['exitosas']
            total_relaciones = resultado_relaciones['total_relaciones_creadas']
            tiempo_total = resultado_relaciones.get('tiempo_total_segundos', 0)
            
            logging.info(f"\nPROCESO COMPLETADO EXITOSAMENTE:")
            logging.info(f"Tipos de relaciones creadas: {exitosas}")
            logging.info(f"Total relaciones en BD: {total_relaciones:,}")
            logging.info(f"Tiempo total: {tiempo_total}s")
            
            if resultado_relaciones.get('fallidas', 0) > 0:
                fallidas = resultado_relaciones['fallidas']
                logging.warning(f"Tipos de relaciones fallidas: {fallidas}")
            
            return 0
        else:
            logging.error("Error: No se pudieron crear relaciones")
            if "error" in resultado_relaciones:
                logging.error(f"Detalles: {resultado_relaciones['error']}")
            if resultado_relaciones.get('fallidas', 0) > 0:
                logging.error(f"Tipos fallidos: {resultado_relaciones['fallidas']}")
            return 1

    
    # CARGAR DATOS DESDE ARCHIVOS CSV
    logging.info("Cargando datos desde archivos CSV...")
    dfs = load_raw_training_data(data_config_updated)
    
    if not dfs:
        logging.error("No se pudieron cargar los DataFrames")
        return 1
    
    logging.info(f"{len(dfs)} DataFrames cargados correctamente")

    # Ejecución modo = train
    if args.mode == "train":
        # Decidir si borrar BD según parámetro
        if args.borrar_neo4j:
            logging.info("Modo TRAIN: Borrar Neo4j + Cargar datos")
            
            # PASO 1: Borrar la base de datos Neo4j
            logging.info("\n" + "="*60)
            logging.info("PASO 1: Borrando toda la base de datos Neo4j...")
            logging.info("="*60)
            resultado_borrado = borrar_toda_bd_neo4j(
                uri=NEO4J_CONFIG["uri"],
                usuario=NEO4J_CONFIG["user"],
                password=NEO4J_CONFIG["password"],
                confirmar=True,
                borrar_props=True 
            )
            
            # Mostrar resumen del borrado
            logging.info("Resumen del borrado:")
            for clave, valor in resultado_borrado.items():
                if clave != "errores":
                    logging.info(f"  {clave}: {valor}")
            if resultado_borrado.get("errores"):
                logging.warning(f"Errores en borrado: {len(resultado_borrado['errores'])}")
                for error in resultado_borrado["errores"]:
                    logging.warning(f"      - {error}")
            
            logging.info("\n" + "="*60)
            logging.info("PASO 2: Cargando datos frescos a Neo4j...")
            logging.info("="*60)
        else:
            logging.info("Modo TRAIN: Solo cargar datos (BD se mantiene)")
        
        # Filtrar solo las tablas que se cargaron exitosamente
        tablas_disponibles = {}
        for nombre_tabla, config_tabla in tablas_neo4j.items():
            dataframe_name = config_tabla["dataframe"]
            if dataframe_name in dfs:
                tablas_disponibles[nombre_tabla] = config_tabla
            else:
                logging.warning(f"Saltando {nombre_tabla}: DataFrame {dataframe_name} no disponible")

        if not tablas_disponibles:
            logging.error("No hay tablas disponibles para procesar")
            return 1

        logging.info(f"Procesando {len(tablas_disponibles)} tablas...")

        # Procesar tablas en Neo4j
        resultados = procesar_tablas_neo4j(tablas_disponibles, dfs, NEO4J_CONFIG)

        # Evaluación de resultados
        exitosos = sum(1 for r in resultados.values() if r.get("exito", False))
        fallidos = len(resultados) - exitosos

        if exitosos > 0:
            if args.borrar_neo4j:
                logging.info(f"{exitosos} tablas procesadas exitosamente en Neo4j limpio!")
            else:
                logging.info(f"{exitosos} tablas procesadas exitosamente en Neo4j existente!")
            if fallidos > 0:
                logging.warning(f"{fallidos} tablas fallaron")

            # CREAR RELACIONES SI SE SOLICITA
            if args.crear_relaciones:
                logging.info("\n" + "="*60)
                logging.info("PASO 3: Creando relaciones entre nodos...")
                logging.info("="*60)
                
                # Limpiar relaciones si se solicita
                if args.limpiar_relaciones:
                    logging.info("Limpiando relaciones existentes...")
                    resultado_limpieza = limpiar_relaciones_especificas(NEO4J_CONFIG)
                    
                    if resultado_limpieza.get('exito', False):
                        eliminadas = resultado_limpieza['relaciones_eliminadas']
                        tiempo = resultado_limpieza.get('tiempo_segundos', 0)
                        logging.info(f"{eliminadas:,} relaciones eliminadas en {tiempo}s")
                    else:
                        error_msg = resultado_limpieza.get('error', 'Error desconocido')
                        logging.warning(f"Error limpiando relaciones: {error_msg}")
                
                # Validar nodos antes de crear relaciones
                if args.validar_nodos:
                    tipos_nodo_disponibles = [info['nombre_neo4j'] for info in resultados.values() if info.get("exito", False)]
                    
                    logging.info("Validando nodos disponibles para relaciones...")
                    validacion = validar_nodos_para_relaciones(NEO4J_CONFIG, tipos_nodo_disponibles)
                    
                    if not validacion.get("valido", False):
                        logging.warning("Algunos nodos pueden estar faltantes para relaciones óptimas")
                        for nodo_faltante in validacion.get("nodos_faltantes", []):
                            logging.warning(f"Nodo faltante: {nodo_faltante}")
                    else:
                        logging.info("Validación de nodos para relaciones exitosa")
                        for tipo_nodo, cantidad in validacion.get("nodos_existentes", {}).items():
                            logging.info(f"{tipo_nodo}: {cantidad:,} nodos")
                
                # Crear relaciones
                logging.info("Iniciando creación de relaciones...")
                resultado_relaciones = crear_todas_las_relaciones(
                    NEO4J_CONFIG, 
                    relaciones_a_crear=args.relaciones_especificas,
                    validar_antes=not args.validar_nodos
                )
                
                if resultado_relaciones.get('exitosas', 0) > 0:
                    exitosas_rel = resultado_relaciones['exitosas']
                    total_relaciones = resultado_relaciones['total_relaciones_creadas']
                    tiempo_total_rel = resultado_relaciones.get('tiempo_total_segundos', 0)
                    
                    logging.info(f"\nPROCESO COMPLETO EXITOSO:")
                    logging.info(f"Tablas cargadas: {exitosos}")
                    logging.info(f"Tipos de relaciones: {exitosas_rel}")
                    logging.info(f"Total relaciones: {total_relaciones:,}")
                    logging.info(f"Tiempo relaciones: {tiempo_total_rel}s")
                    
                    # Mostrar resumen detallado de relaciones creadas
                    if resultado_relaciones.get('resultados_detallados'):
                        logging.info("\nDETALLE DE RELACIONES CREADAS:")
                        for nombre, detalle in resultado_relaciones['resultados_detallados'].items():
                            if detalle.get('exito', False):
                                relaciones_creadas = detalle['relaciones_creadas']
                                tiempo_rel = detalle.get('tiempo_segundos', 0)
                                logging.info(f"{nombre}: {relaciones_creadas:,} relaciones ({tiempo_rel}s)")
                            else:
                                error_msg = detalle.get('error', 'Error desconocido')
                                logging.error(f"{nombre}: {error_msg}")
                    
                    if resultado_relaciones.get('fallidas', 0) > 0:
                        fallidas_rel = resultado_relaciones['fallidas']
                        logging.warning(f"Tipos de relaciones fallidas: {fallidas_rel}")
                else:
                    logging.error("Error: No se pudieron crear relaciones")
                    if "error" in resultado_relaciones:
                        logging.error(f"Detalles: {resultado_relaciones['error']}")
                    if resultado_relaciones.get('fallidas', 0) > 0:
                        fallidas_rel = resultado_relaciones['fallidas']
                        logging.error(f"Tipos de relaciones fallidas: {fallidas_rel}")
            else:
                logging.info("\nOPCIONES ADICIONALES:")
                logging.info("Para crear relaciones, usa --crear-relaciones")
                logging.info("Para solo crear relaciones, usa --solo-relaciones")
                logging.info("Para validar nodos, usa --validar-nodos")
        
        else:
            logging.error("Error: Ninguna tabla se procesó exitosamente")
            return 1

        # Mostrar información detallada de las tablas procesadas exitosamente
        if exitosos > 0:
            logging.info("\nINFORMACIÓN DETALLADA DE TABLAS CARGADAS:")
            logging.info("=" * 60)
            
            for nombre_tabla, info in resultados.items():
                if info.get("exito", False):
                    dataframe_name = info.get("dataframe_name", "unknown")
                    df = dfs.get(dataframe_name)
                    if df:
                        try:
                            filas = df.count()
                            columnas = len(df.columns)
                            nodos_creados = info.get("resultado", {}).get("total_creados", 0)
                            
                            logging.info(f"{nombre_tabla.upper()}:")
                            logging.info(f"Filas: {filas:,}")
                            logging.info(f"Columnas: {columnas}")
                            logging.info(f"Nodos en Neo4j: {nodos_creados:,}")
                            logging.info(f"Etiqueta: {info['nombre_neo4j']}")
                            
                            # Mostrar muestra de datos (solo primeras 3 filas)
                            logging.info(f"Muestra de datos:")
                            df.show(3, truncate=True)
                            
                        except Exception as e:
                            logging.error(f"Error mostrando info de {nombre_tabla}: {e}")

    elif args.mode == "test":
        logging.info("Modo TEST: Funcionalidad no implementada aún")
        logging.info("En desarrollo para futuras versiones")
        # Aquí podrías agregar lógica para modo test
        
    else:
        logging.warning(f"Modo de ejecución '{args.mode}' no implementado")
        logging.info("Modos disponibles: train, test")
        return 1

    # Mostrar resumen final
    logging.info("\n" + "==" * 30)
    logging.info("RESUMEN FINAL")
    logging.info("==" * 30)
    logging.info(f"Carpeta procesada: {args.input}")
    
    if not args.solo_relaciones:
        logging.info(f"DataFrames cargados: {len(dfs)}")
        
        if args.mode == "train" and 'resultados' in locals():
            total_nodos = sum(
                r.get("resultado", {}).get("total_creados", 0) 
                for r in resultados.values() 
                if r.get("exito", False)
            )
            logging.info(f"Total nodos en Neo4j: {total_nodos:,}")
            logging.info(f"Tablas exitosas: {exitosos}")
            logging.info(f"Tablas fallidas: {fallidos}")
    
    # Mostrar resumen de relaciones si se crearon
    if (args.crear_relaciones or args.solo_relaciones) and 'resultado_relaciones' in locals():
        exitosas_rel = resultado_relaciones.get('exitosas', 0)
        total_relaciones = resultado_relaciones.get('total_relaciones_creadas', 0)
        fallidas_rel = resultado_relaciones.get('fallidas', 0)
        
        logging.info(f"Tipos de relaciones creadas: {exitosas_rel}")
        logging.info(f"Total relaciones en BD: {total_relaciones:,}")
        
        if fallidas_rel > 0:
            logging.warning(f"Tipos de relaciones fallidas: {fallidas_rel}")

    logging.info("¡Proceso completado!")
    
    return 0


if __name__ == "__main__":
    try:
        exit_code = main()
    except KeyboardInterrupt:
        logging.info("Proceso interrumpido por el usuario")
        exit_code = 1
    except Exception as e:
        logging.error(f"Error fatal: {e}", exc_info=True)
        exit_code = 1
    finally:
        # Cerrar SparkSession correctamente
        try:
            spark_session = SparkSession.getActiveSession()
            if spark_session:
                spark_session.stop()
                logging.info("SparkSession cerrado correctamente")
        except Exception as e:
            logging.warning(f"Error cerrando SparkSession: {e}")
    
    exit(exit_code)