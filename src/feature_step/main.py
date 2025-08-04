import argparse
import logging
import os
from config import DATA_CONFIG, NEO4J_CONFIG, tablas_neo4j
from functional_code import (
    load_raw_training_data_parquet,
    borrar_toda_bd_neo4j, 
    validar_neo4j_config, 
    procesar_tablas_neo4j,
    crear_todas_las_relaciones
)
from pyspark.sql import SparkSession


def main():
    # Argumentos simplificados - solo los que necesitas
    parser = argparse.ArgumentParser(description="Sistema de Carga UFD a Neo4j - Versión Simplificada")
    
    parser.add_argument("-i", "--input", help="Carpeta donde están los datasets", required=True)
    parser.add_argument("-m", "--mode", help="Modo de ejecución (solo 'train' soportado)", default="train")
    parser.add_argument("-l", "--loglevel", help="Nivel de log", default="INFO")
    parser.add_argument("--borrar-neo4j", action="store_true", help="Borrar BD Neo4j antes de cargar datos")
    parser.add_argument("--crear-relaciones", action="store_true", help="Crear relaciones después de cargar datos")
    parser.add_argument("--anio-desde", type=int, help="Año inicial del rango de carga", required=True)
    parser.add_argument("--anio-hasta", type=int, help="Año final del rango de carga", required=True)
    
    args = parser.parse_args()

    # ==========================================
    # VALIDACIONES DE ARGUMENTOS
    # ==========================================
    
    # Validar años
    if args.anio_desde > args.anio_hasta:
        raise ValueError("El año inicial no puede ser mayor que el año final")
    
    # Validar nivel de log
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Nivel de log inválido: {args.loglevel}')
    
    # Validar que la carpeta existe
    if not os.path.exists(args.input):
        logging.error(f"La carpeta {args.input} no existe")
        return 1
    
    # Validar configuración de Neo4j
    if not validar_neo4j_config(NEO4J_CONFIG):
        logging.error("Configuración de Neo4j inválida. Revisa NEO4J_CONFIG en config.py")
        return 1

    # Configurar logging
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler("carga_tablas.log"),
            logging.StreamHandler()
        ]
    )

    logging.info("=" * 60)
    logging.info("INICIANDO PROCESO DE CARGA UFD - MODO SIMPLIFICADO")
    logging.info("=" * 60)
    logging.info(f"Carpeta de entrada: {args.input}")
    logging.info(f"Nivel de log: {args.loglevel}")
    logging.info(f"Rango de años: {args.anio_desde} - {args.anio_hasta}")
    logging.info(f"Borrar Neo4j: {'SÍ' if args.borrar_neo4j else 'NO'}")
    logging.info(f"Crear relaciones: {'SÍ' if args.crear_relaciones else 'NO'}")

    # ==========================================
    # EJECUCIÓN MODO TRAIN
    # ==========================================
    if args.mode == "train":
        logging.info(f"Modo de ejecución: {args.mode}")
        
        # ==========================================
        # PASO 1: BORRAR BASE DE DATOS Y RELACIONES
        # ==========================================
        if args.borrar_neo4j:
            logging.info("\n" + "=" * 60)
            logging.info("PASO 1: Borrando toda la base de datos Neo4j...")
            logging.info("=" * 60)
            
            resultado_borrado = borrar_toda_bd_neo4j(
                uri=NEO4J_CONFIG["uri"],
                usuario=NEO4J_CONFIG["user"],
                password=NEO4J_CONFIG["password"],
                confirmar=True,
                borrar_props=True 
            )
            
            logging.info("Resumen del borrado:")
            for clave, valor in resultado_borrado.items():
                if clave != "errores":
                    logging.info(f"  {clave}: {valor}")
            
            if resultado_borrado.get("errores"):
                logging.warning(f"Errores en borrado: {len(resultado_borrado['errores'])}")
                for error in resultado_borrado["errores"]:
                    logging.warning(f"  - {error}")
        else:
            logging.info("\n" + "=" * 60)
            logging.info("PASO 1: Manteniendo base de datos existente (sin borrar)")
            logging.info("=" * 60)

        # ==========================================
        # PASO 2: CARGAR TABLAS EN DATAFRAMES
        # ==========================================
        logging.info("\n" + "=" * 60)
        logging.info("PASO 2: Cargando tablas en DataFrames desde archivos PARQUET...")
        logging.info("=" * 60)
        
        # Actualizar rutas de configuración
        data_config_updated = {}
        archivos_faltantes = []
        
        for nombre_tabla, cfg in DATA_CONFIG.items():
            ruta_relativa = cfg["path"]
            ruta_completa = os.path.join(args.input, ruta_relativa)

            if not os.path.exists(ruta_completa):
                archivos_faltantes.append(ruta_relativa)

            cfg_actualizada = cfg.copy()
            cfg_actualizada["path"] = ruta_completa
            data_config_updated[nombre_tabla] = cfg_actualizada
        
        # Mostrar archivos faltantes si los hay
        if archivos_faltantes:
            logging.warning(f"Archivos/carpetas faltantes: {archivos_faltantes}")
            logging.info("Archivos disponibles:")
            for item in os.listdir(args.input):
                item_path = os.path.join(args.input, item)
                if os.path.isdir(item_path):
                    logging.info(f"  {item}/ (carpeta)")
                elif item.endswith('.parquet'):
                    logging.info(f"  {item} (archivo parquet)")
                else:
                    logging.info(f"  {item}")
        
        # Cargar DataFrames con filtro de años
        dfs = load_raw_training_data_parquet(
            data_config_updated,
            anios=list(range(args.anio_desde, args.anio_hasta + 1))
        )
        
        if not dfs:
            logging.error("No se pudieron cargar los DataFrames")
            return 1
        
        logging.info(f"{len(dfs)} DataFrames cargados correctamente para años {args.anio_desde}-{args.anio_hasta}")

        # ==========================================
        # PASO 3: CREAR NODOS Y RELACIONES EN NEO4J
        # ==========================================
        logging.info("\n" + "=" * 60)
        logging.info("PASO 3: Creando nodos y relaciones en Neo4j...")
        logging.info("=" * 60)
        
        # Subfase 3A: Crear nodos
        logging.info("\n--- SUBFASE 3A: Creando nodos ---")
        
        # Filtrar tablas disponibles
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

        logging.info(f"Procesando {len(tablas_disponibles)} tablas en Neo4j...")

        # Procesar tablas (crear nodos)
        resultados = procesar_tablas_neo4j(tablas_disponibles, dfs, NEO4J_CONFIG)

        # Evaluar resultados de creación de nodos
        exitosos = sum(1 for r in resultados.values() if r.get("exito", False))
        fallidos = len(resultados) - exitosos

        if exitosos == 0:
            logging.error("Error: Ninguna tabla se procesó exitosamente")
            return 1

        logging.info(f"{exitosos} tablas procesadas exitosamente (nodos creados)!")
        if fallidos > 0:
            logging.warning(f"{fallidos} tablas fallaron")

        # Subfase 3B: Crear relaciones
        if args.crear_relaciones:
            logging.info("\n--- SUBFASE 3B: Creando relaciones ---")
            
            resultado_relaciones = crear_todas_las_relaciones(NEO4J_CONFIG)
            
            if resultado_relaciones.get('exitosas', 0) > 0:
                exitosas_rel = resultado_relaciones['exitosas']
                total_relaciones = resultado_relaciones['total_relaciones_creadas']
                tiempo_total_rel = resultado_relaciones.get('tiempo_total_segundos', 0)
                
                logging.info(f"Tipos de relaciones creadas: {exitosas_rel}")
                logging.info(f"Total relaciones en BD: {total_relaciones:,}")
                logging.info(f"Tiempo de creación: {tiempo_total_rel}s")
                
                # Mostrar detalle si está disponible
                if resultado_relaciones.get('resultados_detallados'):
                    logging.info("\nDetalle de relaciones creadas:")
                    for nombre, detalle in resultado_relaciones['resultados_detallados'].items():
                        if detalle.get('exito', False):
                            relaciones_creadas = detalle['relaciones_creadas']
                            tiempo_rel = detalle.get('tiempo_segundos', 0)
                            logging.info(f"  {nombre}: {relaciones_creadas:,} relaciones ({tiempo_rel}s)")
                        else:
                            error_msg = detalle.get('error', 'Error desconocido')
                            logging.error(f"  {nombre}: {error_msg}")
                
                if resultado_relaciones.get('fallidas', 0) > 0:
                    fallidas_rel = resultado_relaciones['fallidas']
                    logging.warning(f"Tipos de relaciones fallidas: {fallidas_rel}")
            else:
                logging.error("Error: No se pudieron crear relaciones")
                if "error" in resultado_relaciones:
                    logging.error(f"Detalles: {resultado_relaciones['error']}")
                return 1
        else:
            logging.info("\n--- SUBFASE 3B: Relaciones omitidas (--crear-relaciones no especificado) ---")

        # ==========================================
        # INFORMACIÓN DETALLADA Y RESUMEN FINAL
        # ==========================================
        logging.info("\n" + "=" * 60)
        logging.info("INFORMACIÓN DETALLADA DE TABLAS CARGADAS")
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
                        
                        logging.info(f"\n{nombre_tabla.upper()}:")
                        logging.info(f"  Filas: {filas:,}")
                        logging.info(f"  Columnas: {columnas}")
                        logging.info(f"  Nodos en Neo4j: {nodos_creados:,}")
                        logging.info(f"  Etiqueta: {info['nombre_neo4j']}")
                        
                        # Muestra de datos
                        logging.info("  Muestra de datos:")
                        df.show(3, truncate=True)
                        
                    except Exception as e:
                        logging.error(f"Error mostrando info de {nombre_tabla}: {e}")

        # Resumen final
        logging.info("\n" + "=" * 60)
        logging.info("RESUMEN FINAL")
        logging.info("=" * 60)
        logging.info(f"Carpeta procesada: {args.input}")
        logging.info(f"Años procesados: {args.anio_desde}-{args.anio_hasta}")
        logging.info(f"DataFrames cargados: {len(dfs)}")
        logging.info(f"Tablas exitosas: {exitosos}")
        logging.info(f"Tablas fallidas: {fallidos}")
        
        total_nodos = sum(
            r.get("resultado", {}).get("total_creados", 0) 
            for r in resultados.values() 
            if r.get("exito", False)
        )
        logging.info(f"Total nodos en Neo4j: {total_nodos:,}")
        
        if args.crear_relaciones and 'resultado_relaciones' in locals():
            exitosas_rel = resultado_relaciones.get('exitosas', 0)
            total_relaciones = resultado_relaciones.get('total_relaciones_creadas', 0)
            
            logging.info(f"Tipos de relaciones creadas: {exitosas_rel}")
            logging.info(f"Total relaciones en BD: {total_relaciones:,}")

        logging.info("¡Proceso completado exitosamente!")
        return 0
        
    else:
        logging.error(f"Modo '{args.mode}' no soportado en esta versión simplificada")
        logging.info("Solo se soporta el modo 'train'")
        return 1


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