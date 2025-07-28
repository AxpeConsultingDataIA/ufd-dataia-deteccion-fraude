"""
Script para lectura de datos y generación de features.
Este se ejecutará en su propio contenedor dentro de las
plataformas de entrenamiento e inferencia.
El código incluido en este script es puramente técnico y
no necesita ser cambiado.
"""

# 1. Import necessary libraries
import argparse
import os
import sys
import joblib
import pandas as pd
import numpy as np
import torch
from torch_geometric.data import HeteroData
import json
import logging
from neo4j import GraphDatabase
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
import pickle
from datetime import datetime

from functional_code import (
    Neo4jDataLoaderSageMaker,
    HeterogeneousGraphProcessorSageMaker,
    convert_neo4j_date,
    FRAUD_CLASSES,
)

# 2. Define global variables
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# 3. Functions
def parse_args():
    """
    Parse arguments of SageMaker
    """
    parser = argparse.ArgumentParser()

    # Parámetros de conexión Neo4j
    parser.add_argument(
        "--neo4j-uri", type=str, required=True, help="URI de conexión a Neo4j"
    )
    parser.add_argument(
        "--neo4j-username", type=str, required=True, help="Usuario de Neo4j"
    )
    parser.add_argument(
        "--neo4j-password", type=str, required=True, help="Contraseña de Neo4j"
    )

    # Parámetros de procesamiento
    parser.add_argument(
        "--train-split",
        type=float,
        default=0.6,
        help="Proporción de datos para entrenamiento",
    )
    parser.add_argument(
        "--val-split",
        type=float,
        default=0.2,
        help="Proporción de datos para validación",
    )

    # Rutas de SageMaker
    parser.add_argument(
        "--output-data-dir",
        type=str,
        default=os.environ.get("SM_OUTPUT_DATA_DIR", "/opt/ml/processing"),
    )

    return parser.parse_args()


def create_output_directories(base_path):
    """
    Creates output directories
    """
    directories = {
        "train": os.path.join(base_path, "train"),
        "validation": os.path.join(base_path, "validation"),
        "test": os.path.join(base_path, "test"),
        "report": os.path.join(base_path, "report"),
    }

    for dir_path in directories.values():
        os.makedirs(dir_path, exist_ok=True)

    return directories


def generate_data_report(
    raw_data, train_indices, val_indices, test_indices, output_path
):
    """
    Generates a report of the processed data.
    """

    report = {
        "processing_timestamp": datetime.now().isoformat(),
        "data_summary": {
            "total_nodes": sum(len(nodes) for nodes in raw_data["nodes"].values()),
            "total_edges": sum(len(edges) for edges in raw_data["edges"].values()),
            "node_types": list(raw_data["nodes"].keys()),
            "edge_types": list(raw_data["edges"].keys()),
        },
        "split_summary": {
            "train_size": len(train_indices),
            "validation_size": len(val_indices),
            "test_size": len(test_indices),
            "train_percentage": len(train_indices)
            / (len(train_indices) + len(val_indices) + len(test_indices))
            * 100,
            "validation_percentage": len(val_indices)
            / (len(train_indices) + len(val_indices) + len(test_indices))
            * 100,
            "test_percentage": len(test_indices)
            / (len(train_indices) + len(val_indices) + len(test_indices))
            * 100,
        },
        "fraud_distribution": {},
    }

    # Analizar distribución de fraudes si existe
    if (
        "contador" in raw_data["nodes"]
        and "label" in raw_data["nodes"]["contador"].columns
    ):
        label_counts = raw_data["nodes"]["contador"]["label"].value_counts()
        report["fraud_distribution"] = label_counts.to_dict()

    # Información por tipo de nodo
    for node_type, node_df in raw_data["nodes"].items():
        report[f"{node_type}_features"] = {
            "count": len(node_df),
            "columns": list(node_df.columns),
            "feature_count": len(node_df.columns),
        }

    # Guardar reporte
    with open(os.path.join(output_path, "data_report.json"), "w") as f:
        json.dump(report, f, indent=2)

    logger.info("📊 Reporte de datos generado")
    return report


def main():
    """Función principal del procesamiento de features"""

    logger.info("🚀 Iniciando procesamiento de features desde Neo4j...")

    # Parsear argumentos
    args = parse_args()

    # Crear directorios de salida
    dirs = create_output_directories(args.output_data_dir)

    try:
        # 1. Conectar a Neo4j y cargar datos
        logger.info("🔌 Conectando a Neo4j...")
        loader = Neo4jDataLoaderSageMaker(
            args.neo4j_uri, args.neo4j_username, args.neo4j_password
        )

        raw_data = loader.load_heterogeneous_data()
        loader.close()

        logger.info(f"✅ Datos cargados desde Neo4j")

        # 2. Procesar a formato PyTorch Geometric
        logger.info("🔄 Procesando datos a formato PyTorch Geometric...")
        processor = HeterogeneousGraphProcessorSageMaker()
        hetero_data = processor.create_heterogeneous_graph(raw_data)

        # 3. Realizar split de datos
        logger.info("📊 Dividiendo datos en train/validation/test...")
        num_contadores = hetero_data["contador"].x.size(0)
        indices = torch.randperm(num_contadores)

        train_size = int(args.train_split * num_contadores)
        val_size = int(args.val_split * num_contadores)

        train_indices = indices[:train_size]
        val_indices = indices[train_size : train_size + val_size]
        test_indices = indices[train_size + val_size :]

        # 4. Guardar datasets
        logger.info("💾 Guardando datasets procesados...")

        # Dataset de entrenamiento
        torch.save(
            {
                "hetero_data": hetero_data,
                "indices": train_indices,
                "raw_data": raw_data,
                "metadata": {
                    "split_type": "train",
                    "node_feature_dims": {
                        nt: hetero_data[nt].x.size(1) for nt in hetero_data.node_types
                    },
                    "edge_types": hetero_data.edge_types,
                },
            },
            os.path.join(dirs["train"], "train_data.pt"),
        )

        # Dataset de validación
        torch.save(
            {
                "hetero_data": hetero_data,
                "indices": val_indices,
                "raw_data": raw_data,
                "metadata": {
                    "split_type": "validation",
                    "node_feature_dims": {
                        nt: hetero_data[nt].x.size(1) for nt in hetero_data.node_types
                    },
                    "edge_types": hetero_data.edge_types,
                },
            },
            os.path.join(dirs["validation"], "validation_data.pt"),
        )

        # Dataset de test
        torch.save(
            {
                "hetero_data": hetero_data,
                "indices": test_indices,
                "raw_data": raw_data,
                "metadata": {
                    "split_type": "test",
                    "node_feature_dims": {
                        nt: hetero_data[nt].x.size(1) for nt in hetero_data.node_types
                    },
                    "edge_types": hetero_data.edge_types,
                },
            },
            os.path.join(dirs["test"], "test_data.pt"),
        )

        # 5. Guardar artefactos de preprocesamiento
        logger.info("🔧 Guardando artefactos de preprocesamiento...")
        processor.save_preprocessing_artifacts(dirs["train"])

        # 6. Generar reporte
        logger.info("📋 Generando reporte de datos...")
        report = generate_data_report(
            raw_data, train_indices, val_indices, test_indices, dirs["report"]
        )

        logger.info("✅ Procesamiento de features completado exitosamente")
        logger.info(f"   - Entrenamiento: {len(train_indices)} muestras")
        logger.info(f"   - Validación: {len(val_indices)} muestras")
        logger.info(f"   - Test: {len(test_indices)} muestras")

    except Exception as e:
        logger.error(f"❌ Error en procesamiento de features: {e}")
        raise


if __name__ == "__main__":
    main()

# ===============================
# src/feature_step/functional_code.py
# ===============================

"""
Código funcional que debe aportar el producto y que es
invocado desde el script principal
"""

import pandas as pd
import numpy as np
import torch
from torch_geometric.data import HeteroData
from neo4j import GraphDatabase
from sklearn.preprocessing import StandardScaler, LabelEncoder
from datetime import datetime
import logging
import pickle
import os

logger = logging.getLogger(__name__)

# Constantes
FRAUD_CLASSES = {"NORMAL": 0, "FRAUDE": 1, "IRREGULARIDAD": 2}


def convert_neo4j_date(neo4j_date):
    """Convertir fecha de Neo4j a formato Python"""
    if neo4j_date is None:
        return None

    if hasattr(neo4j_date, "to_native"):
        return neo4j_date.to_native()
    elif hasattr(neo4j_date, "year"):
        return datetime(neo4j_date.year, neo4j_date.month, neo4j_date.day)
    else:
        return str(neo4j_date)


class Neo4jDataLoaderSageMaker:
    """
    Loads heterogeneous data from Neo4j - Versión adaptada para SageMaker
    """

    def __init__(self, uri: str, username: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.logger = logging.getLogger(__name__)

    def close(self):
        """Cerrar conexión"""
        self.driver.close()

    def load_heterogeneous_data(self):
        """Cargar todos los datos heterogéneos desde Neo4j"""
        logger.info("🔌 Cargando datos desde Neo4j...")

        with self.driver.session() as session:
            # Cargar cada tipo de nodo
            node_data = {
                "contador": self._load_contador_nodes(session),
                "suministro": self._load_suministro_nodes(session),
                "comercializadora": self._load_comercializadora_nodes(session),
                "ubicacion": self._load_ubicacion_nodes(session),
                "concentrador": self._load_concentrador_nodes(session),
                "expediente_fraude": self._load_expediente_fraude_nodes(session),
            }

            # Cargar relaciones
            edge_data = self._load_heterogeneous_edges(session, node_data)

            # Asignar etiquetas de fraude basadas en expedientes
            node_data["contador"] = self._assign_fraud_labels_from_expedientes(
                node_data["contador"], node_data["expediente_fraude"], session
            )

        # Mostrar estadísticas
        total_nodes = sum(len(nodes) for nodes in node_data.values())
        total_edges = sum(len(edges) for edges in edge_data.values())
        logger.info(
            f"✅ Cargados {total_nodes} nodos y {total_edges} relaciones desde Neo4j"
        )

        for node_type, df in node_data.items():
            logger.info(f"   - {node_type}: {len(df)} nodos")

        return {"nodes": node_data, "edges": edge_data}

    def _load_contador_nodes(self, session):
        """Cargar nodos CONTADOR desde Neo4j"""

        query = """
        MATCH (c:CONTADOR)
        OPTIONAL MATCH (c)-[:GENERA_MEDICION]->(m:MEDICION)
        WITH c, 
             COUNT(m) as num_mediciones,
             AVG(m.energia_activa) as consumo_promedio,
             MAX(m.energia_activa) as consumo_maximo,
             MIN(m.energia_activa) as consumo_minimo,
             STDEV(m.energia_activa) as variabilidad_consumo
        RETURN c.nis_rad as nis_rad,
               c.numero_contador as numero_contador,
               c.marca_contador as marca,
               c.modelo_contador as modelo,
               c.tipo_aparato as tipo_aparato,
               c.telegest_activo as telegest_activo,
               c.estado_tg as estado_comunicacion,
               c.tension as tension_nominal,
               c.fases_contador as fases,
               c.potencia_maxima as potencia_maxima,
               c.fecha_instalacion as fecha_instalacion,
               c.version_firmware as version_firmware,
               c.estado_contrato as estado_contrato,
               COALESCE(num_mediciones, 0) as num_mediciones,
               COALESCE(consumo_promedio, 0) as consumo_promedio_diario,
               COALESCE(consumo_maximo, 0) as consumo_maximo_registrado,
               COALESCE(consumo_minimo, 0) as consumo_minimo_registrado,
               COALESCE(variabilidad_consumo, 0) as variabilidad_consumo
        ORDER BY c.nis_rad
        """

        result = session.run(query)
        records = [dict(record) for record in result]

        if not records:
            logger.warning("⚠️ No se encontraron CONTADORES.")
            return pd.DataFrame()

        df = pd.DataFrame(records)

        # Procesar fechas
        df["fecha_instalacion"] = df["fecha_instalacion"].map(convert_neo4j_date)
        df["fecha_instalacion"] = pd.to_datetime(
            df["fecha_instalacion"], errors="coerce"
        )

        # Calcular días desde instalación
        df["dias_desde_instalacion"] = (
            pd.Timestamp.now().normalize() - df["fecha_instalacion"]
        ).dt.days.fillna(0)

        # Crear ID único
        df["node_id"] = df["nis_rad"]
        df.reset_index(drop=True, inplace=True)

        logger.info(f"✅ Cargados {len(df)} contadores")
        return df

    def _load_suministro_nodes(self, session):
        """Cargar nodos SUMINISTRO desde Neo4j"""

        query = """
        MATCH (s:SUMINISTRO)
        RETURN s.nis_rad as nis_rad,
               s.fecha_alta_suministro as fecha_alta,
               s.estado_contrato as estado_contrato,
               s.tipo_punto as tipo_suministro,
               s.potencia_contratada as potencia_contratada,
               s.potencia_maxima as potencia_maxima_demandada,
               s.tarifa_activa as tarifa_activa,
               s.tension_suministro as tension_suministro,
               s.fases_suministro as fases_suministro,
               s.cnae as cnae,
               s.comercializadora_codigo as comercializadora_codigo
        ORDER BY s.nis_rad
        """

        result = session.run(query)
        records = [dict(record) for record in result]

        df = pd.DataFrame(records)
        if not df.empty:
            df["node_id"] = df["nis_rad"]
            df.reset_index(drop=True, inplace=True)

        logger.info(f"✅ Cargados {len(df)} suministros")
        return df

    def _load_comercializadora_nodes(self, session):
        """Cargar nodos COMERCIALIZADORA desde Neo4j"""

        query = """
        MATCH (c:COMERCIALIZADORA)
        RETURN c.codigo_comercializadora as codigo_comercializadora,
               c.nombre_comercializadora as nombre_comercializadora
        ORDER BY c.codigo_comercializadora
        """

        result = session.run(query)
        records = [dict(record) for record in result]

        df = pd.DataFrame(records)
        if not df.empty:
            df["node_id"] = df["codigo_comercializadora"]
            df.reset_index(drop=True, inplace=True)

        logger.info(f"✅ Cargadas {len(df)} comercializadoras")
        return df

    def _load_ubicacion_nodes(self, session):
        """Cargar nodos UBICACION desde Neo4j"""

        query = """
        MATCH (u:UBICACION)
        RETURN u.coordenada_x as coordenada_x,
               u.coordenada_y as coordenada_y,
               u.codigo_postal as codigo_postal,
               u.area_ejecucion as area_ejecucion,
               toString(u.coordenada_x) + '_' + toString(u.coordenada_y) as node_id
        ORDER BY u.coordenada_x, u.coordenada_y
        """

        result = session.run(query)
        records = [dict(record) for record in result]

        df = pd.DataFrame(records)
        if not df.empty:
            df.reset_index(drop=True, inplace=True)

        logger.info(f"✅ Cargadas {len(df)} ubicaciones")
        return df

    def _load_concentrador_nodes(self, session):
        """Cargar nodos CONCENTRADOR desde Neo4j"""

        query = """
        MATCH (c:CONCENTRADOR)
        RETURN c.concentrador_id as concentrador_id,
               c.version_concentrador as version_concentrador,
               c.estado_comunicacion as estado_comunicacion,
               c.tipo_reporte as tipo_reporte
        ORDER BY c.concentrador_id
        """

        result = session.run(query)
        records = [dict(record) for record in result]

        df = pd.DataFrame(records)
        if not df.empty:
            df["node_id"] = df["concentrador_id"]
            df.reset_index(drop=True, inplace=True)

        logger.info(f"✅ Cargados {len(df)} concentradores")
        return df

    def _load_expediente_fraude_nodes(self, session):
        """Cargar nodos EXPEDIENTE_FRAUDE desde Neo4j"""

        query = """
        MATCH (e:EXPEDIENTE_FRAUDE)
        RETURN e.nis_expediente as nis_expediente,
               e.clasificacion_fraude as clasificacion_fraude,
               e.tipo_anomalia as tipo_anomalia,
               e.estado_expediente as estado_expediente,
               e.fecha_acta as fecha_acta,
               e.fecha_inicio_anomalia as fecha_inicio_anomalia,
               e.fecha_fin_anomalia as fecha_fin_anomalia,
               e.energia_liquidable as energia_liquidable,
               e.valoracion_total as valoracion_total,
               e.dias_liquidables as dias_liquidables,
               e.porcentaje_liquidable as porcentaje_liquidable
        ORDER BY e.nis_expediente
        """

        result = session.run(query)
        records = [dict(record) for record in result]

        df = pd.DataFrame(records)
        if not df.empty:
            df["node_id"] = df["nis_expediente"]
            df.reset_index(drop=True, inplace=True)

        logger.info(f"✅ Cargados {len(df)} expedientes de fraude")
        return df

    def _load_heterogeneous_edges(self, session, node_data):
        """Cargar todas las relaciones heterogéneas desde Neo4j"""

        logger.info("🔗 Cargando relaciones desde Neo4j...")

        edge_data = {}

        # 1. CONTADOR -> SUMINISTRO
        edge_data[("contador", "mide", "suministro")] = self._load_edges_from_query(
            session,
            """
            MATCH (c:CONTADOR)-[:MIDE_CONSUMO_DE]->(s:SUMINISTRO)
            RETURN c.nis_rad as source, s.nis_rad as target
            """,
            node_data["contador"],
            node_data["suministro"],
        )

        # 2. CONTADOR -> CONCENTRADOR
        edge_data[("contador", "comunica_via", "concentrador")] = (
            self._load_edges_from_query(
                session,
                """
            MATCH (c:CONTADOR)-[:CONECTADO_A]->(con:CONCENTRADOR)
            RETURN c.nis_rad as source, con.concentrador_id as target
            """,
                node_data["contador"],
                node_data["concentrador"],
            )
        )

        # 3. CONTADOR -> UBICACION
        edge_data[("contador", "ubicado_en", "ubicacion")] = (
            self._load_edges_from_query(
                session,
                """
            MATCH (c:CONTADOR)-[:INSTALADO_EN]->(u:UBICACION)
            RETURN c.nis_rad as source, 
                   toString(u.coordenada_x) + '_' + toString(u.coordenada_y) as target
            """,
                node_data["contador"],
                node_data["ubicacion"],
            )
        )

        # 4. CONTADOR -> EXPEDIENTE_FRAUDE
        if not node_data["expediente_fraude"].empty:
            edge_data[("contador", "involucrado_en", "expediente_fraude")] = (
                self._load_edges_from_query(
                    session,
                    """
                MATCH (c:CONTADOR)-[:INVOLUCRADO_EN_FRAUDE]->(e:EXPEDIENTE_FRAUDE)
                RETURN c.nis_rad as source, e.nis_expediente as target
                """,
                    node_data["contador"],
                    node_data["expediente_fraude"],
                )
            )

        # 5. SUMINISTRO -> COMERCIALIZADORA
        edge_data[("suministro", "contratado_con", "comercializadora")] = (
            self._create_suministro_comercializadora_edges(
                node_data["suministro"], node_data["comercializadora"]
            )
        )

        # 6. Relaciones de proximidad entre CONTADORES
        edge_data[("contador", "cerca_de", "contador")] = (
            self._create_contador_proximidad_edges(
                node_data["contador"], node_data["ubicacion"]
            )
        )

        # 7. Contadores similares
        edge_data[("contador", "similar_a", "contador")] = (
            self._create_contador_similar_edges(node_data["contador"])
        )

        return edge_data

    def _load_edges_from_query(self, session, query, source_df, target_df):
        """Cargar relaciones desde Neo4j usando query específica"""

        result = session.run(query)
        edges = []

        # Crear mapeos de node_id a índice
        source_map = {node_id: idx for idx, node_id in enumerate(source_df["node_id"])}
        target_map = {node_id: idx for idx, node_id in enumerate(target_df["node_id"])}

        for record in result:
            source_id = record["source"]
            target_id = record["target"]

            if source_id in source_map and target_id in target_map:
                source_idx = source_map[source_id]
                target_idx = target_map[target_id]
                edges.append([source_idx, target_idx])

        # Si no hay relaciones, crear algunas básicas para testing
        if not edges and len(source_df) > 0 and len(target_df) > 0:
            logger.warning(
                "⚠️ No se encontraron relaciones reales, creando relaciones de prueba..."
            )
            for i in range(min(len(source_df), len(target_df))):
                target_idx = i % len(target_df)
                edges.append([i, target_idx])

        return edges

    def _create_suministro_comercializadora_edges(self, suministros, comercializadoras):
        """Crear relaciones SUMINISTRO -> COMERCIALIZADORA basadas en código"""

        edges = []
        com_map = {
            row["codigo_comercializadora"]: idx
            for idx, row in comercializadoras.iterrows()
        }

        for idx, row in suministros.iterrows():
            com_codigo = row.get("comercializadora_codigo", "COM_001")
            if com_codigo in com_map:
                edges.append([idx, com_map[com_codigo]])
            else:
                # Asignar aleatoriamente si no existe la comercializadora
                if len(comercializadoras) > 0:
                    com_idx = np.random.randint(0, len(comercializadoras))
                    edges.append([idx, com_idx])

        return edges

    def _create_contador_proximidad_edges(self, contadores, ubicaciones):
        """Crear relaciones de proximidad entre contadores"""

        edges = []
        # Simular proximidad: cada contador se conecta con 2-4 vecinos
        for i in range(len(contadores)):
            num_neighbors = np.random.randint(2, 5)
            for _ in range(num_neighbors):
                neighbor_idx = np.random.randint(0, len(contadores))
                if neighbor_idx != i:
                    edges.append([i, neighbor_idx])

        return edges

    def _create_contador_similar_edges(self, contadores):
        """Crear relaciones entre contadores similares (misma marca/modelo)"""

        edges = []

        # Agrupar por marca y modelo
        for marca in contadores["marca"].unique():
            for modelo in contadores["modelo"].unique():
                subset_indices = contadores[
                    (contadores["marca"] == marca) & (contadores["modelo"] == modelo)
                ].index.tolist()

                # Conectar contadores del mismo tipo
                for i in range(len(subset_indices)):
                    for j in range(i + 1, min(i + 4, len(subset_indices))):
                        edges.append([subset_indices[i], subset_indices[j]])
                        edges.append([subset_indices[j], subset_indices[i]])

        return edges

    def _assign_fraud_labels_from_expedientes(self, contadores, expedientes, session):
        """Asignar etiquetas de fraude basadas en expedientes reales"""

        contadores = contadores.copy()

        # Inicializar todas las etiquetas como NORMAL
        contadores["label"] = "NORMAL"

        if not expedientes.empty:
            # Obtener relaciones contador -> expediente
            query = """
            MATCH (c:CONTADOR)-[:INVOLUCRADO_EN_FRAUDE]->(e:EXPEDIENTE_FRAUDE)
            RETURN c.nis_rad as contador_nis, e.clasificacion_fraude as clasificacion
            """

            result = session.run(query)
            fraud_relations = [dict(record) for record in result]

            # Crear mapeo de NIS a índice
            nis_to_idx = {row["nis_rad"]: idx for idx, row in contadores.iterrows()}

            # Asignar etiquetas basadas en clasificación de fraude
            fraud_count = 0
            irregularity_count = 0

            for relation in fraud_relations:
                contador_nis = relation["contador_nis"]
                clasificacion = relation["clasificacion"]

                if contador_nis in nis_to_idx:
                    idx = nis_to_idx[contador_nis]

                    if clasificacion and "FRAUDE" in clasificacion.upper():
                        contadores.loc[idx, "label"] = "FRAUDE"
                        fraud_count += 1
                    elif clasificacion and any(
                        term in clasificacion.upper()
                        for term in ["IRREGULARIDAD", "ANOMALIA"]
                    ):
                        contadores.loc[idx, "label"] = "IRREGULARIDAD"
                        irregularity_count += 1

            normal_count = len(contadores) - fraud_count - irregularity_count
            logger.info(
                f"✅ Etiquetas desde BD: {normal_count} Normal, {fraud_count} Fraude, {irregularity_count} Irregularidad"
            )

        else:
            # Si no hay expedientes, simular algunas etiquetas para prueba
            logger.warning(
                "⚠️ No se encontraron expedientes de fraude, simulando etiquetas..."
            )
            num_fraudes = max(1, int(len(contadores) * 0.05))  # 5% fraudes
            num_irregularidades = max(
                1, int(len(contadores) * 0.03)
            )  # 3% irregularidades

            fraud_indices = np.random.choice(
                len(contadores), num_fraudes, replace=False
            )
            remaining_indices = [
                i for i in range(len(contadores)) if i not in fraud_indices
            ]
            irregularity_indices = np.random.choice(
                remaining_indices, num_irregularidades, replace=False
            )

            for idx in fraud_indices:
                contadores.loc[idx, "label"] = "FRAUDE"

            for idx in irregularity_indices:
                contadores.loc[idx, "label"] = "IRREGULARIDAD"

            normal_count = len(contadores) - num_fraudes - num_irregularidades
            logger.info(
                f"✅ Etiquetas simuladas: {normal_count} Normal, {num_fraudes} Fraude, {num_irregularidades} Irregularidad"
            )

        return contadores


class HeterogeneousGraphProcessorSageMaker:
    """
    Procesador para convertir datos de Neo4j en formato PyTorch Geometric
    Versión adaptada para SageMaker
    """

    def __init__(self):
        self.scalers = {}
        self.label_encoders = {}

    def create_heterogeneous_graph(self, data):
        """
        Convierte datos heterogéneos en un objeto HeteroData de PyTorch Geometric
        """

        logger.info("🔄 Procesando grafo heterogéneo desde Neo4j...")

        hetero_data = HeteroData()

        # Procesar cada tipo de nodo
        for node_type, node_df in data["nodes"].items():
            if len(node_df) == 0:
                logger.warning(f"⚠️ Saltando nodos tipo: {node_type} (vacío)")
                continue

            logger.info(f"   Procesando nodos tipo: {node_type}")

            # Procesar características del nodo
            x, feature_names = self._process_node_features(node_df, node_type)
            hetero_data[node_type].x = torch.tensor(x, dtype=torch.float)

            # Guardar nombres de características para análisis posterior
            hetero_data[node_type].feature_names = feature_names

            # Para CONTADORES, agregar etiquetas de fraude
            if node_type == "contador" and "label" in node_df.columns:
                labels = [FRAUD_CLASSES[label] for label in node_df["label"]]
                hetero_data[node_type].y = torch.tensor(labels, dtype=torch.long)

            logger.info(f"     ✅ {len(node_df)} nodos, {x.shape[1]} características")

        # Procesar relaciones heterogéneas
        for edge_type, edge_list in data["edges"].items():
            if len(edge_list) > 0:
                src_type, relation, dst_type = edge_type

                # Verificar que ambos tipos de nodo existen
                if (
                    src_type in hetero_data.node_types
                    and dst_type in hetero_data.node_types
                ):
                    edge_index = (
                        torch.tensor(edge_list, dtype=torch.long).t().contiguous()
                    )
                    hetero_data[src_type, relation, dst_type].edge_index = edge_index
                    logger.info(
                        f"   ✅ {relation}: {len(edge_list)} relaciones ({src_type} -> {dst_type})"
                    )
                else:
                    logger.warning(
                        f"   ⚠️ Saltando relación {relation}: tipos de nodo faltantes"
                    )

        logger.info(
            f"✅ Grafo heterogéneo creado con {len(data['nodes'])} tipos de nodos"
        )

        return hetero_data

    def _process_node_features(self, node_df, node_type):
        """
        Procesar características de un tipo de nodo.
        """

        # Excluir columnas no-feature
        exclude_cols = [
            "node_id",
            "label",
            "nis_rad",
            "nis_expediente",
            "codigo_comercializadora",
            "concentrador_id",
        ]
        feature_cols = [col for col in node_df.columns if col not in exclude_cols]

        processed_features = []
        feature_names = []

        for col in feature_cols:
            try:
                if node_df[col].dtype in ["object", "bool"]:
                    # Variables categóricas
                    if node_df[col].dtype == "bool":
                        # Variables booleanas
                        processed_features.append(
                            node_df[col].astype(int).values.reshape(-1, 1)
                        )
                        feature_names.append(f"{col}")
                    else:
                        # Variables categóricas -> One-hot encoding
                        # Limitar número de categorías para evitar dimensiones muy grandes
                        unique_values = node_df[col].nunique()
                        if unique_values > 10:
                            # Si hay muchas categorías, usar solo top-10
                            top_categories = node_df[col].value_counts().head(10).index
                            node_df_temp = node_df[col].copy()
                            node_df_temp[~node_df_temp.isin(top_categories)] = "OTHER"
                            encoded = pd.get_dummies(node_df_temp, prefix=col)
                        else:
                            encoded = pd.get_dummies(node_df[col], prefix=col)

                        processed_features.append(encoded.values)
                        feature_names.extend(encoded.columns.tolist())

                elif node_df[col].dtype in ["datetime64[ns]", "<M8[ns]"]:
                    # Variables temporales -> características numéricas
                    try:
                        days_since = (
                            (datetime.now() - pd.to_datetime(node_df[col]))
                            .dt.days.fillna(0)
                            .values.reshape(-1, 1)
                        )
                        processed_features.append(days_since)
                        feature_names.append(f"{col}_days_since")
                    except:
                        # Si falla la conversión temporal, usar valor por defecto
                        default_values = np.zeros((len(node_df), 1))
                        processed_features.append(default_values)
                        feature_names.append(f"{col}_days_since")

                else:
                    # Características numéricas -> normalización estándar
                    scaler_key = f"{node_type}_{col}"
                    if scaler_key not in self.scalers:
                        self.scalers[scaler_key] = StandardScaler()

                    # Convertir a numérico y manejar NaNs
                    values = (
                        pd.to_numeric(node_df[col], errors="coerce")
                        .fillna(0)
                        .values.reshape(-1, 1)
                    )

                    # Verificar si hay varianza en los datos
                    if np.std(values) > 0:
                        normalized = self.scalers[scaler_key].fit_transform(values)
                    else:
                        normalized = (
                            values  # Si no hay varianza, mantener valores originales
                        )

                    processed_features.append(normalized)
                    feature_names.append(f"{col}_normalized")

            except Exception as e:
                logger.warning(f"   ⚠️ Error procesando columna {col}: {e}")
                # Crear características por defecto en caso de error
                default_values = np.zeros((len(node_df), 1))
                processed_features.append(default_values)
                feature_names.append(f"{col}_default")

        # Concatenar todas las características
        if processed_features:
            final_features = np.concatenate(processed_features, axis=1)
        else:
            final_features = np.zeros((len(node_df), 1))
            feature_names = ["dummy_feature"]

        # Verificar dimensiones
        logger.info(f"     Características finales: {final_features.shape}")

        return final_features, feature_names

    def save_preprocessing_artifacts(self, output_path):
        """Guardar artefactos de preprocesamiento"""
        artifacts = {
            "scalers": self.scalers,
            "label_encoders": self.label_encoders,
            "fraud_classes": FRAUD_CLASSES,
        }

        with open(os.path.join(output_path, "preprocessing_artifacts.pkl"), "wb") as f:
            pickle.dump(artifacts, f)

        logger.info("🔧 Artefactos de preprocesamiento guardados")
