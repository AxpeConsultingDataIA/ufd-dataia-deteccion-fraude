# 1. Import necessary libraries
from datetime import datetime, date
import pandas as pd
from sklearn.preprocessing import StandardScaler
import torch
from torch_geometric.data import HeteroData
from neo4j.time import Date
import logging
from typing import Dict, List
import pandas as pd
import numpy as np
import logging

from neo4j import GraphDatabase

# 2. Define global variables
logger = logging.getLogger(__name__)


# 3. Functions/Classes
def convert_neo4j_date(x):
    """
    Aux function used to convert a neo4j date type column into a datetime type of column

    Parameters:
        - x: The desired date to modify.

    Returns:
        - x: The date with its new format.
    """
    if isinstance(x, Date):
        return date(x.year, x.month, x.day)
    elif isinstance(x, datetime):
        return x.date()
    elif isinstance(x, date):
        return x
    else:
        return pd.NaT


class Neo4jDataLoader:
    """
    Loads heterogeneous data from Neo4j.
    """

    def __init__(self, uri: str, username: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.logger = logging.getLogger(__name__)

    def close(self):
        """
        Used to close the connection
        """
        self.driver.close()

    def load_heterogeneous_data(self) -> Dict:
        """
        Loads all the heterogeneous data from Neo4j. The idea is to use all the defined functions for each node.
        """
        print("🔌 Connecting to Neo4j and loading data ...")

        with self.driver.session() as session:
            # Load each type of node
            node_data = {
                "contador": self._load_contador_nodes(session),
                "suministro": self._load_suministro_nodes(session),
                "comercializadora": self._load_comercializadora_nodes(session),
                "ubicacion": self._load_ubicacion_nodes(session),
                "concentrador": self._load_concentrador_nodes(session),
                "expediente_fraude": self._load_expediente_fraude_nodes(session),
            }

            # Load relations
            edge_data = self._load_heterogeneous_edges(session, node_data)

            # Assign fraud labels based on expedientes.
            node_data["contador"] = self._assign_fraud_labels_from_expedientes(
                node_data["contador"], node_data["expediente_fraude"], session
            )

        # Let's show now some statistics of the graph
        total_nodes = sum(len(nodes) for nodes in node_data.values())
        total_edges = sum(len(edges) for edges in edge_data.values())
        print(f"✅ Loaded {total_nodes} nodes and {total_edges} relations from Neo4j")

        for node_type, df in node_data.items():
            print(f"   - {node_type}: {len(df)} nodes")

        return {"nodes": node_data, "edges": edge_data}

    def _load_contador_nodes(self, session) -> pd.DataFrame:
        """
        Loads CONTADOR nodes from Neo4j. CONTADOR nodes, refer to the nis_rad column in our data.
        """

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
        # Run the query just defined:
        result = session.run(query)
        records = [dict(record) for record in result]

        if not records:
            print("⚠️ We couldn't find CONTADORES.")

        df = pd.DataFrame(records)

        # Add calculated features
        # First, we are going to modify the fecha_instalacion date into a datetime format.
        df["fecha_instalacion"] = df["fecha_instalacion"].map(convert_neo4j_date)
        df["fecha_instalacion"] = pd.to_datetime(df["fecha_instalacion"])

        # Calculate the number of days from the instalation date:
        df["dias_desde_instalacion"] = (
            pd.Timestamp.now().normalize() - df["fecha_instalacion"]
        ).dt.days.fillna(0)

        # Let's create a unique ID for the mapping.
        df["node_id"] = df["nis_rad"]
        df.reset_index(drop=True, inplace=True)

        return df

    def _load_suministro_nodes(self, session) -> pd.DataFrame:
        """Loads SUMINISTRO nodes from Neo4j"""

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
        df["node_id"] = df["nis_rad"]

        df.reset_index(drop=True, inplace=True)
        return df

    def _load_comercializadora_nodes(self, session) -> pd.DataFrame:
        """
        Loads COMERCIALIZADORA nodes from Neo4j
        """

        query = """
        MATCH (c:COMERCIALIZADORA)
        RETURN c.codigo_comercializadora as codigo_comercializadora,
               c.nombre_comercializadora as nombre_comercializadora
        ORDER BY c.codigo_comercializadora
        """

        result = session.run(query)
        records = [dict(record) for record in result]

        df = pd.DataFrame(records)
        df["node_id"] = df["codigo_comercializadora"]

        df.reset_index(drop=True, inplace=True)
        return df

    def _load_ubicacion_nodes(self, session) -> pd.DataFrame:
        """
        Loads UBICACION nodes from Neo4j
        """

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

        df.reset_index(drop=True, inplace=True)
        return df

    def _load_concentrador_nodes(self, session) -> pd.DataFrame:
        """
        Loads CONCENTRADOR nodes from Neo4j
        """

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
        df["node_id"] = df["concentrador_id"]

        df.reset_index(drop=True, inplace=True)
        return df

    def _load_expediente_fraude_nodes(self, session) -> pd.DataFrame:
        """
        Loads EXPEDIENTE_FRAUDE nodes from Neo4j
        """

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

        return df

    def _load_heterogeneous_edges(self, session, node_data: Dict) -> Dict:
        """
        Loads all the heterogeneous relations from Neo4j
        """

        print("🔗 Loading relations from Neo4j...")

        edge_data = {}

        # 1. CONTADOR -> SUMINISTRO (relation MIDE_CONSUMO_DE)
        edge_data[("contador", "mide", "suministro")] = self._load_edges_from_query(
            session,
            """
            MATCH (c:CONTADOR)-[:MIDE_CONSUMO_DE]->(s:SUMINISTRO)
            RETURN c.nis_rad as source, s.nis_rad as target
            """,
            node_data["contador"],
            node_data["suministro"],
        )

        # 2. CONTADOR -> CONCENTRADOR (relation CONECTADO_A)
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

        # 3. CONTADOR -> UBICACION (relation INSTALADO_EN)
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

        # 4. CONTADOR -> EXPEDIENTE_FRAUDE (relation INVOLUCRADO_EN_FRAUDE)
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

        # 5. SUMINISTRO -> COMERCIALIZADORA (basado en código comercializadora)
        edge_data[("suministro", "contratado_con", "comercializadora")] = (
            self._create_suministro_comercializadora_edges(
                node_data["suministro"], node_data["comercializadora"]
            )
        )

        # 6. Proximity relations between CONTADORES (These ones are simulated)
        edge_data[("contador", "cerca_de", "contador")] = (
            self._create_contador_proximidad_edges(
                node_data["contador"], node_data["ubicacion"]
            )
        )

        # 7. Similar CONTADORES in terms of MARCA/MODELO
        edge_data[("contador", "similar_a", "contador")] = (
            self._create_contador_similar_edges(node_data["contador"])
        )

        return edge_data

    def _load_edges_from_query(
        self, session, query: str, source_df: pd.DataFrame, target_df: pd.DataFrame
    ) -> List[List[int]]:
        """
        Loads relations from Neo4j using an specific query.
        """

        result = session.run(query)
        edges = []

        # We create mappings from node_id to index
        source_map = {node_id: idx for idx, node_id in enumerate(source_df["node_id"])}
        target_map = {node_id: idx for idx, node_id in enumerate(target_df["node_id"])}

        for record in result:
            source_id = record["source"]
            target_id = record["target"]

            if source_id in source_map and target_id in target_map:
                source_idx = source_map[source_id]
                target_idx = target_map[target_id]
                edges.append([source_idx, target_idx])

        # If there are no relations in the db, we create some basic ones for testing
        if not edges and len(source_df) > 0 and len(target_df) > 0:
            print(f"   ⚠️ We couldn't find real relations, creating test relations...")
            for i in range(min(len(source_df), len(target_df))):
                target_idx = i % len(target_df)
                edges.append([i, target_idx])

        return edges

    def _create_suministro_comercializadora_edges(
        self, suministros: pd.DataFrame, comercializadoras: pd.DataFrame
    ) -> List[List[int]]:
        """
        Creates relations SUMINISTRO -> COMERCIALIZADORA based on the code.
        """

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
                # Assign randomly if the COMERCIALIZADORA does not exist
                com_idx = np.random.randint(0, len(comercializadoras))
                edges.append([idx, com_idx])

        return edges

    def _create_contador_proximidad_edges(
        self, contadores: pd.DataFrame, ubicaciones: pd.DataFrame
    ) -> List[List[int]]:
        """
        Creates relation of proximity between CONTADORES.
        """

        edges = []
        # We simulate proximity: each CONTADOR connects with 2-4 neighbours
        for i in range(len(contadores)):
            num_neighbors = np.random.randint(2, 5)
            for _ in range(num_neighbors):
                neighbor_idx = np.random.randint(0, len(contadores))
                if neighbor_idx != i:
                    edges.append([i, neighbor_idx])

        return edges

    def _create_contador_similar_edges(
        self, contadores: pd.DataFrame
    ) -> List[List[int]]:
        """
        Creates relation between CONTADORES. The relation is based on similarity, same
        MARCA/MODELO
        """

        edges = []

        # We group CONTADORES by MARCA and MODELO
        for marca in contadores["marca"].unique():
            for modelo in contadores["modelo"].unique():
                subset_indices = contadores[
                    (contadores["marca"] == marca) & (contadores["modelo"] == modelo)
                ].index.tolist()

                # Create connection of same type of CONTADORES
                for i in range(len(subset_indices)):
                    for j in range(i + 1, min(i + 4, len(subset_indices))):
                        edges.append([subset_indices[i], subset_indices[j]])
                        edges.append([subset_indices[j], subset_indices[i]])

        return edges

    def _assign_fraud_labels_from_expedientes(
        self, contadores: pd.DataFrame, expedientes: pd.DataFrame, session
    ) -> pd.DataFrame:
        """
        Function in charge of assigning fraud labels based on real EXPEDIENTES.
        """

        contadores = contadores.copy()

        # Initialize all the labels as NORMAL, then we will be adding more labels
        contadores["label"] = "NORMAL"

        if not expedientes.empty:
            # Obtain relations CONTADOR -> EXPEDIENTE
            query = """
            MATCH (c:CONTADOR)-[:INVOLUCRADO_EN_FRAUDE]->(e:EXPEDIENTE_FRAUDE)
            RETURN c.nis_rad as contador_nis, e.clasificacion_fraude as clasificacion
            """

            result = session.run(query)
            fraud_relations = [dict(record) for record in result]

            # Create the mapping of NIS into index
            nis_to_idx = {row["nis_rad"]: idx for idx, row in contadores.iterrows()}

            # Assign labels based on the fraud classification.
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
            print(
                f"✅ Labels from DB: {normal_count} Normal, {fraud_count} Fraud, {irregularity_count} Irregular"
            )
        else:
            logger.error(
                "ERROR: We couldn't load any 'expedientes'. Check the database"
            )

        return contadores


class HeterogeneousGraphProcessorSageMaker:
    """
    Processor in order to transform Neo4j data into PyTorch Geomtric format.
    We adapted the version for Sagemaker.
    """

    def __init__(self):
        self.scalers = {}
        self.label_encoders = {}

    def create_heterogeneous_graph(self, data):
        """
        Transforms heterogeneous data into a PyTorch Geometric HeteroData object
        """

        logger.info("🔄 Processing the heterogeneous graph from Neo4j...")

        hetero_data = HeteroData()

        # Process each node type
        for node_type, node_df in data["nodes"].items():
            if len(node_df) == 0:
                logger.warning(f"   ⚠️ Skipping nodes: {node_type} (empty)")
                continue

            logger.info(f"   Processing nodes: {node_type}")

            # Process features of the node
            x, feature_names = self._process_node_features(node_df, node_type)
            hetero_data[node_type].x = torch.tensor(x, dtype=torch.float)

            # Save the name of the feature for future analysis
            hetero_data[node_type].feature_names = feature_names

            # For CONTADORES, add fraud labels
            if node_type == "contador" and "label" in node_df.columns:
                labels = [FRAUD_CLASSES[label] for label in node_df["label"]]
                hetero_data[node_type].y = torch.tensor(labels, dtype=torch.long)

            logger.info(f"     ✅ {len(node_df)} nodes, {x.shape[1]} features")

        # Process heterogeneous relations
        for edge_type, edge_list in data["edges"].items():
            if len(edge_list) > 0:
                src_type, relation, dst_type = edge_type

                # Verify that both node types exist
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
                        f"   ⚠️ Skipping relation {relation}: type of nodes remaining"
                    )

        logger.info(
            f"✅ Heterogeneous graph created with {len(data['nodes'])} type of nodes"
        )

        return hetero_data

    def _process_node_features(self, node_df, node_type):
        """
        Process features of a node type
        """

        # Exclude non-feature columns
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
                    # Categorical variables
                    if node_df[col].dtype == "bool":
                        # Boolean variables
                        processed_features.append(
                            node_df[col].astype(int).values.reshape(-1, 1)
                        )
                        feature_names.append(f"{col}")
                    else:
                        # Categorical variables -> One-hot encoding
                        # Limit the number of categories in order to avoid too big dimensions
                        unique_values = node_df[col].nunique()
                        if unique_values > 10:
                            # Only top10 in case we have more than 10 categories
                            top_categories = node_df[col].value_counts().head(10).index
                            node_df_temp = node_df[col].copy()
                            # We set the remaining to "OTHER"
                            node_df_temp[~node_df_temp.isin(top_categories)] = "OTHER"
                            encoded = pd.get_dummies(node_df_temp, prefix=col)
                        else:
                            encoded = pd.get_dummies(node_df[col], prefix=col)

                        processed_features.append(encoded.values)
                        feature_names.extend(encoded.columns.tolist())

                elif node_df[col].dtype in ["datetime64[ns]", "<M8[ns]"]:
                    try:
                        days_since = (
                            (datetime.now() - pd.to_datetime(node_df[col]))
                            .dt.days.fillna(0)
                            .values.reshape(-1, 1)
                        )
                        processed_features.append(days_since)
                        feature_names.append(f"{col}_days_since")
                    except:
                        # If the temporal transformation fails, use values by default
                        default_values = np.zeros((len(node_df), 1))
                        processed_features.append(default_values)
                        feature_names.append(f"{col}_days_since")

                else:
                    # Numerical features -> StandardScaler
                    scaler_key = f"{node_type}_{col}"
                    if scaler_key not in self.scalers:
                        self.scalers[scaler_key] = StandardScaler()

                    # Transform to numeric and deal with nulls (fillna to 0)
                    values = (
                        pd.to_numeric(node_df[col], errors="coerce")
                        .fillna(0)
                        .values.reshape(-1, 1)
                    )

                    # Verify if there is variance in the data. If there is no variance, we mantain original values
                    if np.std(values) > 0:
                        normalized = self.scalers[scaler_key].fit_transform(values)
                    else:
                        normalized = values

                    processed_features.append(normalized)
                    feature_names.append(f"{col}_normalized")

            except Exception as e:
                logger.warning(f"   ⚠️ Error processing the column {col}: {e}")
                # Create features by default in case of error
                default_values = np.zeros((len(node_df), 1))
                processed_features.append(default_values)
                feature_names.append(f"{col}_default")

        # Concat all the features
        if processed_features:
            final_features = np.concatenate(processed_features, axis=1)
        else:
            final_features = np.zeros((len(node_df), 1))
            feature_names = ["dummy_feature"]

        # Verificar dimensiones
        logger.info(f"     Final features: {final_features.shape}")

        return final_features, feature_names
