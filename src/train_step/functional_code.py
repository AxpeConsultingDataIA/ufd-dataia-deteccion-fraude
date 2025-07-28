"""
Functional code that is called from the principal script
"""

# 1. Import necessary libraries
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import HeteroConv, GCNConv, SAGEConv, GATConv
from torch_geometric.data import HeteroData
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score, f1_score
from sklearn.preprocessing import StandardScaler
from datetime import datetime
import logging
import os

# 2. Define global variables
logger = logging.getLogger(__name__)

FRAUD_CLASSES = {"NORMAL": 0, "FRAUDE": 1, "IRREGULARIDAD": 2}
NODE_DIMS_ADV = {
    "contador": 128,
    "suministro": 64,
    "comercializadora": 32,
    "ubicacion": 16,
    "concentrador": 32,
    "expediente_fraude": 48,
}


# 3. Functions/Classes
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


class HeterogeneousFraudGNNSageMaker(nn.Module):
    """
    Modelo GNN heterogéneo avanzado para detección de fraudes
    Versión adaptada para SageMaker
    """

    def __init__(
        self, metadata, hidden_dim=128, num_classes=3, num_layers=4, dropout=0.3
    ):
        super(HeterogeneousFraudGNNSageMaker, self).__init__()

        self.metadata = metadata
        self.hidden_dim = hidden_dim
        self.num_classes = num_classes
        self.num_layers = num_layers
        self.dropout = dropout

        # Capas de embedding iniciales para cada tipo de nodo
        self.node_embeddings = nn.ModuleDict()
        for node_type in metadata[0]:
            input_dim = NODE_DIMS_ADV.get(node_type, 64)
            self.node_embeddings[node_type] = nn.Linear(input_dim, hidden_dim)

        # Capas de convolución heterogéneas
        self.convs = nn.ModuleList()
        for _ in range(num_layers):
            conv_dict = {}
            for edge_type in metadata[1]:  # metadata[1] contiene tipos de edges
                src_type, relation, dst_type = edge_type

                # Usar diferentes tipos de convolución según la relación
                if "cerca_de" in relation or "similar_a" in relation:
                    # Para relaciones de similaridad, usar GAT (con atención)
                    conv_dict[edge_type] = GATConv(
                        hidden_dim,
                        hidden_dim // 4,
                        heads=4,
                        concat=True,
                        dropout=dropout,
                        add_self_loops=False,
                    )
                elif "mide" in relation or "pertenece" in relation:
                    # Para relaciones jerárquicas, usar SAGE
                    conv_dict[edge_type] = SAGEConv(hidden_dim, hidden_dim, aggr="mean")
                else:
                    # Para otras relaciones, usar SAGE estándar
                    conv_dict[edge_type] = SAGEConv(hidden_dim, hidden_dim, aggr="mean")

            hetero_conv = HeteroConv(conv_dict, aggr="mean")
            self.convs.append(hetero_conv)

        # Capas de normalización
        self.norms = nn.ModuleList()
        for _ in range(num_layers):
            norm_dict = {}
            for node_type in metadata[0]:
                norm_dict[node_type] = nn.BatchNorm1d(hidden_dim)
            self.norms.append(nn.ModuleDict(norm_dict))

        # Mecanismo de atención inter-tipo
        self.cross_attention = nn.MultiheadAttention(
            hidden_dim, num_heads=8, batch_first=True
        )

        # Clasificador final (solo para nodos contador)
        self.classifier = nn.Sequential(
            nn.Linear(
                hidden_dim * 2, hidden_dim
            ),  # *2 por concatenación de características
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, hidden_dim // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, num_classes),
        )

        # Pesos de atención para diferentes tipos de información
        self.info_attention = nn.Parameter(torch.randn(5))  # 5 tipos de información

    def forward(self, x_dict, edge_index_dict, batch_dict=None):

        # 1. Embedding inicial para cada tipo de nodo
        h_dict = {}
        for node_type, x in x_dict.items():
            if node_type in self.node_embeddings:
                h_dict[node_type] = F.relu(self.node_embeddings[node_type](x))

        # 2. Aplicar capas de convolución heterogéneas
        for i, conv in enumerate(self.convs):
            # Convolución heterogénea
            h_dict = conv(h_dict, edge_index_dict)

            # Normalización y activación
            for node_type in h_dict:
                if node_type in self.norms[i]:
                    h_dict[node_type] = self.norms[i][node_type](h_dict[node_type])
                    h_dict[node_type] = F.relu(h_dict[node_type])
                    h_dict[node_type] = F.dropout(
                        h_dict[node_type], p=self.dropout, training=self.training
                    )

        # 3. Obtener representaciones de contadores
        contador_h = h_dict["contador"]

        # 4. Agregar información contextual de otros tipos de nodos
        contextual_info = self._aggregate_contextual_info(h_dict, edge_index_dict)

        # 5. Combinar información del contador con contexto
        combined_h = torch.cat([contador_h, contextual_info], dim=1)

        # 6. Clasificación final
        logits = self.classifier(combined_h)

        return F.log_softmax(logits, dim=1)

    def _aggregate_contextual_info(self, h_dict, edge_index_dict):
        """Agrega información contextual de diferentes tipos de nodos"""

        contador_h = h_dict["contador"]
        batch_size = contador_h.size(0)

        # Información de diferentes fuentes
        contextual_features = []

        # 1. Información de suministro
        if ("contador", "mide", "suministro") in edge_index_dict:
            suministro_info = self._aggregate_neighbor_info(
                contador_h,
                h_dict.get("suministro", torch.zeros_like(contador_h)),
                edge_index_dict[("contador", "mide", "suministro")],
            )
            contextual_features.append(suministro_info)
        else:
            contextual_features.append(
                torch.zeros(batch_size, self.hidden_dim, device=contador_h.device)
            )

        # 2. Información de ubicación
        if ("contador", "ubicado_en", "ubicacion") in edge_index_dict:
            ubicacion_info = self._aggregate_neighbor_info(
                contador_h,
                h_dict.get("ubicacion", torch.zeros_like(contador_h)),
                edge_index_dict[("contador", "ubicado_en", "ubicacion")],
            )
            contextual_features.append(ubicacion_info)
        else:
            contextual_features.append(
                torch.zeros(batch_size, self.hidden_dim, device=contador_h.device)
            )

        # 3. Información de concentrador
        if ("contador", "comunica_via", "concentrador") in edge_index_dict:
            concentrador_info = self._aggregate_neighbor_info(
                contador_h,
                h_dict.get("concentrador", torch.zeros_like(contador_h)),
                edge_index_dict[("contador", "comunica_via", "concentrador")],
            )
            contextual_features.append(concentrador_info)
        else:
            contextual_features.append(
                torch.zeros(batch_size, self.hidden_dim, device=contador_h.device)
            )

        # 4. Información de comercializadora (vía suministro)
        if ("suministro", "contratado_con", "comercializadora") in edge_index_dict:
            # Información indirecta de comercializadora
            contextual_features.append(
                torch.zeros(batch_size, self.hidden_dim, device=contador_h.device)
            )
        else:
            contextual_features.append(
                torch.zeros(batch_size, self.hidden_dim, device=contador_h.device)
            )

        # 5. Información de contadores similares
        if ("contador", "cerca_de", "contador") in edge_index_dict:
            similar_info = self._aggregate_neighbor_info(
                contador_h,
                contador_h,
                edge_index_dict[("contador", "cerca_de", "contador")],
            )
            contextual_features.append(similar_info)
        else:
            contextual_features.append(
                torch.zeros(batch_size, self.hidden_dim, device=contador_h.device)
            )

        # Apilar características contextuales
        if contextual_features:
            stacked_context = torch.stack(
                contextual_features, dim=1
            )  # [batch, num_contexts, hidden_dim]

            # Aplicar atención para ponderar diferentes tipos de información
            num_contexts = len(contextual_features)
            attention_weights = (
                F.softmax(self.info_attention[:num_contexts], dim=0)
                .unsqueeze(0)
                .unsqueeze(2)
            )
            weighted_context = (stacked_context * attention_weights).sum(dim=1)
        else:
            weighted_context = torch.zeros_like(contador_h)

        return weighted_context

    def _aggregate_neighbor_info(self, source_h, target_h, edge_index):
        """Agrega información de nodos vecinos usando edge_index"""

        if edge_index.size(1) == 0:
            return torch.zeros_like(source_h)

        # Obtener información de nodos destino
        target_info = target_h[edge_index[1]]

        # Agregar por nodo fuente
        source_indices = edge_index[0]
        aggregated = torch.zeros_like(source_h)

        # Usar scatter para agregar
        for i in range(source_h.size(0)):
            mask = source_indices == i
            if mask.any():
                aggregated[i] = target_info[mask].mean(dim=0)

        return aggregated


class HeterogeneousFraudTrainerSageMaker:
    """
    Trainer class especializada en modelos heterogéneos para SageMaker
    """

    def __init__(
        self, model, device: str = "cpu", learning_rate=0.001, weight_decay=0.01
    ):
        self.model = model.to(device)
        self.device = device
        self.optimizer = torch.optim.AdamW(
            model.parameters(), lr=learning_rate, weight_decay=weight_decay
        )

        # Loss function con pesos para clases desbalanceadas
        class_weights = torch.tensor(
            [1.0, 5.0, 3.0], device=device
        )  # Normal, Fraude, Irregularidad
        self.criterion = nn.NLLLoss(weight=class_weights)

        # Scheduler para la tasa de aprendizaje
        self.scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            self.optimizer, mode="max", factor=0.5, patience=10
        )

        # Métricas que se almacenarán
        self.train_losses = []
        self.val_losses = []
        self.train_accuracies = []
        self.val_accuracies = []

    def train_epoch(self, data, train_mask):
        """
        Entrena una época
        """
        self.model.train()
        self.optimizer.zero_grad()

        # Forward pass
        out = self.model(data.x_dict, data.edge_index_dict)

        # Calcular pérdida solo para nodos contador con etiquetas
        contador_out = out
        contador_labels = data["contador"].y

        loss = self.criterion(contador_out[train_mask], contador_labels[train_mask])

        # Backward pass
        loss.backward()

        # Gradient clipping para estabilidad
        torch.nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=1.0)

        self.optimizer.step()

        # Calcular precisión
        pred = contador_out[train_mask].argmax(dim=1)
        train_acc = (pred == contador_labels[train_mask]).float().mean()

        return loss.item(), train_acc.item()

    def evaluate(self, data, mask):
        """
        Evalúa el modelo
        """
        self.model.eval()
        with torch.no_grad():
            out = self.model(data.x_dict, data.edge_index_dict)
            contador_labels = data["contador"].y

            loss = self.criterion(out[mask], contador_labels[mask])
            pred = out[mask].argmax(dim=1)
            acc = (pred == contador_labels[mask]).float().mean()

            # Métricas adicionales
            y_true = contador_labels[mask].cpu().numpy()
            y_pred = pred.cpu().numpy()
            y_prob = torch.softmax(out[mask], dim=1).cpu().numpy()

            f1 = f1_score(y_true, y_pred, average="weighted")

            # ROC AUC para clasificación multiclase
            try:
                roc_auc = roc_auc_score(y_true, y_prob, multi_class="ovr")
            except:
                roc_auc = 0.0

        return loss.item(), acc.item(), f1, roc_auc

    def train(self, data, train_mask, val_mask, epochs=100, patience=15):
        """
        Entrena el modelo completo
        """

        logger.info(f"🎓 Iniciando entrenamiento por {epochs} épocas...")

        best_val_acc = 0
        patience_counter = 0

        for epoch in range(epochs):
            # Entrenar
            train_loss, train_acc = self.train_epoch(data, train_mask)

            # Validar
            val_loss, val_acc, val_f1, val_roc_auc = self.evaluate(data, val_mask)

            # Actualizar scheduler
            self.scheduler.step(val_acc)

            # Guardar métricas
            self.train_losses.append(train_loss)
            self.val_losses.append(val_loss)
            self.train_accuracies.append(train_acc)
            self.val_accuracies.append(val_acc)

            # Early stopping
            if val_acc > best_val_acc:
                best_val_acc = val_acc
                patience_counter = 0

                # Guardar mejor modelo
                model_dir = os.environ.get("SM_MODEL_DIR", "/opt/ml/model")
                os.makedirs(model_dir, exist_ok=True)
                torch.save(
                    self.model.state_dict(), os.path.join(model_dir, "best_model.pth")
                )
            else:
                patience_counter += 1
                if patience_counter >= patience:
                    logger.info(f"Early stopping en época {epoch+1}")
                    break

            # Log progreso (para métricas de SageMaker)
            if (epoch + 1) % 10 == 0:
                print(f"Epoch {epoch+1:03d}")
                print(f"Train Loss: {train_loss:.4f}")
                print(f"Validation Accuracy: {val_acc:.4f}")
                print(f"Validation F1: {val_f1:.4f}")

                logger.info(
                    f"Época {epoch+1:03d}, Train Loss: {train_loss:.4f}, "
                    f"Train Acc: {train_acc:.4f}, Val Loss: {val_loss:.4f}, "
                    f"Val Acc: {val_acc:.4f}, Val F1: {val_f1:.4f}"
                )

        return {
            "best_val_acc": best_val_acc,
            "final_train_loss": self.train_losses[-1] if self.train_losses else 0,
            "final_val_acc": self.val_accuracies[-1] if self.val_accuracies else 0,
            "final_val_f1": val_f1,
            "final_val_roc_auc": val_roc_auc,
            "train_losses": self.train_losses,
            "val_losses": self.val_losses,
            "train_accuracies": self.train_accuracies,
            "val_accuracies": self.val_accuracies,
            "total_epochs": len(self.train_losses),
        }


class ModelAnalyzer:
    """
    Analizador para obtener insights del modelo entrenado
    """

    def __init__(self, model, device="cpu"):
        self.model = model.to(device)
        self.device = device

    def get_attention_weights(self):
        """Obtener pesos de atención del modelo"""

        if hasattr(self.model, "info_attention"):
            attention_weights = F.softmax(self.model.info_attention, dim=0)

            attention_info = {
                "suministro": attention_weights[0].item(),
                "ubicacion": attention_weights[1].item(),
                "concentrador": attention_weights[2].item(),
                "comercializadora": attention_weights[3].item(),
                "contadores_similares": attention_weights[4].item(),
            }

            return attention_info

        return {}

    def analyze_node_embeddings(self, data, node_type="contador", top_k=10):
        """Analizar embeddings de nodos"""

        self.model.eval()
        with torch.no_grad():
            # Forward pass hasta obtener embeddings
            h_dict = {}
            for nt, x in data.x_dict.items():
                if nt in self.model.node_embeddings:
                    h_dict[nt] = F.relu(self.model.node_embeddings[nt](x))

            if node_type in h_dict:
                embeddings = h_dict[node_type]

                # Calcular importancia basada en norma
                importance_scores = torch.norm(embeddings, dim=1)
                top_indices = torch.topk(
                    importance_scores, min(top_k, len(importance_scores))
                ).indices

                return {
                    "top_nodes": top_indices.cpu().tolist(),
                    "importance_scores": importance_scores[top_indices].cpu().tolist(),
                    "embedding_dim": embeddings.size(1),
                }

        return {}
