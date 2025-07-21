# 1. Import necessary libraries
from config import *
import torch.nn as nn
from torch_geometric.nn import HeteroConv, GCNConv, GATConv, SAGEConv
import torch
import torch.nn.functional as F


# 2. Classes/Functions
# We create the FraudGNN class composed of two functions: __init__ and forward.
class FraudGNN(nn.Module):
    """
    Construction of the simple architecture for our GNN fraud detection model.
    """

    # We consider 3 labels: Normal, irregularidad y fraude:
    def __init__(
        self,
        input_dim: int,
        hidden_dim: int = HIDDEN_DIM,
        num_classes: int = 3,
        num_layers: int = NUM_LAYERS,
        dropout: float = DROPOUT,
        model_type: str = "GCN",
    ):
        """
        The constructor of the architecture, what layers will it have and how are they going to be connected.

        Parámetros:
            - input_dim: Number of features (per contador, in this case)
            - hidden_dim: Tamaño de las capas ocultas (ej: 64 neuronas)
            - num_classes: Número de clases a predecir (en este caso, 3)
            - num_layers: Número de capas del modelo.
            - dropout: Probabilidad de "apagar" neuronas, evitamos overfitting.
            - model_type: Tipo de GNN, o bien una GCN o bien una GAT.
        """
        super(FraudGNN, self).__init__()

        self.model_type = model_type
        self.num_layers = num_layers
        self.dropout = dropout

        # Creamos dos listas, por un lado una guardará las capas de convolución y la otra las capas de normalización.
        # Recuérdese que en una convolución en grafos, lo que hacemos es mirar nodos vecinos, permitiendo así que cada
        # nodo aprenda de sus vecinos.
        self.convs = nn.ModuleList()
        self.batch_norms = nn.ModuleList()

        # Primera capa
        # En GCN todos los nodos tienen la misma importancia y en GAT se da cierto peso a cada vecino.
        if model_type == "GCN":
            self.convs.append(GCNConv(input_dim, hidden_dim))
        elif model_type == "GAT":
            self.convs.append(
                GATConv(
                    input_dim, hidden_dim // NUM_HEADS, heads=NUM_HEADS, concat=True
                )
            )

        self.batch_norms.append(nn.BatchNorm1d(hidden_dim))

        # Capas intermediasm (en nuestro caso 1, eliminando la primera y última capa, ya que sólo tenemos 3 capas)
        for _ in range(num_layers - 2):
            if model_type == "GCN":
                self.convs.append(GCNConv(hidden_dim, hidden_dim))
            elif model_type == "GAT":
                self.convs.append(
                    GATConv(
                        hidden_dim,
                        hidden_dim // NUM_HEADS,
                        heads=NUM_HEADS,
                        concat=True,
                    )
                )
            self.batch_norms.append(nn.BatchNorm1d(hidden_dim))

        # Última capa de convolución
        if model_type == "GCN":
            self.convs.append(GCNConv(hidden_dim, hidden_dim))
        elif model_type == "GAT":
            self.convs.append(
                GATConv(
                    hidden_dim, hidden_dim // NUM_HEADS, heads=NUM_HEADS, concat=True
                )
            )

        # Clasificador final.
        # Lo que hacemos es tomar todas las características aprendidas por las capas de GNN y las convertimos en probabilidades
        # para cada clase
        self.classifier = nn.Sequential(
            nn.Linear(
                hidden_dim, hidden_dim // 2
            ),  ## Capa densa normal (redes neuronales clásicas)
            nn.ReLU(),  # función de activación
            nn.Dropout(dropout),
            nn.Linear(hidden_dim // 2, num_classes),  # última capa
        )

    def forward(self, x, edge_index, batch=None):
        """
        Función que toma los datos de entrada y produce predicciones.

        PArámetros:
            - x: Características de todos los nodos (matriz de features)
            - edge_index: Conexiones entre nodos (quién es vecino de quién)
            - batch: Para procesamiento por lotes (opcional)
        """
        # Aplicar capas de convolución
        for i, conv in enumerate(self.convs[:-1]):
            x = conv(
                x, edge_index
            )  # convolución de grafos para que cada nodo combine su información con la de sus vecinos
            x = self.batch_norms[i](
                x
            )  # normalización para estabilizar el entrenamiento
            x = F.relu(x)  # Función activación
            x = F.dropout(x, p=self.dropout, training=self.training)  # dropout

        # Última capa de convolución
        x = self.convs[-1](x, edge_index)

        # Clasificación para productir las características finales para cada nodo.
        # Cada contador en este caso, tiene 3 probs [P(Normal), P(Fraude), P(Irregularidad)]
        x = self.classifier(x)

        return F.log_softmax(
            x, dim=1
        )  # finalmente convertimos las probs en log-probs para facilitar el cálculo de la loss function.


class HeterogeneousFraudGNN(nn.Module):
    """Modelo GNN heterogéneo avanzado para detección de fraudes"""

    def __init__(
        self, metadata, hidden_dim=128, num_classes=3, num_layers=4, dropout=0.3
    ):
        super(HeterogeneousFraudGNN, self).__init__()

        self.metadata = metadata
        self.hidden_dim = hidden_dim
        self.num_classes = num_classes
        self.num_layers = num_layers
        self.dropout = dropout

        # Capas de embedding iniciales para cada tipo de nodo
        self.node_embeddings = nn.ModuleDict()
        for node_type in metadata[0]:  # metadata[0] contiene tipos de nodos
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
                    # Para otras relaciones, usar GCN estándar
                    conv_dict[edge_type] = SAGEConv(
                        (hidden_dim, hidden_dim), hidden_dim, aggr="mean"
                    )
                    # GCNConv(hidden_dim, hidden_dim, add_self_loops=False)

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

        # Agregación temporal para nodos contador
        self.temporal_aggregator = nn.LSTM(
            hidden_dim, hidden_dim // 2, batch_first=True, bidirectional=True
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
            h_dict[node_type] = F.relu(self.node_embeddings[node_type](x))

        # 2. Aplicar capas de convolución heterogéneas
        for i, conv in enumerate(self.convs):
            # Convolución heterogénea
            h_dict = conv(h_dict, edge_index_dict)

            # Normalización y activación
            for node_type in h_dict:
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
                h_dict["suministro"],
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
                h_dict["ubicacion"],
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
                h_dict["concentrador"],
                edge_index_dict[("contador", "comunica_via", "concentrador")],
            )
            contextual_features.append(concentrador_info)
        else:
            contextual_features.append(
                torch.zeros(batch_size, self.hidden_dim, device=contador_h.device)
            )

        # 4. Información de transformador
        if ("contador", "alimentado_por", "transformador") in edge_index_dict:
            transformador_info = self._aggregate_neighbor_info(
                contador_h,
                h_dict["transformador"],
                edge_index_dict[("contador", "alimentado_por", "transformador")],
            )
            contextual_features.append(transformador_info)
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
        stacked_context = torch.stack(
            contextual_features, dim=1
        )  # [batch, 5, hidden_dim]

        # Aplicar atención para ponderar diferentes tipos de información
        attention_weights = (
            F.softmax(self.info_attention, dim=0).unsqueeze(0).unsqueeze(2)
        )
        weighted_context = (stacked_context * attention_weights).sum(dim=1)

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
