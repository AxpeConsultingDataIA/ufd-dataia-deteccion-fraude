### Info for both models ###
RANDOM_SEED = 42
TEST_SIZE = 0.2
VAL_SIZE = 0.2

# Fraud classes
FRAUD_CLASSES = {"NORMAL": 0, "FRAUDE": 1, "IRREGULARIDAD": 2}

### Simple model ###
# GNN model constants:
HIDDEN_DIM = 64
NUM_LAYERS = 3
DROPOUT = 0.3
NUM_HEADS = 4  # For GAT

# Entrenamiento
LEARNING_RATE = 0.001
WEIGHT_DECAY = 5e-4
EPOCHS = 200
BATCH_SIZE = 32
PATIENCE = 20

### Advanced model ###
HIDDEN_DIM_ADV = 128
NUM_LAYERS_ADV = 4
DROPOUT_ADV = 0.3
NUM_HEADS_ADV = 8

# Dim for every type of node
NODE_DIMS_ADV = {
    "contador": 64,
    "suministro": 48,
    "comercializadora": 32,
    "ubicacion": 24,
    "concentrador": 40,
    "cliente": 36,
    "transformador": 28,
    "zona": 20,
}

# Training
LEARNING_RATE_ADV = 0.0005
WEIGHT_DECAY_ADV = 1e-4
EPOCHS_ADV = 300
BATCH_SIZE_ADV = 64
PATIENCE_ADV = 30

# Data Simulation
NUM_CONTADORES_ADV = 5000
NUM_SUMINISTROS_ADV = 5000
NUM_COMERCIALIZADORAS_ADV = 20
NUM_UBICACIONES_ADV = 1000
NUM_CONCENTRADORES_ADV = 100
NUM_CLIENTES_ADV = 5000
NUM_TRANSFORMADORES_ADV = 200
NUM_ZONAS_ADV = 50

# Probabilidades de fraude
FRAUD_PROBABILITY_ADV = 0.08  # 8% de fraudes
IRREGULARITY_PROBABILITY_ADV = 0.04  # 4% de irregularidades
