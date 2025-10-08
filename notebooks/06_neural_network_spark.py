# 1. Import necessary libraries
import argparse
import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, sum as spark_sum, mean, stddev, count, desc, isnan, isnull
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, StringType
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
import torch.optim as optim
from sklearn.preprocessing import StandardScaler as SklearnStandardScaler
from sklearn.metrics import roc_auc_score, roc_curve, confusion_matrix, classification_report, f1_score, precision_score, recall_score
import pandas as pd
from utils import find_optimal_thres, get_probability_thresholds, plot_gain, lift_chart_plot

# 2. Define global variables
TARGET_COL = "target"
UFD_ORANGE = "#f26122"
UFD_BLUE = "#003865"
UFD_GRAY = "#cccccc"

# 3. Functions
def create_spark_session(app_name="UFD-Fraud-Detection-NeuralNetwork"):
    """
    Crea una sesión de Spark optimizada para el procesamiento
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

class FullyConnectedNN(nn.Module):
    """
    Red neuronal completamente conectada para clasificación binaria
    """
    def __init__(self, input_dim, hidden_sizes=[256, 128, 64, 32], dropout_rates=[0.5, 0.3, 0.3, 0.0]):
        super(FullyConnectedNN, self).__init__()
        
        layers = []
        prev_size = input_dim
        
        for i, (hidden_size, dropout_rate) in enumerate(zip(hidden_sizes, dropout_rates)):
            # Capa linear
            layers.append(nn.Linear(prev_size, hidden_size))
            
            # Batch normalization (excepto en la última capa)
            if i < len(hidden_sizes) - 1:
                layers.append(nn.BatchNorm1d(hidden_size))
            
            # Activación
            layers.append(nn.ReLU())
            
            # Dropout
            if dropout_rate > 0:
                layers.append(nn.Dropout(dropout_rate))
                
            prev_size = hidden_size
        
        # Capa de salida
        layers.append(nn.Linear(prev_size, 1))
        layers.append(nn.Sigmoid())
        
        self.network = nn.Sequential(*layers)

    def forward(self, x):
        return self.network(x)

def handle_missing_values_spark(df, target_col=TARGET_COL):
    """
    Maneja valores faltantes imputando con 0
    """
    # Obtener columnas numéricas (excluyendo identificadores y target)
    exclude_cols = ['cups_sgc', 'cnt_id', target_col]
    numeric_cols = [col for col in df.columns if col not in exclude_cols]
    
    # Imputar valores faltantes con 0
    for col_name in numeric_cols:
        df = df.withColumn(col_name, 
                          when(col(col_name).isNull() | isnan(col(col_name)), 0).otherwise(col(col_name)))
    
    # Imputar target con 0
    df = df.withColumn(target_col, 
                      when(col(target_col).isNull(), 0).otherwise(col(target_col)))
    
    return df

def identify_categorical_columns(df, exclude_cols=['cups_sgc', 'cnt_id', TARGET_COL]):
    """
    Identifica columnas categóricas en el DataFrame
    """
    categorical_cols = []
    
    for col_name in df.columns:
        if col_name not in exclude_cols:
            col_type = dict(df.dtypes)[col_name]
            if col_type in ['string', 'StringType']:
                categorical_cols.append(col_name)
    
    return categorical_cols

def apply_target_encoding_spark(df_train, df_eval, categorical_cols, target_col=TARGET_COL):
    """
    Aplica target encoding a las columnas categóricas usando PySpark
    """
    for col_name in categorical_cols:
        # Calcular la media del target por categoría
        target_means = df_train.groupBy(col_name).agg(mean(target_col).alias(f"{col_name}_target_mean"))
        
        # Aplicar encoding al conjunto de entrenamiento
        df_train = df_train.join(target_means, col_name, "left")
        df_train = df_train.drop(col_name).withColumnRenamed(f"{col_name}_target_mean", col_name)
        
        # Aplicar encoding al conjunto de evaluación
        df_eval = df_eval.join(target_means, col_name, "left")
        df_eval = df_eval.drop(col_name).withColumnRenamed(f"{col_name}_target_mean", col_name)
        
        # Rellenar valores nulos con la media global
        global_mean = df_train.agg(mean(col_name).alias("global_mean")).collect()[0]["global_mean"]
        df_train = df_train.withColumn(col_name, when(col(col_name).isNull(), global_mean).otherwise(col(col_name)))
        df_eval = df_eval.withColumn(col_name, when(col(col_name).isNull(), global_mean).otherwise(col(col_name)))
    
    return df_train, df_eval

def prepare_data_for_pytorch(df, feature_cols, target_col=TARGET_COL):
    """
    Prepara datos de Spark para PyTorch
    """
    # Convertir a pandas (necesario para PyTorch)
    df_pandas = df.select(feature_cols + [target_col]).toPandas()
    
    # Separar características y target
    X = df_pandas[feature_cols]
    y = df_pandas[target_col]
    
    return X, y

def scale_features(X_train, X_eval):
    """
    Escala las características usando StandardScaler
    """
    scaler = SklearnStandardScaler()
    
    X_train_scaled = pd.DataFrame(
        scaler.fit_transform(X_train), 
        columns=X_train.columns
    )
    X_eval_scaled = pd.DataFrame(
        scaler.transform(X_eval), 
        columns=X_eval.columns
    )
    
    return X_train_scaled, X_eval_scaled, scaler

def create_pytorch_datasets(X_train, y_train, X_eval, y_eval, batch_size_train=256, batch_size_eval=512):
    """
    Crea datasets y dataloaders de PyTorch
    """
    # Convertir a tensores
    X_train_tensor = torch.tensor(X_train.values, dtype=torch.float32)
    y_train_tensor = torch.tensor(y_train.values, dtype=torch.float32)
    X_eval_tensor = torch.tensor(X_eval.values, dtype=torch.float32)
    y_eval_tensor = torch.tensor(y_eval.values, dtype=torch.float32)
    
    # Crear datasets
    train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
    eval_dataset = TensorDataset(X_eval_tensor, y_eval_tensor)
    
    # Crear dataloaders
    train_loader = DataLoader(train_dataset, batch_size=batch_size_train, shuffle=True)
    eval_loader = DataLoader(eval_dataset, batch_size=batch_size_eval, shuffle=False)
    
    return train_loader, eval_loader

def train_neural_network(model, train_loader, eval_loader, epochs=30, lr=1e-3, weight_decay=1e-5, device='cpu'):
    """
    Entrena la red neuronal
    """
    model = model.to(device)
    criterion = nn.BCELoss()
    optimizer = optim.Adam(model.parameters(), lr=lr, weight_decay=weight_decay)
    
    training_history = []
    
    print("🧠 Iniciando entrenamiento de red neuronal...")
    
    for epoch in range(epochs):
        # Entrenamiento
        model.train()
        train_loss = 0
        
        for X_batch, y_batch in train_loader:
            X_batch, y_batch = X_batch.to(device), y_batch.to(device).view(-1, 1)
            
            optimizer.zero_grad()
            outputs = model(X_batch)
            loss = criterion(outputs, y_batch)
            loss.backward()
            optimizer.step()
            
            train_loss += loss.item()
        
        # Evaluación
        model.eval()
        y_train_true, y_train_pred = [], []
        y_eval_true, y_eval_pred = [], []
        
        with torch.no_grad():
            # Predicciones de entrenamiento
            for X_batch, y_batch in train_loader:
                X_batch, y_batch = X_batch.to(device), y_batch.to(device)
                preds = model(X_batch).cpu().numpy()
                y_train_true.extend(y_batch.cpu().numpy())
                y_train_pred.extend(preds)
            
            # Predicciones de evaluación
            for X_batch, y_batch in eval_loader:
                X_batch, y_batch = X_batch.to(device), y_batch.to(device)
                preds = model(X_batch).cpu().numpy()
                y_eval_true.extend(y_batch.cpu().numpy())
                y_eval_pred.extend(preds)
        
        # Convertir a arrays numpy
        y_train_true, y_train_pred = np.array(y_train_true), np.array(y_train_pred)
        y_eval_true, y_eval_pred = np.array(y_eval_true), np.array(y_eval_pred)
        
        # Encontrar umbral óptimo
        threshold_opt, _ = find_optimal_thres(y_train_true, y_train_pred, objetivo="f1", plot=False)
        
        # Aplicar umbral a evaluación
        y_eval_pred_bin = (y_eval_pred >= threshold_opt).astype(int)
        
        # Calcular métricas
        auc = roc_auc_score(y_eval_true, y_eval_pred)
        precision = precision_score(y_eval_true, y_eval_pred_bin)
        recall = recall_score(y_eval_true, y_eval_pred_bin)
        f1 = f1_score(y_eval_true, y_eval_pred_bin)
        
        # Guardar historial
        epoch_metrics = {
            'epoch': epoch + 1,
            'train_loss': train_loss / len(train_loader),
            'auc': auc,
            'precision': precision,
            'recall': recall,
            'f1': f1,
            'threshold': threshold_opt
        }
        training_history.append(epoch_metrics)
        
        # Imprimir progreso
        if (epoch + 1) % 5 == 0 or epoch == 0:
            print(f"Epoch [{epoch+1}/{epochs}] | Loss: {epoch_metrics['train_loss']:.4f} | "
                  f"AUC: {auc:.4f} | Precision: {precision:.4f} | Recall: {recall:.4f} | F1: {f1:.4f}")
    
    return model, training_history, y_train_pred, y_eval_true, y_eval_pred

def evaluate_model_performance(y_true, y_pred, y_pred_bin, dataset_name="test"):
    """
    Evalúa el rendimiento del modelo
    """
    # Métricas básicas
    auc_score = roc_auc_score(y_true, y_pred)
    f1 = f1_score(y_true, y_pred_bin)
    precision = precision_score(y_true, y_pred_bin)
    recall = recall_score(y_true, y_pred_bin)
    
    # Matriz de confusión
    cm = confusion_matrix(y_true, y_pred_bin)
    
    # Reporte de clasificación
    report = classification_report(y_true, y_pred_bin, output_dict=True)
    
    print(f"\n📊 Métricas finales para {dataset_name}:")
    print(f"   AUC: {auc_score:.4f}")
    print(f"   F1-Score: {f1:.4f}")
    print(f"   Precisión: {precision:.4f}")
    print(f"   Recall: {recall:.4f}")
    
    return {
        'auc': auc_score,
        'f1': f1,
        'precision': precision,
        'recall': recall,
        'confusion_matrix': cm.tolist()
    }

def add_predictions_to_spark_df(spark, df, predictions, feature_cols):
    """
    Añade predicciones del modelo neural al DataFrame de Spark
    """
    # Crear DataFrame temporal con predicciones
    pred_data = [(float(pred),) for pred in predictions]
    pred_df = spark.createDataFrame(pred_data, ["pred_proba"])
    
    # Añadir índice para hacer join
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window = Window.orderBy(lit(1))
    df_with_index = df.withColumn("row_id", row_number().over(window))
    pred_df_with_index = pred_df.withColumn("row_id", row_number().over(window))
    
    # Join y eliminar índice
    result_df = df_with_index.join(pred_df_with_index, "row_id").drop("row_id")
    
    return result_df

def compute_final_scoring_spark(df, unsupervised_col="fraude_score_no_supervisado", 
                               supervised_col="pred_proba", weight_unsupervised=0.3, weight_supervised=0.7):
    """
    Calcula el scoring final combinando modelos supervisados y no supervisados
    """
    return df.withColumn(
        "final_scoring",
        col(unsupervised_col) * weight_unsupervised + col(supervised_col) * weight_supervised
    )

def calculate_thresholds_and_categories_spark(df, score_col, target_col, cuts=[25, 35]):
    """
    Calcula umbrales y categorías de fraude usando PySpark
    """
    # Convertir a pandas para cálculo de percentiles (más eficiente)
    scores_pandas = df.select(score_col, target_col).toPandas()
    
    # Calcular umbrales usando percentiles
    thresholds = []
    for cut in cuts:
        threshold = np.percentile(scores_pandas[score_col], 100 - cut)
        thresholds.append(threshold)
    
    # Aplicar categorización
    df = df.withColumn(
        "categoria_fraude",
        when(col(score_col) < thresholds[0], "Bajo")
        .when((col(score_col) >= thresholds[0]) & (col(score_col) < thresholds[1]), "Medio")
        .otherwise("Alto")
    )
    
    return df, thresholds

def save_model_and_scaler(model, scaler, output_path):
    """
    Guarda el modelo entrenado y el scaler
    """
    os.makedirs(output_path, exist_ok=True)
    
    # Guardar modelo PyTorch
    torch.save(model.state_dict(), f"{output_path}/neural_network_model.pth")
    
    # Guardar scaler (usando pickle)
    import pickle
    with open(f"{output_path}/feature_scaler.pkl", 'wb') as f:
        pickle.dump(scaler, f)
    
    print(f"✅ Modelo y scaler guardados en: {output_path}")

# 4. Code
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entrenamiento de red neuronal supervisada para detección de fraude usando PySpark")
    
    # Argumentos para archivos de entrada
    parser.add_argument("--train-x-file", type=str, default="data_training_may.csv", 
                       help="Archivo de características de entrenamiento")
    parser.add_argument("--train-y-file", type=str, default="data_training_may_y.csv", 
                       help="Archivo de etiquetas de entrenamiento")
    parser.add_argument("--val-x-file", type=str, default="data_validation_june.csv", 
                       help="Archivo de características de validación")
    parser.add_argument("--val-y-file", type=str, default="data_validation_june_y.csv", 
                       help="Archivo de etiquetas de validación")
    parser.add_argument("--train-final-df", type=str, default="final_df_non_supervised_may.csv", 
                       help="DataFrame final de entrenamiento")
    parser.add_argument("--val-final-df", type=str, default="final_df_eval_non_supervised_june.csv", 
                       help="DataFrame final de validación")
    
    # Argumentos para archivos de salida
    parser.add_argument("--output-train-final", type=str, default="final_df_supervised_nn_train.csv", 
                       help="DataFrame final de entrenamiento con predicciones")
    parser.add_argument("--output-val-final", type=str, default="final_df_supervised_nn_val.csv", 
                       help="DataFrame final de validación con predicciones")
    parser.add_argument("--model-output-path", type=str, default="models/neural_network", 
                       help="Directorio para guardar el modelo entrenado")
    
    # Parámetros del modelo
    parser.add_argument("--hidden-sizes", nargs='+', type=int, default=[256, 128, 64, 32], 
                       help="Tamaños de capas ocultas")
    parser.add_argument("--dropout-rates", nargs='+', type=float, default=[0.5, 0.3, 0.3, 0.0], 
                       help="Tasas de dropout para cada capa")
    parser.add_argument("--epochs", type=int, default=30, help="Número de épocas de entrenamiento")
    parser.add_argument("--lr", type=float, default=1e-3, help="Tasa de aprendizaje")
    parser.add_argument("--weight-decay", type=float, default=1e-5, help="Regularización L2")
    parser.add_argument("--batch-size-train", type=int, default=256, help="Tamaño de lote para entrenamiento")
    parser.add_argument("--batch-size-eval", type=int, default=512, help="Tamaño de lote para evaluación")
    
    # Parámetros de scoring
    parser.add_argument("--cuts", nargs='+', type=float, default=[25, 35], help="Cortes para categorización")
    parser.add_argument("--weight-unsupervised", type=float, default=0.3, help="Peso para score no supervisado")
    parser.add_argument("--weight-supervised", type=float, default=0.7, help="Peso para score supervisado")
    
    args = parser.parse_args()
    
    # Detectar dispositivo
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"🔧 Usando dispositivo: {device}")
    
    # Crear sesión de Spark
    spark = create_spark_session()
    
    # Paths de SageMaker
    input_path = "/opt/ml/processing/input"
    output_path = "/opt/ml/processing/output"
    
    print("🚀 Iniciando entrenamiento de red neuronal supervisada con PySpark...")
    
    # =====================================
    # CARGA DE DATOS
    # =====================================
    
    print("📂 Cargando datos...")
    X = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv(f"{input_path}/{args.train_x_file}")
    y = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv(f"{input_path}/{args.train_y_file}").select(TARGET_COL)
    X_eval = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv(f"{input_path}/{args.val_x_file}")
    y_eval = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv(f"{input_path}/{args.val_y_file}").select(TARGET_COL)
    
    final_df = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv(f"{input_path}/{args.train_final_df}")
    final_df_eval = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv(f"{input_path}/{args.val_final_df}")
    
    print(f"✅ Datos cargados: Train={X.count()} filas, Val={X_eval.count()} filas")
    
    # Cache para mejor rendimiento
    X.cache()
    X_eval.cache()
    final_df.cache()
    final_df_eval.cache()
    
    # =====================================
    # PREPROCESAMIENTO
    # =====================================
    
    print("🔧 Preprocesando datos...")
    
    # Identificar columnas categóricas
    cat_cols = identify_categorical_columns(X)
    print(f"📋 Columnas categóricas identificadas: {len(cat_cols)}")
    
    # Unir X e y para procesamiento
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window = Window.orderBy(lit(1))
    X_with_index = X.withColumn("row_id", row_number().over(window))
    y_with_index = y.withColumn("row_id", row_number().over(window))
    X_eval_with_index = X_eval.withColumn("row_id", row_number().over(window))
    y_eval_with_index = y_eval.withColumn("row_id", row_number().over(window))
    
    train_df = X_with_index.join(y_with_index, "row_id").drop("row_id")
    eval_df = X_eval_with_index.join(y_eval_with_index, "row_id").drop("row_id")
    
    # Manejo de valores faltantes
    print("🔄 Manejando valores faltantes...")
    train_df = handle_missing_values_spark(train_df)
    eval_df = handle_missing_values_spark(eval_df)
    
    # Target encoding para variables categóricas
    if cat_cols:
        print("🏷️ Aplicando target encoding...")
        train_df, eval_df = apply_target_encoding_spark(train_df, eval_df, cat_cols)
    
    # =====================================
    # PREPARACIÓN PARA PYTORCH
    # =====================================
    
    print("⚡ Preparando datos para PyTorch...")
    
    # Obtener columnas de características
    feature_cols = [col for col in train_df.columns if col not in ['cups_sgc', 'cnt_id', TARGET_COL]]
    
    # Convertir a pandas para PyTorch
    X_train_pandas, y_train_pandas = prepare_data_for_pytorch(train_df, feature_cols)
    X_eval_pandas, y_eval_pandas = prepare_data_for_pytorch(eval_df, feature_cols)
    
    # Escalar características
    print("📏 Escalando características...")
    X_train_scaled, X_eval_scaled, scaler = scale_features(X_train_pandas, X_eval_pandas)
    
    # Crear datasets de PyTorch
    train_loader, eval_loader = create_pytorch_datasets(
        X_train_scaled, y_train_pandas, X_eval_scaled, y_eval_pandas,
        batch_size_train=args.batch_size_train, batch_size_eval=args.batch_size_eval
    )
    
    # =====================================
    # ENTRENAMIENTO DEL MODELO
    # =====================================
    
    print("🧠 Creando y entrenando red neuronal...")
    
    # Crear modelo
    input_dim = X_train_scaled.shape[1]
    model = FullyConnectedNN(
        input_dim=input_dim,
        hidden_sizes=args.hidden_sizes,
        dropout_rates=args.dropout_rates
    )
    
    print(f"🏗️ Arquitectura de red: {input_dim} -> {' -> '.join(map(str, args.hidden_sizes))} -> 1")
    
    # Entrenar modelo
    model, training_history, y_train_pred, y_eval_true, y_eval_pred = train_neural_network(
        model, train_loader, eval_loader,
        epochs=args.epochs, lr=args.lr, weight_decay=args.weight_decay, device=device
    )
    
    # =====================================
    # EVALUACIÓN DEL MODELO
    # =====================================
    
    print("📊 Evaluando modelo final...")
    
    # Encontrar umbral óptimo final
    threshold_final, _ = find_optimal_thres(y_train_pred, y_train_pred, objetivo="f1", plot=False)
    y_eval_pred_bin = (y_eval_pred >= threshold_final).astype(int)
    
    # Evaluar rendimiento
    final_metrics = evaluate_model_performance(y_eval_true, y_eval_pred, y_eval_pred_bin, "validación")
    
    # =====================================
    # GUARDAR MODELO
    # =====================================
    
    print("💾 Guardando modelo entrenado...")
    model_output_full_path = f"{output_path}/{args.model_output_path}"
    save_model_and_scaler(model, scaler, model_output_full_path)
    
    # =====================================
    # AÑADIR PREDICCIONES A DATAFRAMES FINALES
    # =====================================
    
    print("🔗 Añadiendo predicciones a DataFrames finales...")
    
    # Generar predicciones para todos los datos
    model.eval()
    with torch.no_grad():
        X_train_tensor = torch.tensor(X_train_scaled.values, dtype=torch.float32).to(device)
        X_eval_tensor = torch.tensor(X_eval_scaled.values, dtype=torch.float32).to(device)
        
        y_train_final_pred = model(X_train_tensor).cpu().numpy().flatten()
        y_eval_final_pred = model(X_eval_tensor).cpu().numpy().flatten()
    
    # Añadir predicciones a DataFrames finales
    final_df = add_predictions_to_spark_df(spark, final_df, y_train_final_pred, feature_cols)
    final_df_eval = add_predictions_to_spark_df(spark, final_df_eval, y_eval_final_pred, feature_cols)
    
    # Calcular scoring final
    final_df = compute_final_scoring_spark(
        final_df, weight_unsupervised=args.weight_unsupervised, 
        weight_supervised=args.weight_supervised
    )
    final_df_eval = compute_final_scoring_spark(
        final_df_eval, weight_unsupervised=args.weight_unsupervised,
        weight_supervised=args.weight_supervised
    )
    
    # Calcular categorías de fraude
    final_df, thresholds_train = calculate_thresholds_and_categories_spark(
        final_df, "final_scoring", TARGET_COL, cuts=args.cuts
    )
    final_df_eval, thresholds_eval = calculate_thresholds_and_categories_spark(
        final_df_eval, "final_scoring", TARGET_COL, cuts=args.cuts
    )
    
    print(f"📏 Umbrales calculados: {thresholds_train}")
    
    # =====================================
    # GUARDAR RESULTADOS
    # =====================================
    
    print("💾 Guardando resultados finales...")
    
    # Extraer nombres de los meses
    train_month = args.train_x_file.replace("data_training_", "").replace(".csv", "")
    val_month = args.val_x_file.replace("data_validation_", "").replace(".csv", "")
    
    # Crear directorios de salida
    train_output_dir = f"{output_path}/{train_month}"
    val_output_dir = f"{output_path}/{val_month}"
    
    # Guardar DataFrames finales
    final_df.coalesce(1).write.mode("overwrite").option("header", "true").option("sep", ";").csv(f"{train_output_dir}/{args.output_train_final}")
    final_df_eval.coalesce(1).write.mode("overwrite").option("header", "true").option("sep", ";").csv(f"{val_output_dir}/{args.output_val_final}")
    
    # =====================================
    # ESTADÍSTICAS FINALES
    # =====================================
    
    print("📈 Estadísticas finales:")
    
    # Distribución de categorías de fraude
    print("\n🎯 Distribución de categorías de fraude:")
    train_categories = final_df.groupBy("categoria_fraude").count().collect()
    val_categories = final_df_eval.groupBy("categoria_fraude").count().collect()
    
    for row in train_categories:
        print(f"   Train - {row['categoria_fraude']}: {row['count']}")
    
    for row in val_categories:
        print(f"   Val - {row['categoria_fraude']}: {row['count']}")
    
    # Estadísticas de scoring
    final_scoring_stats_train = final_df.agg(
        mean("final_scoring").alias("mean_score"),
        stddev("final_scoring").alias("std_score"),
        count("*").alias("count")
    ).collect()[0]
    
    final_scoring_stats_val = final_df_eval.agg(
        mean("final_scoring").alias("mean_score"),
        stddev("final_scoring").alias("std_score"),
        count("*").alias("count")
    ).collect()[0]
    
    print(f"\n📊 Estadísticas de scoring final:")
    print(f"   Train: μ={final_scoring_stats_train['mean_score']:.4f}, σ={final_scoring_stats_train['std_score']:.4f}")
    print(f"   Val: μ={final_scoring_stats_val['mean_score']:.4f}, σ={final_scoring_stats_val['std_score']:.4f}")
    
    # Resumen del entrenamiento
    print(f"\n🏆 Resumen del entrenamiento:")
    print(f"   🎯 AUC final: {final_metrics['auc']:.4f}")
    print(f"   🎯 F1 final: {final_metrics['f1']:.4f}")
    print(f"   📏 Umbral óptimo: {threshold_final:.3f}")
    print(f"   🧠 Épocas entrenadas: {args.epochs}")
    print(f"   🏗️ Arquitectura: {args.hidden_sizes}")
    print(f"   ⚡ Dispositivo usado: {device}")
    
    print(f"\n✅ Entrenamiento completado exitosamente!")
    print(f"   📁 Archivos guardados en: {output_path}")
    print(f"   🤖 Modelo guardado en: {model_output_full_path}")
    print(f"   🔧 Framework: PyTorch + PySpark")
    
    # Cerrar sesión de Spark
    spark.stop()