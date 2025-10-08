# 1. Import necessary libraries
import argparse
import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, sum as spark_sum, mean, stddev, count, desc
from pyspark.sql.types import DoubleType, IntegerType, BooleanType
from pyspark.ml.feature import StandardScaler, MinMaxScaler, VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from scipy.special import expit
from utils import Autoencoder

# 2. Define global variables
TARGET_COL = "target"
UFD_ORANGE = "#f26122"
UFD_BLUE = "#003865"
UFD_GRAY = "#cccccc"

# 3. Functions
def create_spark_session(app_name="UFD-Fraud-Detection-Unsupervised"):
    """
    Crea una sesión de Spark optimizada para el procesamiento
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def prepare_feature_vector(df, feature_cols, output_col="features"):
    """
    Prepara el vector de características para los algoritmos de ML
    """
    assembler = VectorAssembler(inputCols=feature_cols, outputCol=output_col)
    return assembler.transform(df)

def apply_standard_scaling(df, input_col="features", output_col="scaled_features"):
    """
    Aplica StandardScaler a las características
    """
    scaler = StandardScaler(inputCol=input_col, outputCol=output_col, withStd=True, withMean=True)
    scaler_model = scaler.fit(df)
    return scaler_model.transform(df), scaler_model

def apply_dbscan_spark(df, feature_cols, eps=3.0, min_samples=100):
    """
    Implementa una aproximación de DBSCAN usando KMeans + densidad
    Nota: PySpark no tiene DBSCAN nativo, usamos una aproximación
    """
    # Preparar datos
    df_features = prepare_feature_vector(df, feature_cols)
    df_scaled, _ = apply_standard_scaling(df_features)
    
    # Usar KMeans como aproximación inicial
    kmeans = KMeans(k=10, featuresCol="scaled_features", predictionCol="cluster")
    model = kmeans.fit(df_scaled)
    df_clustered = model.transform(df_scaled)
    
    # Calcular densidad por cluster
    cluster_stats = df_clustered.groupBy("cluster").agg(
        count("*").alias("cluster_size"),
        mean("scaled_features").alias("centroid")
    )
    
    # Marcar clusters pequeños como anomalías
    df_with_stats = df_clustered.join(cluster_stats, "cluster")
    df_anomalies = df_with_stats.withColumn(
        "anomaly_dbscan",
        when(col("cluster_size") < min_samples, 1).otherwise(0)
    )
    
    return df_anomalies.select(df.columns + ["anomaly_dbscan"])

def isolation_forest_udf(contamination=0.01):
    """
    UDF para aplicar Isolation Forest usando sklearn
    """
    from sklearn.ensemble import IsolationForest
    from scipy.special import expit
    
    def predict_anomalies(features_array):
        # Convertir a numpy array
        features = np.array([list(row) for row in features_array])
        
        # Aplicar Isolation Forest
        iso = IsolationForest(contamination=contamination, random_state=42)
        preds = iso.fit_predict(features)
        scores = iso.decision_function(features)
        
        # Convertir a probabilidades
        probabilities = expit(scores * 5)
        
        # Convertir anomalías (-1 -> 1, 1 -> 0)
        anomalies = [(1 if pred == -1 else 0) for pred in preds]
        
        return list(zip(anomalies, scores.tolist(), probabilities.tolist()))
    
    return udf(predict_anomalies, ArrayType(ArrayType(DoubleType())))

def lof_udf(n_neighbors=20, contamination=0.05):
    """
    UDF para aplicar Local Outlier Factor
    """
    from sklearn.neighbors import LocalOutlierFactor
    from sklearn.preprocessing import MinMaxScaler
    
    def predict_lof(features_array):
        # Convertir a numpy array
        features = np.array([list(row) for row in features_array])
        
        # Aplicar LOF
        lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=contamination, novelty=False)
        lof_preds = lof.fit_predict(features)
        lof_scores_continuous = -lof.negative_outlier_factor_
        
        # Normalizar scores
        scaler = MinMaxScaler()
        lof_probabilities = scaler.fit_transform(lof_scores_continuous.reshape(-1, 1)).flatten()
        
        # Convertir anomalías (1 -> 0, -1 -> 1)
        anomalies = [(0 if pred == 1 else 1) for pred in lof_preds]
        
        return list(zip(anomalies, lof_scores_continuous.tolist(), lof_probabilities.tolist()))
    
    return udf(predict_lof, ArrayType(ArrayType(DoubleType())))

def svm_udf(nu=0.05):
    """
    UDF para aplicar One-Class SVM
    """
    from sklearn.svm import OneClassSVM
    from scipy.special import expit
    
    def predict_svm(features_array):
        # Convertir a numpy array
        features = np.array([list(row) for row in features_array])
        
        # Aplicar One-Class SVM
        svm = OneClassSVM(nu=nu, kernel='rbf', gamma='scale')
        svm_preds = svm.fit_predict(features)
        svm_scores = svm.decision_function(features)
        
        # Convertir a probabilidades
        svm_probabilities = expit(svm_scores)
        
        # Convertir anomalías (1 -> 0, -1 -> 1)
        anomalies = [(0 if pred == 1 else 1) for pred in svm_preds]
        
        return list(zip(anomalies, svm_scores.tolist(), svm_probabilities.tolist()))
    
    return udf(predict_svm, ArrayType(ArrayType(DoubleType())))

def apply_isolation_forest_spark(df, feature_cols, contamination=0.01):
    """
    Aplica Isolation Forest usando UDF
    """
    # Preparar datos
    df_features = prepare_feature_vector(df, feature_cols)
    df_scaled, _ = apply_standard_scaling(df_features)
    
    # Convertir a array para UDF
    df_array = df_scaled.select("*", col("scaled_features").alias("features_array"))
    
    # Aplicar UDF en particiones
    iso_udf = isolation_forest_udf(contamination)
    
    # Procesar en lotes para evitar problemas de memoria
    df_partitioned = df_array.repartition(10)  # Ajustar según el tamaño de datos
    
    # Aplicar usando mapPartitions para eficiencia
    def process_partition(iterator):
        from sklearn.ensemble import IsolationForest
        from scipy.special import expit
        
        rows = list(iterator)
        if not rows:
            return []
        
        # Extraer características
        features = np.array([[float(x) for x in row.features_array] for row in rows])
        
        # Aplicar Isolation Forest
        iso = IsolationForest(contamination=contamination, random_state=42)
        preds = iso.fit_predict(features)
        scores = iso.decision_function(features)
        probabilities = expit(scores * 5)
        
        # Preparar resultados
        results = []
        for i, row in enumerate(rows):
            anomaly = 1 if preds[i] == -1 else 0
            row_dict = row.asDict()
            row_dict.update({
                'anomaly_iso': anomaly,
                'iso_anomaly_scores': float(scores[i]),
                'iso_prob_sigmoid': float(probabilities[i])
            })
            results.append(row_dict)
        
        return results
    
    # Aplicar transformación
    result_rdd = df_partitioned.rdd.mapPartitions(process_partition)
    result_df = df.sparkSession.createDataFrame(result_rdd)
    
    return result_df

def apply_autoencoder_spark(df, feature_cols, epochs=30, quantile_threshold=0.95):
    """
    Aplica Autoencoder para detección de anomalías
    """
    # Convertir a pandas para entrenar autoencoder (más eficiente para redes neuronales)
    df_pandas = df.select(feature_cols).toPandas()
    
    # Normalizar
    from sklearn.preprocessing import StandardScaler, MinMaxScaler
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_pandas)
    
    # Entrenar autoencoder
    X_tensor = torch.tensor(X_scaled, dtype=torch.float32)
    model = train_autoencoder(X_tensor, epochs=epochs)
    
    # Obtener reconstrucciones
    with torch.no_grad():
        model.eval()
        reconstructed = model(X_tensor)
    
    # Calcular errores
    mse = torch.mean((X_tensor - reconstructed) ** 2, dim=1)
    
    # Normalizar errores
    minmax_scaler = MinMaxScaler()
    reconstruction_error_norm = minmax_scaler.fit_transform(
        mse.numpy().reshape(-1, 1)
    ).flatten()
    
    # Determinar anomalías
    threshold = np.quantile(mse.numpy(), quantile_threshold)
    anomalies = (mse.numpy() > threshold).astype(int)
    
    # Convertir de vuelta a Spark DataFrame
    result_pandas = df_pandas.copy()
    result_pandas['anomaly_autoencoder'] = anomalies
    result_pandas['reconstruction_error'] = mse.numpy()
    result_pandas['reconstruction_error_norm'] = reconstruction_error_norm
    
    # Crear DataFrame temporal
    temp_df = df.sparkSession.createDataFrame(result_pandas[['anomaly_autoencoder', 'reconstruction_error', 'reconstruction_error_norm']])
    
    # Añadir índice para hacer join
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window = Window.orderBy(lit(1))
    df_with_index = df.withColumn("row_id", row_number().over(window))
    temp_df_with_index = temp_df.withColumn("row_id", row_number().over(window))
    
    # Join
    result_df = df_with_index.join(temp_df_with_index, "row_id").drop("row_id")
    
    return result_df

def train_autoencoder(X_tensor, epochs=30, batch_size=256, lr=1e-3):
    """
    Entrena un autoencoder para detección de anomalías
    """
    model = Autoencoder(input_dim=X_tensor.shape[1])
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    
    train_data = TensorDataset(X_tensor)
    loader = DataLoader(train_data, batch_size=batch_size, shuffle=True)
    
    train_losses = []
    
    for epoch in range(epochs):
        model.train()
        running_loss = 0
        for batch in loader:
            inputs = batch[0]
            outputs = model(inputs)
            loss = criterion(outputs, inputs)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            running_loss += loss.item()
        
        train_losses.append(running_loss / len(loader))
        if (epoch + 1) % 10 == 0:
            print(f"Epoch {epoch+1:02d} | Train Loss: {train_losses[-1]:.4f}")
    
    return model

def compute_unsupervised_fraud_score_spark(df):
    """
    Calcula el score de fraude no supervisado combinando todas las anomalías
    """
    return df.withColumn(
        "fraude_score_no_supervisado",
        (col("anomaly_dbscan") + col("anomaly_iso") + col("anomaly_autoencoder") + 
         col("anomaly_lof") + col("anomaly_svm")) / 5.0
    )

def process_lof_batch(df, feature_cols, n_neighbors=20, contamination=0.05):
    """
    Procesa LOF en lotes para mejor rendimiento
    """
    def process_partition(iterator):
        from sklearn.neighbors import LocalOutlierFactor
        from sklearn.preprocessing import MinMaxScaler
        
        rows = list(iterator)
        if not rows:
            return []
        
        # Extraer características
        features = np.array([[float(x) for x in getattr(row, col)] for row in rows for col in feature_cols])
        features = features.reshape(len(rows), len(feature_cols))
        
        # Aplicar LOF
        lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=contamination, novelty=False)
        lof_preds = lof.fit_predict(features)
        lof_scores_continuous = -lof.negative_outlier_factor_
        
        # Normalizar
        scaler = MinMaxScaler()
        lof_probabilities = scaler.fit_transform(lof_scores_continuous.reshape(-1, 1)).flatten()
        
        # Preparar resultados
        results = []
        for i, row in enumerate(rows):
            anomaly = 0 if lof_preds[i] == 1 else 1
            row_dict = row.asDict()
            row_dict.update({
                'anomaly_lof': anomaly,
                'lof_scores': float(lof_scores_continuous[i]),
                'lof_probabilities': float(lof_probabilities[i])
            })
            results.append(row_dict)
        
        return results
    
    # Aplicar transformación
    result_rdd = df.rdd.mapPartitions(process_partition)
    result_df = df.sparkSession.createDataFrame(result_rdd)
    
    return result_df

def process_svm_batch(df, feature_cols, nu=0.05):
    """
    Procesa One-Class SVM en lotes
    """
    def process_partition(iterator):
        from sklearn.svm import OneClassSVM
        from scipy.special import expit
        
        rows = list(iterator)
        if not rows:
            return []
        
        # Extraer características
        features = np.array([[float(getattr(row, col)) for col in feature_cols] for row in rows])
        
        # Aplicar SVM
        svm = OneClassSVM(nu=nu, kernel='rbf', gamma='scale')
        svm_preds = svm.fit_predict(features)
        svm_scores = svm.decision_function(features)
        svm_probabilities = expit(svm_scores)
        
        # Preparar resultados
        results = []
        for i, row in enumerate(rows):
            anomaly = 0 if svm_preds[i] == 1 else 1
            row_dict = row.asDict()
            row_dict.update({
                'anomaly_svm': anomaly,
                'svm_scores': float(svm_scores[i]),
                'svm_probabilities': float(svm_probabilities[i])
            })
            results.append(row_dict)
        
        return results
    
    # Aplicar transformación
    result_rdd = df.rdd.mapPartitions(process_partition)
    result_df = df.sparkSession.createDataFrame(result_rdd)
    
    return result_df

# 4. Code
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entrenamiento de modelos no supervisados para detección de fraude usando PySpark")
    
    # Argumentos para archivos de entrada
    parser.add_argument("--train-x-file", type=str, default="data_training_june.csv", 
                       help="Archivo de características de entrenamiento")
    parser.add_argument("--train-y-file", type=str, default="data_training_june_y.csv", 
                       help="Archivo de etiquetas de entrenamiento")
    parser.add_argument("--val-x-file", type=str, default="data_validation_test.csv", 
                       help="Archivo de características de validación")
    parser.add_argument("--train-final-df", type=str, default="final_df_june.csv", 
                       help="DataFrame final de entrenamiento")
    parser.add_argument("--val-final-df", type=str, default="final_df_eval_test.csv", 
                       help="DataFrame final de validación")
    
    # Argumentos para archivos de salida
    parser.add_argument("--output-train-file", type=str, default="final_df_non_supervised_sept.csv", 
                       help="Archivo de salida de entrenamiento")
    parser.add_argument("--output-val-file", type=str, default="final_df_eval_non_supervised_sept.csv", 
                       help="Archivo de salida de validación")
    
    # Parámetros de los modelos
    parser.add_argument("--dbscan-eps", type=float, default=3.0, help="Parámetro eps para DBSCAN")
    parser.add_argument("--dbscan-min-samples", type=int, default=100, help="Mínimas muestras para DBSCAN")
    parser.add_argument("--iso-contamination", type=float, default=0.01, help="Contaminación para Isolation Forest")
    parser.add_argument("--lof-neighbors", type=int, default=20, help="Vecinos para LOF")
    parser.add_argument("--lof-contamination", type=float, default=0.05, help="Contaminación para LOF")
    parser.add_argument("--svm-nu", type=float, default=0.05, help="Parámetro nu para One-Class SVM")
    parser.add_argument("--autoencoder-epochs", type=int, default=30, help="Épocas para entrenar autoencoder")
    parser.add_argument("--autoencoder-threshold", type=float, default=0.95, help="Percentil threshold para autoencoder")
    
    args = parser.parse_args()
    
    # Crear sesión de Spark
    spark = create_spark_session()
    
    # Paths de SageMaker
    input_path = "/opt/ml/processing/input"
    output_path = "/opt/ml/processing/output"
    
    print("🚀 Iniciando entrenamiento de modelos no supervisados con PySpark...")
    
    # Cargar datos
    print("📂 Cargando datos...")
    X = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv(f"{input_path}/{args.train_x_file}")
    y = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv(f"{input_path}/{args.train_y_file}").select(TARGET_COL)
    X_eval = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv(f"{input_path}/{args.val_x_file}")
    
    final_df = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv(f"{input_path}/{args.train_final_df}")
    final_df_eval = spark.read.option("header", "true").option("inferSchema", "true").option("sep", ";").csv(f"{input_path}/{args.val_final_df}")
    
    print(f"✅ Datos cargados: Train={X.count()} filas, Val={X_eval.count()} filas")
    
    # Obtener columnas numéricas (excluyendo identificadores)
    feature_cols = [col for col in X.columns if col not in ['cups_sgc', 'cnt_id', TARGET_COL]]
    
    # Cache de DataFrames para mejor rendimiento
    X.cache()
    X_eval.cache()
    final_df.cache()
    final_df_eval.cache()
    
    # =====================================
    # MODELOS NO SUPERVISADOS - ENTRENAMIENTO
    # =====================================
    
    print("🤖 Aplicando DBSCAN...")
    final_df = apply_dbscan_spark(final_df, feature_cols, eps=args.dbscan_eps, min_samples=args.dbscan_min_samples)
    
    print("🌲 Aplicando Isolation Forest...")
    final_df = apply_isolation_forest_spark(final_df, feature_cols, contamination=args.iso_contamination)
    
    print("👥 Aplicando Local Outlier Factor...")
    final_df = process_lof_batch(final_df, feature_cols, n_neighbors=args.lof_neighbors, contamination=args.lof_contamination)
    
    print("🎯 Aplicando One-Class SVM...")
    final_df = process_svm_batch(final_df, feature_cols, nu=args.svm_nu)
    
    print("🧠 Aplicando Autoencoder...")
    final_df = apply_autoencoder_spark(final_df, feature_cols, epochs=args.autoencoder_epochs, quantile_threshold=args.autoencoder_threshold)
    
    # =====================================
    # MODELOS NO SUPERVISADOS - VALIDACIÓN
    # =====================================
    
    print("🔄 Aplicando modelos a datos de validación...")
    
    # Aplicar todos los modelos a validación
    final_df_eval = apply_dbscan_spark(final_df_eval, feature_cols, eps=args.dbscan_eps, min_samples=args.dbscan_min_samples)
    final_df_eval = apply_isolation_forest_spark(final_df_eval, feature_cols, contamination=args.iso_contamination)
    final_df_eval = process_lof_batch(final_df_eval, feature_cols, n_neighbors=args.lof_neighbors, contamination=args.lof_contamination)
    final_df_eval = process_svm_batch(final_df_eval, feature_cols, nu=args.svm_nu)
    final_df_eval = apply_autoencoder_spark(final_df_eval, feature_cols, epochs=args.autoencoder_epochs, quantile_threshold=args.autoencoder_threshold)
    
    # =====================================
    # CÁLCULO DE SCORE FINAL
    # =====================================
    
    print("📊 Calculando scores de fraude no supervisado...")
    final_df = compute_unsupervised_fraud_score_spark(final_df)
    final_df_eval = compute_unsupervised_fraud_score_spark(final_df_eval)
    
    # =====================================
    # GUARDAR RESULTADOS
    # =====================================
    
    print("💾 Guardando resultados...")
    
    # Extraer nombres de los meses
    train_month = args.train_x_file.replace("data_training_", "").replace(".csv", "")
    val_month = args.val_x_file.replace("data_validation_", "").replace(".csv", "")
    
    # Guardar como CSV
    final_df.coalesce(1).write.mode("overwrite").option("header", "true").option("sep", ";").csv(f"{output_path}/{train_month}/{args.output_train_file}")
    final_df_eval.coalesce(1).write.mode("overwrite").option("header", "true").option("sep", ";").csv(f"{output_path}/{val_month}/{args.output_val_file}")
    
    # Estadísticas finales
    train_stats = final_df.select(mean("fraude_score_no_supervisado").alias("mean_score"), count("*").alias("count")).collect()[0]
    val_stats = final_df_eval.select(mean("fraude_score_no_supervisado").alias("mean_score"), count("*").alias("count")).collect()[0]
    
    print(f"✅ Entrenamiento completado:")
    print(f"   📈 Train: {train_stats['count']} filas, Score promedio: {train_stats['mean_score']:.4f}")
    print(f"   📉 Val: {val_stats['count']} filas, Score promedio: {val_stats['mean_score']:.4f}")
    print(f"   📁 Archivos guardados en: {output_path}")
    
    # Resumen de anomalías detectadas
    print("\n📋 Resumen de anomalías detectadas:")
    for model in ['dbscan', 'iso', 'autoencoder', 'lof', 'svm']:
        train_count = final_df.agg(spark_sum(f"anomaly_{model}").alias("count")).collect()[0]["count"]
        val_count = final_df_eval.agg(spark_sum(f"anomaly_{model}").alias("count")).collect()[0]["count"]
        print(f"   🔍 {model.upper()}: Train={train_count}, Val={val_count}")
    
    # Cerrar sesión de Spark
    spark.stop()