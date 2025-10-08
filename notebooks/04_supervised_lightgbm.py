# 1. Import necessary libraries
import argparse
import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, sum as spark_sum, mean, stddev, count, desc, isnan, isnull
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, StringType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType
import lightgbm as lgb
from lightgbm import LGBMClassifier
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import roc_auc_score, roc_curve, confusion_matrix, classification_report, f1_score
import pandas as pd
from utils import compute_individual_auc, find_optimal_thres, get_probability_thresholds, plot_gain, lift_chart_plot, ks_stat, find_cost_optimal_threshold

# 2. Define global variables
TARGET_COL = "target"
UFD_ORANGE = "#f26122"
UFD_BLUE = "#003865"
UFD_GRAY = "#cccccc"

# 3. Functions
def create_spark_session(app_name="UFD-Fraud-Detection-LightGBM"):
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
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def handle_missing_values_spark_simple(df, target_col=TARGET_COL):
    """
    Maneja valores faltantes imputando con -1 (versión simplificada)
    """
    # Obtener columnas numéricas (excluyendo identificadores y target)
    exclude_cols = ['cups_sgc', 'cnt_id', target_col]
    numeric_cols = [col for col in df.columns if col not in exclude_cols]
    
    # Imputar valores faltantes con -1
    for col_name in numeric_cols:
        df = df.withColumn(col_name, 
                          when(col(col_name).isNull() | isnan(col(col_name)), -1).otherwise(col(col_name)))
    
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

def train_lightgbm_model(X_train_pandas, y_train_pandas, n_estimators=100, max_depth=4, 
                        random_state=42, is_unbalance=True):
    """
    Entrena un modelo LightGBM usando pandas (más eficiente para LightGBM)
    """
    model = LGBMClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        random_state=random_state,
        is_unbalance=is_unbalance,
        verbose=-1
    )
    
    model.fit(X_train_pandas, y_train_pandas)
    return model

def compute_individual_auc_spark(df, target_col=TARGET_COL):
    """
    Calcula AUC individual para cada característica usando PySpark
    """
    exclude_cols = ['cups_sgc', 'cnt_id', target_col]
    feature_cols = [col for col in df.columns if col not in exclude_cols]
    
    auc_results = []
    
    # Convertir a pandas para cálculo de AUC (más eficiente)
    df_pandas = df.select(feature_cols + [target_col]).toPandas()
    
    for feature in feature_cols:
        try:
            if df_pandas[feature].nunique() > 1:
                auc_score = roc_auc_score(df_pandas[target_col], df_pandas[feature])
                auc_results.append({'feature': feature, 'auc': auc_score})
        except:
            continue
    
    return sorted(auc_results, key=lambda x: x['auc'], reverse=True)

def calculate_thresholds_and_categories_spark(df, score_col, target_col, cuts=[25, 35]):
    """
    Calcula umbrales y categorías de fraude usando PySpark
    """
    # Convertir a pandas para cálculo de percentiles (más eficiente)
    scores_pandas = df.select(score_col, target_col).toPandas()
    
    # Calcular umbrales
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

def save_feature_importance_spark(spark, model, feature_names, output_path):
    """
    Guarda la importancia de características usando PySpark
    """
    importances = model.feature_importances_
    
    # Crear DataFrame de importancia
    importance_data = [(feature_names[i], float(importances[i])) 
                      for i in range(len(feature_names))]
    
    importance_df = spark.createDataFrame(importance_data, ["feature", "importance"])
    importance_df = importance_df.orderBy(desc("importance"))
    
    # Guardar top 10
    importance_df.limit(10).coalesce(1).write.mode("overwrite").option("header", "true").option("sep", ";").csv(output_path)
    
    return importance_df

def compute_final_scoring_spark(df, unsupervised_col="fraude_score_no_supervisado", 
                               supervised_col="pred_proba", weight_unsupervised=0.3, weight_supervised=0.7):
    """
    Calcula el scoring final combinando modelos supervisados y no supervisados
    """
    return df.withColumn(
        "final_scoring",
        col(unsupervised_col) * weight_unsupervised + col(supervised_col) * weight_supervised
    )

def add_predictions_to_spark_df(spark, df, model, feature_cols):
    """
    Añade predicciones del modelo LightGBM al DataFrame de Spark
    """
    # Convertir características a pandas para predicción
    features_pandas = df.select(feature_cols).toPandas()
    
    # Obtener predicciones
    probabilities = model.predict_proba(features_pandas)[:, 1]
    predictions = model.predict(features_pandas)
    
    # Crear DataFrame temporal con predicciones
    pred_data = [(float(prob), int(pred)) for prob, pred in zip(probabilities, predictions)]
    pred_df = spark.createDataFrame(pred_data, ["pred_proba", "prediction"])
    
    # Añadir índice para hacer join
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    window = Window.orderBy(lit(1))
    df_with_index = df.withColumn("row_id", row_number().over(window))
    pred_df_with_index = pred_df.withColumn("row_id", row_number().over(window))
    
    # Join y eliminar índice
    result_df = df_with_index.join(pred_df_with_index, "row_id").drop("row_id")
    
    return result_df

def evaluate_model_performance(y_true, y_pred, y_proba, dataset_name="test"):
    """
    Evalúa el rendimiento del modelo
    """
    # Métricas básicas
    auc_score = roc_auc_score(y_true, y_proba)
    f1 = f1_score(y_true, y_pred)
    
    # Matriz de confusión
    cm = confusion_matrix(y_true, y_pred)
    
    # Reporte de clasificación
    report = classification_report(y_true, y_pred, output_dict=True)
    
    print(f"\n📊 Métricas para {dataset_name}:")
    print(f"   AUC: {auc_score:.4f}")
    print(f"   F1-Score: {f1:.4f}")
    print(f"   Precisión: {report['1']['precision']:.4f}")
    print(f"   Recall: {report['1']['recall']:.4f}")
    
    return {
        'auc': auc_score,
        'f1': f1,
        'precision': report['1']['precision'],
        'recall': report['1']['recall'],
        'confusion_matrix': cm.tolist()
    }

def calibrate_model_predictions(model, X_train, y_train, X_eval, method="isotonic", cv=5):
    """
    Calibra las predicciones del modelo usando CalibratedClassifierCV
    """
    calibrated_model = CalibratedClassifierCV(
        estimator=model,
        method=method,
        cv=cv
    )
    
    calibrated_model.fit(X_train, y_train)
    calibrated_proba = calibrated_model.predict_proba(X_eval)[:, 1]
    
    return calibrated_model, calibrated_proba

def compute_cost_optimal_threshold(y_true, y_proba, cost_fn=1000, cost_fp=50):
    """
    Calcula el umbral óptimo basado en costes
    """
    best_cost, best_thr, fn, fp = find_cost_optimal_threshold(y_true, y_proba, cost_fn, cost_fp)
    
    print(f"💰 Análisis de costes:")
    print(f"   Umbral coste-óptimo: {best_thr:.3f}")
    print(f"   Coste esperado: {best_cost}")
    print(f"   Falsos Negativos: {fn}")
    print(f"   Falsos Positivos: {fp}")
    
    return best_thr, best_cost

# 4. Code
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entrenamiento de modelo LightGBM supervisado para detección de fraude usando PySpark")
    
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
    parser.add_argument("--output-feature-importance", type=str, default="feature_importance_lgbm_june.csv", 
                       help="Archivo de importancia de características")
    parser.add_argument("--output-train-final", type=str, default="final_df_supervised_lgbm_train.csv", 
                       help="DataFrame final de entrenamiento con predicciones")
    parser.add_argument("--output-val-final", type=str, default="final_df_supervised_lgbm_val.csv", 
                       help="DataFrame final de validación con predicciones")
    
    # Parámetros del modelo
    parser.add_argument("--n-estimators", type=int, default=100, help="Número de estimadores para LightGBM")
    parser.add_argument("--max-depth", type=int, default=4, help="Profundidad máxima de árboles")
    parser.add_argument("--random-state", type=int, default=42, help="Semilla aleatoria")
    parser.add_argument("--is-unbalance", type=bool, default=True, help="Usar balanceado automático")
    parser.add_argument("--cuts", nargs='+', type=float, default=[25, 35], help="Cortes para categorización")
    parser.add_argument("--weight-unsupervised", type=float, default=0.3, help="Peso para score no supervisado")
    parser.add_argument("--weight-supervised", type=float, default=0.7, help="Peso para score supervisado")
    
    # Parámetros de calibración y costes
    parser.add_argument("--calibration-method", type=str, default="isotonic", help="Método de calibración")
    parser.add_argument("--cost-fn", type=float, default=1000, help="Coste de falso negativo")
    parser.add_argument("--cost-fp", type=float, default=50, help="Coste de falso positivo")
    
    args = parser.parse_args()
    
    # Crear sesión de Spark
    spark = create_spark_session()
    
    # Paths de SageMaker
    input_path = "/opt/ml/processing/input"
    output_path = "/opt/ml/processing/output"
    
    print("🚀 Iniciando entrenamiento de modelo LightGBM supervisado con PySpark...")
    
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
    
    # Manejo de valores faltantes (versión simplificada)
    print("🔄 Manejando valores faltantes...")
    train_df = handle_missing_values_spark_simple(train_df)
    eval_df = handle_missing_values_spark_simple(eval_df)
    
    # Target encoding para variables categóricas
    if cat_cols:
        print("🏷️ Aplicando target encoding...")
        train_df, eval_df = apply_target_encoding_spark(train_df, eval_df, cat_cols)
    
    # =====================================
    # ENTRENAMIENTO DEL MODELO
    # =====================================
    
    print("🤖 Entrenando modelo LightGBM...")
    
    # Obtener columnas de características
    feature_cols = [col for col in train_df.columns if col not in ['cups_sgc', 'cnt_id', TARGET_COL]]
    
    # Convertir a pandas para entrenar LightGBM (más eficiente)
    X_train_pandas = train_df.select(feature_cols).toPandas()
    y_train_pandas = train_df.select(TARGET_COL).toPandas()[TARGET_COL]
    X_eval_pandas = eval_df.select(feature_cols).toPandas()
    y_eval_pandas = eval_df.select(TARGET_COL).toPandas()[TARGET_COL]
    
    # Entrenar modelo
    model = train_lightgbm_model(
        X_train_pandas, y_train_pandas,
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        random_state=args.random_state,
        is_unbalance=args.is_unbalance
    )
    
    print("✅ Modelo LightGBM entrenado correctamente")
    
    # =====================================
    # PREDICCIONES Y MÉTRICAS
    # =====================================
    
    print("📊 Generando predicciones y calculando métricas...")
    
    # Predicciones de entrenamiento
    y_proba_train = model.predict_proba(X_train_pandas)[:, 1]
    y_pred_train = model.predict(X_train_pandas)
    
    # Predicciones de validación
    y_proba_eval = model.predict_proba(X_eval_pandas)[:, 1]
    y_pred_eval = model.predict(X_eval_pandas)
    
    # Calcular umbrales óptimos
    thres_train, _ = find_optimal_thres(y_train_pandas, y_proba_train)
    
    # Evaluar rendimiento
    train_metrics = evaluate_model_performance(y_train_pandas, y_pred_train, y_proba_train, "entrenamiento")
    val_metrics = evaluate_model_performance(y_eval_pandas, y_pred_eval, y_proba_eval, "validación")
    
    # Calcular AUC individual para características principales
    print("📈 Calculando AUC individual por característica...")
    auc_results_train = compute_individual_auc_spark(train_df)
    auc_results_eval = compute_individual_auc_spark(eval_df)
    
    print(f"🔝 Top 5 características por AUC (entrenamiento):")
    for i, result in enumerate(auc_results_train[:5]):
        print(f"   {i+1}. {result['feature']}: {result['auc']:.4f}")
    
    # =====================================
    # CALIBRACIÓN DEL MODELO
    # =====================================
    
    print("⚖️ Calibrando predicciones del modelo...")
    calibrated_model, proba_cal_val = calibrate_model_predictions(
        model, X_train_pandas, y_train_pandas, X_eval_pandas,
        method=args.calibration_method
    )
    
    # Análisis de costes
    best_threshold, best_cost = compute_cost_optimal_threshold(
        y_eval_pandas, proba_cal_val, 
        cost_fn=args.cost_fn, cost_fp=args.cost_fp
    )
    
    # =====================================
    # GUARDAR IMPORTANCIA DE CARACTERÍSTICAS
    # =====================================
    
    print("💾 Guardando importancia de características...")
    feature_importance_df = save_feature_importance_spark(
        spark, model, feature_cols, 
        f"{output_path}/powerbi/{args.output_feature_importance}"
    )
    
    # =====================================
    # AÑADIR PREDICCIONES A DATAFRAMES FINALES
    # =====================================
    
    print("🔗 Añadiendo predicciones a DataFrames finales...")
    
    # Añadir predicciones a DataFrames finales
    final_df = add_predictions_to_spark_df(spark, final_df, model, feature_cols)
    final_df_eval = add_predictions_to_spark_df(spark, final_df_eval, model, feature_cols)
    
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
    
    # Resumen de rendimiento
    print(f"\n🏆 Resumen de rendimiento del modelo:")
    print(f"   🎯 AUC Train: {train_metrics['auc']:.4f}")
    print(f"   🎯 AUC Val: {val_metrics['auc']:.4f}")
    print(f"   💰 Umbral óptimo por coste: {best_threshold:.3f}")
    print(f"   💰 Coste esperado: {best_cost}")
    print(f"   📏 Características principales: {len(feature_cols)}")
    print(f"   📊 Categorías de fraude: Bajo, Medio, Alto")
    
    print(f"\n✅ Entrenamiento completado exitosamente!")
    print(f"   📁 Archivos guardados en: {output_path}")
    print(f"   🔧 Modelo: LightGBM con {args.n_estimators} estimadores")
    print(f"   ⚡ Procesamiento distribuido: PySpark")
    
    # Cerrar sesión de Spark
    spark.stop()