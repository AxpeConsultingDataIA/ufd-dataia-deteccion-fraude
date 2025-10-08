# 1. Import necessary libraries
import argparse
import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.cluster import DBSCAN
from sklearn.ensemble import IsolationForest
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.svm import OneClassSVM
from sklearn.neighbors import LocalOutlierFactor
from scipy.special import expit
from utils import Autoencoder

# 2. Define global variables
TARGET_COL = "target"
UFD_ORANGE = "#f26122"
UFD_BLUE = "#003865"
UFD_GRAY = "#cccccc"

# 3. Functions
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

def apply_dbscan_anomaly_detection(X_scaled, eps=3, min_samples=100):
    """
    Aplica DBSCAN para detección de anomalías
    """
    db = DBSCAN(eps=eps, min_samples=min_samples).fit(X_scaled)
    anomalies = db.labels_ == -1
    # Convertir True/False a 1/0 (True->0, False->1)
    anomalies = anomalies.replace({True: 0, False: 1})
    return anomalies

def apply_isolation_forest_anomaly_detection(X_scaled, contamination=0.01, random_state=42):
    """
    Aplica Isolation Forest para detección de anomalías
    """
    iso = IsolationForest(contamination=contamination, random_state=random_state)
    preds = iso.fit_predict(X_scaled)
    
    # Extraer scores continuos
    anomaly_scores = iso.decision_function(X_scaled)
    iso_probabilities_sigmoid = expit(anomaly_scores * 5)
    
    anomalies = preds == -1
    
    return anomalies, anomaly_scores, iso_probabilities_sigmoid

def apply_lof_anomaly_detection(X_scaled, n_neighbors=20, contamination=0.05):
    """
    Aplica Local Outlier Factor para detección de anomalías
    """
    lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=contamination, novelty=False)
    lof_scores = lof.fit_predict(X_scaled)
    lof_scores_continuous = -lof.negative_outlier_factor_
    
    # Normalizar
    lof_probabilities = MinMaxScaler().fit_transform(
        lof_scores_continuous.reshape(-1, 1)
    ).flatten()
    
    # Convertir 1->0, -1->1
    anomalies = pd.Series(lof_scores).replace({1: 0, -1: 1})
    
    return anomalies, lof_scores_continuous, lof_probabilities

def apply_svm_anomaly_detection(X_scaled, nu=0.05, kernel='rbf', gamma='scale'):
    """
    Aplica One-Class SVM para detección de anomalías
    """
    svm = OneClassSVM(nu=nu, kernel=kernel, gamma=gamma)
    svm_preds = svm.fit_predict(X_scaled)
    svm_scores = svm.decision_function(X_scaled)
    
    # Convertir a probabilidades
    svm_probabilities = expit(svm_scores)
    
    # Convertir 1->0, -1->1
    anomalies = pd.Series(svm_preds).replace({1: 0, -1: 1})
    
    return anomalies, svm_scores, svm_probabilities

def apply_autoencoder_anomaly_detection(X_scaled, epochs=30, quantile_threshold=0.95):
    """
    Aplica Autoencoder para detección de anomalías
    """
    X_tensor = torch.tensor(X_scaled, dtype=torch.float32)
    
    # Entrenar autoencoder
    model = train_autoencoder(X_tensor, epochs=epochs)
    
    # Obtener reconstrucciones
    with torch.no_grad():
        model.eval()
        reconstructed = model(X_tensor)
    
    # Calcular error de reconstrucción
    mse = torch.mean((X_tensor - reconstructed) ** 2, dim=1)
    
    # Normalizar error
    minmax_scaler = MinMaxScaler()
    reconstruction_error_norm = minmax_scaler.fit_transform(
        mse.numpy().reshape(-1, 1)
    ).flatten()
    
    # Determinar anomalías
    threshold = np.quantile(mse.numpy(), quantile_threshold)
    anomalies = mse.numpy() > threshold
    
    return anomalies, mse.numpy(), reconstruction_error_norm

def compute_unsupervised_fraud_score(df):
    """
    Calcula el score de fraude no supervisado combinando todas las anomalías
    """
    anomaly_columns = ['anomaly_dbscan', 'anomaly_iso', 'anomaly_autoencoder', 
                      'anomaly_lof', 'anomaly_svm']
    
    fraud_score = df[anomaly_columns].astype(int).sum(axis=1) / len(anomaly_columns)
    
    return fraud_score

# 4. Code
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entrenamiento de modelos no supervisados para detección de fraude")
    
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
    
    # Paths de SageMaker
    input_path = "/opt/ml/processing/input"
    output_path = "/opt/ml/processing/output"
    
    os.makedirs(output_path, exist_ok=True)
    
    print("🚀 Iniciando entrenamiento de modelos no supervisados...")
    
    # Cargar datos
    print("📂 Cargando datos...")
    X = pd.read_csv(f"{input_path}/{args.train_x_file}", sep=";")
    y = pd.read_csv(f"{input_path}/{args.train_y_file}", sep=";")[TARGET_COL]
    X_eval = pd.read_csv(f"{input_path}/{args.val_x_file}", sep=";")
    
    final_df = pd.read_csv(f"{input_path}/{args.train_final_df}", sep=";")
    final_df_eval = pd.read_csv(f"{input_path}/{args.val_final_df}", sep=";")
    
    print(f"✅ Datos cargados: Train={len(X)} filas, Val={len(X_eval)} filas")
    
    # Preprocesamiento
    print("🔧 Aplicando normalización...")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    X_val_scaled = scaler.transform(X_eval)  # Usar transform, no fit_transform para validación
    
    # =====================================
    # MODELOS NO SUPERVISADOS - ENTRENAMIENTO
    # =====================================
    
    print("🤖 Aplicando DBSCAN...")
    final_df["anomaly_dbscan"] = apply_dbscan_anomaly_detection(
        X_scaled, eps=args.dbscan_eps, min_samples=args.dbscan_min_samples
    )
    
    print("🌲 Aplicando Isolation Forest...")
    iso_anomalies, iso_scores, iso_probs = apply_isolation_forest_anomaly_detection(
        X_scaled, contamination=args.iso_contamination
    )
    final_df["anomaly_iso"] = iso_anomalies
    final_df["iso_anomaly_scores"] = iso_scores
    final_df["iso_prob_sigmoid"] = iso_probs
    
    print("👥 Aplicando Local Outlier Factor...")
    lof_anomalies, lof_scores, lof_probs = apply_lof_anomaly_detection(
        X_scaled, n_neighbors=args.lof_neighbors, contamination=args.lof_contamination
    )
    final_df["anomaly_lof"] = lof_anomalies
    final_df["lof_scores"] = lof_scores
    final_df["lof_probabilities"] = lof_probs
    
    print("🎯 Aplicando One-Class SVM...")
    svm_anomalies, svm_scores, svm_probs = apply_svm_anomaly_detection(
        X_scaled, nu=args.svm_nu
    )
    final_df["anomaly_svm"] = svm_anomalies
    final_df["svm_scores"] = svm_scores
    final_df["svm_probabilities"] = svm_probs
    
    print("🧠 Aplicando Autoencoder...")
    ae_anomalies, ae_errors, ae_errors_norm = apply_autoencoder_anomaly_detection(
        X_scaled, epochs=args.autoencoder_epochs, quantile_threshold=args.autoencoder_threshold
    )
    final_df["anomaly_autoencoder"] = ae_anomalies
    final_df["reconstruction_error"] = ae_errors
    final_df["reconstruction_error_norm"] = ae_errors_norm
    
    # =====================================
    # MODELOS NO SUPERVISADOS - VALIDACIÓN
    # =====================================
    
    print("🔄 Aplicando modelos a datos de validación...")
    
    # DBSCAN para validación
    final_df_eval["anomaly_dbscan"] = apply_dbscan_anomaly_detection(
        X_val_scaled, eps=args.dbscan_eps, min_samples=args.dbscan_min_samples
    )
    
    # Isolation Forest para validación
    iso_anomalies_val, iso_scores_val, iso_probs_val = apply_isolation_forest_anomaly_detection(
        X_val_scaled, contamination=args.iso_contamination
    )
    final_df_eval["anomaly_iso"] = iso_anomalies_val
    final_df_eval["iso_anomaly_scores"] = iso_scores_val
    final_df_eval["iso_prob_sigmoid"] = iso_probs_val
    
    # LOF para validación
    lof_anomalies_val, lof_scores_val, lof_probs_val = apply_lof_anomaly_detection(
        X_val_scaled, n_neighbors=args.lof_neighbors, contamination=args.lof_contamination
    )
    final_df_eval["anomaly_lof"] = lof_anomalies_val
    final_df_eval["lof_scores"] = lof_scores_val
    final_df_eval["lof_probabilities"] = lof_probs_val
    
    # SVM para validación
    svm_anomalies_val, svm_scores_val, svm_probs_val = apply_svm_anomaly_detection(
        X_val_scaled, nu=args.svm_nu
    )
    final_df_eval["anomaly_svm"] = svm_anomalies_val
    final_df_eval["svm_scores"] = svm_scores_val
    final_df_eval["svm_probabilities"] = svm_probs_val
    
    # Autoencoder para validación
    ae_anomalies_val, ae_errors_val, ae_errors_norm_val = apply_autoencoder_anomaly_detection(
        X_val_scaled, epochs=args.autoencoder_epochs, quantile_threshold=args.autoencoder_threshold
    )
    final_df_eval["anomaly_autoencoder"] = ae_anomalies_val
    final_df_eval["reconstruction_error"] = ae_errors_val
    final_df_eval["reconstruction_error_norm"] = ae_errors_norm_val
    
    # =====================================
    # CÁLCULO DE SCORE FINAL
    # =====================================
    
    print("📊 Calculando scores de fraude no supervisado...")
    final_df["fraude_score_no_supervisado"] = compute_unsupervised_fraud_score(final_df)
    final_df_eval["fraude_score_no_supervisado"] = compute_unsupervised_fraud_score(final_df_eval)
    
    # =====================================
    # GUARDAR RESULTADOS
    # =====================================
    
    print("💾 Guardando resultados...")
    
    # Extraer nombres de los meses de los archivos
    train_month = args.train_x_file.replace("data_training_", "").replace(".csv", "")
    val_month = args.val_x_file.replace("data_validation_", "").replace(".csv", "")
    
    # Crear directorios de salida
    train_output_dir = f"{output_path}/{train_month}"
    val_output_dir = f"{output_path}/{val_month}"
    
    os.makedirs(train_output_dir, exist_ok=True)
    os.makedirs(val_output_dir, exist_ok=True)
    
    # Guardar archivos
    final_df.to_csv(f"{train_output_dir}/{args.output_train_file}", sep=";", index=False)
    final_df_eval.to_csv(f"{val_output_dir}/{args.output_val_file}", sep=";", index=False)
    
    # Estadísticas finales
    train_anomalies = final_df["fraude_score_no_supervisado"].describe()
    val_anomalies = final_df_eval["fraude_score_no_supervisado"].describe()
    
    print(f"✅ Entrenamiento completado:")
    print(f"   📈 Train: {len(final_df)} filas, Score promedio: {train_anomalies['mean']:.4f}")
    print(f"   📉 Val: {len(final_df_eval)} filas, Score promedio: {val_anomalies['mean']:.4f}")
    print(f"   📁 Archivos guardados en: {output_path}")
    
    # Resumen de anomalías detectadas
    print("\n📋 Resumen de anomalías detectadas:")
    for model in ['dbscan', 'iso', 'autoencoder', 'lof', 'svm']:
        train_count = final_df[f"anomaly_{model}"].sum()
        val_count = final_df_eval[f"anomaly_{model}"].sum()
        print(f"   🔍 {model.upper()}: Train={train_count}, Val={val_count}")