# 1. Import necessary libraries
import matplotlib.pyplot as plt
import torch.nn as nn

import pandas as pd
import numpy as np

from sklearn.metrics import (
    roc_auc_score, roc_curve, precision_recall_curve, recall_score, precision_score,
)
from tqdm import tqdm

# 2. Define global variables
ufd_orange = "#f26122"
ufd_blue = "#003865"
ufd_gray = "#cccccc"

# 3. Functions/Classes

# nn.Module is the baseline of every model in PyTorch
class Autoencoder(nn.Module):
    def __init__(self, input_dim: int):
        # with super() we initialize the object nn.Module
        super().__init__()
        # We define the decoder that goes little by little reducing the dim
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 64), # 64 neurons for the first layer
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(64,32), # 32 neurons for the second layer
            nn.ReLU(),
            nn.Linear(32, 16)
            
        )
        # We define the decoder that goes little by little upgrading the dim. Reconstruction part
        self.decoder = nn.Sequential(
            nn.Linear(16, 32), 
            nn.ReLU(),
            nn.Linear(32, 64),
            nn.ReLU(),
            nn.Linear(64,input_dim)
        )

    def forward(self, x):
        """
        Forward method that defines how the information flows. It just simply applys the encoder-decoder
        architecture.
        """
        z = self.encoder(x)
        return self.decoder(z)


def plot_gain(y_true, y_proba):
    data = list(zip(y_true, y_proba))
    data = sorted(data, key=lambda x: x[1], reverse=True)
    y_sorted = np.array([d[0] for d in data])

    cum_gains = np.cumsum(y_sorted) / sum(y_sorted)
    perc_samples = np.arange(1, len(y_sorted)+1) / len(y_sorted)

    plt.figure(figsize=(7,6))
    plt.plot(perc_samples, cum_gains, color=ufd_orange, lw=2, label="Curva de ganancia")
    plt.plot([0,1], [0,1], linestyle="--", color=ufd_gray, label="Modelo aleatorio")

    # Estilo
    plt.grid(alpha=0.3)
    plt.xlabel("% de muestras")
    plt.ylabel("% de positivos acumulados")
    plt.title("Curva de Ganancia", fontsize=14, weight="bold", color=ufd_blue)
    plt.legend()
    plt.show()



def lift_chart_plot(y_test, predict_proba_test, num_chunks = None, cuts = None, color1 = ufd_orange, color2 = ufd_blue):
    """
    Generates the lift chart with the standard visualization.
    
    Parameters:
        - y_test: `DataFrame`. Object with data regarding the target variable. It corresponds to the dataset left aside for the testing phase.
        - predict_proba_test: `list` of `float`. Object with the predicted probabilites for the given model to study.
        - num_chunks: `int`. Number of equivalent groups into which the gain porportion will be studies (2: for 50% and 100%)
        - cuts: `list` of `float`. Specific decimal amount of register for which to calculate gain proportion (values from 0 to 1)
        - color1: `str`. Color code for plot representation.
        - color2: `str`. Color code for plot representation.

    Returns: 
        Plot of lift chart.
    """    
    values = sorted([(true, proba) for true, proba in zip(y_test, predict_proba_test)], key=lambda x: -x[1])
    sorted_labels = [x[0] for x in values]
    
    num_points = len(sorted_labels)
    percent_1s = sum(sorted_labels)/num_points
    
    if num_chunks is not None:
        percent_chunks = [ i/num_chunks for i in range(1, num_chunks+1)]
        
    if cuts is not None:
        percent_chunks = cuts
    
    index_chunks = [0]+[round(num_points*perc) for perc in percent_chunks]
    
    y_values = [round((sum(sorted_labels[index_chunks[index]:index_chunks[index+1]])/
                       len(sorted_labels[index_chunks[index]:index_chunks[index+1]]))/percent_1s, 2)
                for index in range(len(index_chunks)-1)]
    x_values = [round(percent*100,2) for percent in percent_chunks]
    
    plt.plot(x_values, y_values, marker='o', color = color2)
    plt.plot([0,100], [1,1], color=color1)
    plt.xlabel('Porcentaje de contadores', fontsize=14, weight='bold')
    plt.xticks(x_values)
    plt.xlim([x_values[0], x_values[-1]])
    plt.ylabel('Valor LIFT', fontsize=14, weight='bold')
    plt.title('Curva LIFT', fontsize=18, weight='bold')
    for x, y in zip(x_values, y_values):
        plt.annotate(f'{y}', (x,y), (x, y+0.07))
    plt.show()


def get_probability_thresholds(y_test, predict_proba_test, cuts):
    """
    Computes the probability thresholds to split the population into groups 
    based on predicted probabilities.

    Example: if cuts = [15, 30], the function will return thresholds that 
    split the population into 3 groups:
        - Top 15% (high risk)
        - Next 15% (medium risk)
        - Remaining 70% (low risk)

    Parameters:
        - y_test: array-like. Ground truth labels (not used for thresholds, 
                  but kept for consistency if later needed).
        - predict_proba_test: list or np.array. Predicted probabilities for the positive class.
        - cuts: list of int. Cumulative percentages that define the split. 
                Values must be between 0 and 100, sorted in ascending order.

    Returns:
        - thresholds: list of float. Probability values that can be used as cut-offs
                      to assign groups.
    """

    # Sort probabilities in descending order
    sorted_proba = np.sort(predict_proba_test)[::-1]
    n = len(sorted_proba)
    
    thresholds = []
    for cut in cuts:
        # index corresponding to the cut (top % of population)
        idx = int(np.ceil(n * (cut/100)))
        # take the probability at that position as threshold
        thresholds.append(float(sorted_proba[idx-1]))  
    
    return thresholds

def ks_stat(y_true, y_proba):
    fpr, tpr, _ = roc_curve(y_true, y_proba)
    return max(tpr - fpr)

def find_optimal_thres(y_true, y_proba, objetivo="f1", plot=True):
    """
    Finds the optimal threshold to maximize recall or F1-score.
    
    Parameters:
        y_true: array-like, true labels (0 or 1)
        y_proba: array-like, predicted probabilities
        objective: "recall" or "f1"
        plot: if True, plots the Precision-Recall curve
    
    Returns:
        optimal_threshold, results: (dict with metrics)
    """
    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    
    # Avoid dividing by zero in f1
    f1_scores = 2 * (precision * recall) / (precision + recall + 1e-10)
    
    if objetivo == "recall":
        idx_optimo = np.argmax(recall)   # max recall
    elif objetivo == "f1":
        idx_optimo = np.argmax(f1_scores)  # max f1
    else:
        raise ValueError("objective must be 'recall' or 'f1'")
    
    threshold_optimo = thresholds[idx_optimo if idx_optimo < len(thresholds) else -1]
    
    # Binary predictions with optimal threshold
    y_pred = (y_proba >= threshold_optimo).astype(int)
    
    resultados = {
        "threshold": threshold_optimo,
        "precision": precision_score(y_true, y_pred),
        "recall": recall_score(y_true, y_pred),
        "f1": 2 * (precision_score(y_true, y_pred) * recall_score(y_true, y_pred)) / 
              (precision_score(y_true, y_pred) + recall_score(y_true, y_pred) + 1e-10)
    }
    
    if plot:
        plt.figure(figsize=(8,6))
        plt.plot(recall, precision, marker='.')
        plt.scatter(recall[idx_optimo], precision[idx_optimo], color='red', label=f"Optimal threshold={threshold_optimo:.2f}")
        plt.xlabel("Recall")
        plt.ylabel("Precision")
        plt.title("Precision-Recall curve")
        plt.legend()
        plt.grid(True)
        plt.show()
    
    return threshold_optimo, resultados

def comparar_distribuciones_target(X: pd.DataFrame, y: pd.Series,
                                   X_eval: pd.DataFrame, y_eval: pd.Series,
                                   bins=30):
    """
    Compara distribuciones de variables numéricas en train (X, y) y test (X_eval, y_eval),
    diferenciando por target (0/1).

    - Cada variable -> un plot con dos subplots (train izquierda, test derecha).
    - Histograma con densidad para cada clase del target.
    """
    # asegurar que X y X_eval tienen las mismas columnas
    common_cols = [col for col in X.columns if col in X_eval.columns]
    
    for col in common_cols:
        plt.figure(figsize=(10, 4))

        # --- Train (marzo) ---
        plt.subplot(1, 2, 1)
        plt.hist(X[y==0][col].dropna(), bins=bins, density=True, alpha=0.5, label="Target=0")
        plt.hist(X[y==1][col].dropna(), bins=bins, density=True, alpha=0.5, label="Target=1")
        plt.title(f"{col} - Marzo")
        plt.xlabel(col); plt.ylabel("Densidad")
        plt.legend()

        # --- Test (abril) ---
        plt.subplot(1, 2, 2)
        plt.hist(X_eval[y_eval==0][col].dropna(), bins=bins, density=True, alpha=0.5, label="Target=0")
        plt.hist(X_eval[y_eval==1][col].dropna(), bins=bins, density=True, alpha=0.5, label="Target=1")
        plt.title(f"{col} - Abril")
        plt.xlabel(col); plt.ylabel("Densidad")
        plt.legend()

        plt.tight_layout()
        plt.show()


def compute_individual_auc(df, y, target_col):
    """
    Computes the individual ROC AUC of each feature against the target.
    
    Parameters:
        df : pd.DataFrame
            DataFrame containing features and target
        target_col : str
            Name of the target column (binary: 0/1)
    
    Returns
        auc_df : pd.DataFrame
            DataFrame with feature name and corresponding AUC score
    """

    auc_list = []
    features = [col for col in df.columns if col not in [target_col]]

    for col in tqdm(features, desc="Computing AUCs"):
        if df[col].nunique() > 1:  # avoid constant columns
            auc = roc_auc_score(y, df[col])
            auc_list.append((col, auc))

    auc_df = pd.DataFrame(auc_list, columns=["feature", "auc"]).sort_values("auc", ascending=False).reset_index(drop=True)
    return auc_df

def find_cost_optimal_threshold(y_true, proba, cost_fn, cost_fp, grid=200):
    thresholds = np.linspace(0, 1, grid)
    best = None
    
    for t in thresholds:
        pred = (proba >= t).astype(int)
        FN = ((y_true == 1) & (pred == 0)).sum()
        FP = ((y_true == 0) & (pred == 1)).sum()
        cost = FN * cost_fn + FP * cost_fp
        
        if best is None or cost < best[0]:
            best = (cost, t, FN, FP)
    return best