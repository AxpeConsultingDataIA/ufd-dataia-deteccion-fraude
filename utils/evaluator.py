# 1. Import necessary libraries
import numpy as np
import torch
from torch_geometric.data import Data
import torch.nn.functional as F
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import matplotlib.pyplot as plt
import seaborn as sns

from typing import Dict

# 2. Classes/Functions
class FraudEvaluator:
    """
    Evaluador del modelo de fraudes. Aquí utilizaremos el modelo entrenado para ver cuan bien lo hemos hecho
    """
    
    def __init__(self, model, device: str = 'cpu'):
        self.model = model.to(device)
        self.device = device
        
    def predict(self, data: Data) -> np.ndarray:
        """
        Hace predicciones
        """
        self.model.eval()
        with torch.no_grad():
            out = self.model(data.x, data.edge_index)
            pred = out.argmax(dim=1)
        return pred.cpu().numpy()
    
    def predict_proba(self, data: Data) -> np.ndarray:
        """
        Obtiene probabilidades
        """
        self.model.eval()
        with torch.no_grad():
            out = self.model(data.x, data.edge_index)
            proba = F.softmax(out, dim=1) # convertir a probabilidades
        return proba.cpu().numpy()
    # a diferencia del predict, el predict_proba nos devuelve directamente las probabilidades
    
    def evaluate_detailed(self, data: Data, test_mask: torch.Tensor) -> Dict:
        """Evaluación detallada"""
        predictions = self.predict(data)
        probabilities = self.predict_proba(data)
        
        # Filtrar solo casos de test, aquellos datos que el modelo nunca vio durante el training.
        test_indices = test_mask.nonzero().squeeze()
        y_true = data.y[test_indices].cpu().numpy()
        y_pred = predictions[test_indices]
        y_proba = probabilities[test_indices]
        
        # Métricas
        results = {
            'classification_report': classification_report(y_true, y_pred, 
                                                         target_names=['Normal', 'Fraude', 'Irregularidad']),
            'confusion_matrix': confusion_matrix(y_true, y_pred),
            'predictions': y_pred,
            'probabilities': y_proba,
            'true_labels': y_true
        }
        
        # ROC AUC para detección binaria (fraude vs no fraude)
        binary_true = (y_true > 0).astype(int)
        binary_proba = y_proba[:, 1] + y_proba[:, 2]  # Prob de fraude + irregularidad
        results['roc_auc'] = roc_auc_score(binary_true, binary_proba)
        
        return results
    
    def plot_training_history(self, history: Dict):
        """
        Plotea historial de entrenamiento
        """
        fig, axes = plt.subplots(1, 2, figsize=(15, 5))
        
        # Loss
        axes[0].plot(history['train_losses'], label='Train Loss')
        axes[0].plot(history['val_losses'], label='Val Loss')
        axes[0].set_title('Pérdida durante el entrenamiento')
        axes[0].set_xlabel('Época')
        axes[0].set_ylabel('Loss')
        axes[0].legend()
        axes[0].grid(True)
        
        # Accuracy
        axes[1].plot(history['train_accuracies'], label='Train Accuracy')
        axes[1].plot(history['val_accuracies'], label='Val Accuracy')
        axes[1].set_title('Precisión durante el entrenamiento')
        axes[1].set_xlabel('Época')
        axes[1].set_ylabel('Accuracy')
        axes[1].legend()
        axes[1].grid(True)
        
        plt.tight_layout()
        plt.show()
    
    def plot_confusion_matrix(self, cm: np.ndarray):
        """
        Plotea matriz de confusión con seaborn
        """
        plt.figure(figsize=(8, 6))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                   xticklabels=['Normal', 'Fraude', 'Irregularidad'],
                   yticklabels=['Normal', 'Fraude', 'Irregularidad'])
        plt.title('Matriz de Confusión')
        plt.xlabel('Predicción')
        plt.ylabel('Verdadero')
        plt.show()

class HeterogeneousEvaluator:
    """Evaluador especializado para modelos heterogéneos"""
    
    def __init__(self, model, device='cpu'):
        self.model = model.to(device)
        self.device = device
        
    def predict(self, data):
        """Hace predicciones"""
        self.model.eval()
        with torch.no_grad():
            out = self.model(data.x_dict, data.edge_index_dict)
            pred = out.argmax(dim=1)
        return pred.cpu().numpy()
    
    def predict_proba(self, data):
        """Obtiene probabilidades"""
        self.model.eval()
        with torch.no_grad():
            out = self.model(data.x_dict, data.edge_index_dict)
            proba = F.softmax(out, dim=1)
        return proba.cpu().numpy()
    
    def evaluate_detailed(self, data, test_mask):
        """Evaluación detallada"""
        predictions = self.predict(data)
        probabilities = self.predict_proba(data)
        
        # Filtrar solo casos de test
        test_indices = test_mask.nonzero().squeeze()
        y_true = data['contador'].y[test_indices].cpu().numpy()
        y_pred = predictions[test_indices]
        y_proba = probabilities[test_indices]
        
        # Métricas
        results = {
            'classification_report': classification_report(
                y_true, y_pred, 
                target_names=['Normal', 'Fraude', 'Irregularidad']
            ),
            'confusion_matrix': confusion_matrix(y_true, y_pred),
            'predictions': y_pred,
            'probabilities': y_proba,
            'true_labels': y_true
        }
        
        # ROC AUC para detección binaria
        binary_true = (y_true > 0).astype(int)
        binary_proba = y_proba[:, 1] + y_proba[:, 2]
        results['roc_auc'] = roc_auc_score(binary_true, binary_proba)
        
        return results
    
    def analyze_attention_weights(self, data):
        """Analiza pesos de atención del modelo"""
        self.model.eval()
        with torch.no_grad():
            # Obtener pesos de atención inter-tipo
            attention_weights = F.softmax(self.model.info_attention, dim=0)
            
        info_types = ['Suministro', 'Ubicación', 'Concentrador', 'Transformador', 'Contadores Similares']
        
        return dict(zip(info_types, attention_weights.cpu().numpy()))
