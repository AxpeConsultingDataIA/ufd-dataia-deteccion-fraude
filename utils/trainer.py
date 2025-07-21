# 1. Import necessary libraries
import torch
from torch_geometric.data import Data
import torch.nn as nn

from typing import Tuple, Dict

from config import *


# 2. Classes/Functions
class FraudTrainer:
    """
    Trainer for the simple GNN model
    """

    def __init__(self, model, device: str = "cpu"):
        """
        The initialization of the trainer class.
        """
        self.model = model.to(device)
        self.device = device
        self.optimizer = torch.optim.Adam(
            model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY
        )
        self.criterion = nn.NLLLoss()
        self.train_losses = []
        self.val_losses = []
        self.train_accuracies = []
        self.val_accuracies = []

    def train_epoch(self, data: Data, train_mask: torch.Tensor) -> Tuple[float, float]:
        """
        Function used to train a simple epoch
        """
        self.model.train()
        self.optimizer.zero_grad()

        out = self.model(data.x, data.edge_index)
        loss = self.criterion(out[train_mask], data.y[train_mask])
        loss.backward()
        self.optimizer.step()

        # Calcular precisión
        pred = out[train_mask].argmax(dim=1)
        train_acc = (pred == data.y[train_mask]).float().mean()

        return loss.item(), train_acc.item()

    def evaluate(self, data: Data, mask: torch.Tensor) -> Tuple[float, float]:
        """
        Function used to evaluate the model. This evaluation is done over the validation set.
        """
        self.model.eval()
        with torch.no_grad():
            out = self.model(data.x, data.edge_index)
            loss = self.criterion(out[mask], data.y[mask])
            pred = out[mask].argmax(dim=1)
            acc = (pred == data.y[mask]).float().mean()

        return loss.item(), acc.item()

    def train(
        self, data: Data, train_mask: torch.Tensor, val_mask: torch.Tensor
    ) -> Dict:
        """
        Train the whole model.
        """
        best_val_acc = 0
        patience_counter = 0

        for epoch in range(EPOCHS):
            # Entrenar
            train_loss, train_acc = self.train_epoch(data, train_mask)

            # Validar
            val_loss, val_acc = self.evaluate(data, val_mask)

            # Guardar métricas
            self.train_losses.append(train_loss)
            self.val_losses.append(val_loss)
            self.train_accuracies.append(train_acc)
            self.val_accuracies.append(val_acc)

            # Early stopping
            if val_acc > best_val_acc:
                best_val_acc = val_acc
                patience_counter = 0
                torch.save(self.model.state_dict(), "models/best_fraud_model.pth")
            else:
                patience_counter += 1
                if patience_counter >= PATIENCE:
                    print(f"Early stopping en época {epoch+1}")
                    break

            # Log progreso
            if (epoch + 1) % 20 == 0:
                print(
                    f"Época {epoch+1:03d}, Train Loss: {train_loss:.4f}, "
                    f"Train Acc: {train_acc:.4f}, Val Loss: {val_loss:.4f}, "
                    f"Val Acc: {val_acc:.4f}"
                )

        return {
            "best_val_acc": best_val_acc,
            "train_losses": self.train_losses,
            "val_losses": self.val_losses,
            "train_accuracies": self.train_accuracies,
            "val_accuracies": self.val_accuracies,
        }


class HeterogeneousFraudTrainer:
    """
    Trainer class specialized in heterogeneous models.
    """

    def __init__(self, model, device: str = "cpu"):
        self.model = model.to(device)
        self.device = device
        self.optimizer = torch.optim.AdamW(
            model.parameters(), lr=LEARNING_RATE_ADV, weight_decay=WEIGHT_DECAY_ADV
        )

        # Loss function with weights for imbalanced classes
        class_weights = torch.tensor(
            [1.0, 5.0, 3.0], device=device
        )  # Normal, Fraude, Irregularidad
        self.criterion = nn.NLLLoss(weight=class_weights)

        # Scheduler for the learning rate
        self.scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            self.optimizer, mode="max", factor=0.5, patience=10
        )

        # Metrics that are going to be stored in the following lists:
        self.train_losses = []
        self.val_losses = []
        self.train_accuracies = []
        self.val_accuracies = []

    def train_epoch(self, data, train_mask):
        """
        Trains a simple epoch.
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

        return loss.item(), acc.item()

    def train(self, data, train_mask, val_mask, epochs=None) -> dict:
        """
        Train the whole model.
        """

        if epochs is None:
            epochs = EPOCHS_ADV

        best_val_acc = 0
        patience_counter = 0

        print(f"🎓 Iniciando entrenamiento por {epochs} épocas...")

        for epoch in range(epochs):
            # Entrenar
            train_loss, train_acc = self.train_epoch(data, train_mask)

            # Validar
            val_loss, val_acc = self.evaluate(data, val_mask)

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
                torch.save(
                    self.model.state_dict(), "models/best_hetero_fraud_model.pth"
                )
            else:
                patience_counter += 1
                if patience_counter >= PATIENCE_ADV:
                    print(f"Early stopping en época {epoch+1}")
                    break

            # Log progreso
            if (epoch + 1) % 20 == 0:
                print(
                    f"Época {epoch+1:03d}, Train Loss: {train_loss:.4f}, "
                    f"Train Acc: {train_acc:.4f}, Val Loss: {val_loss:.4f}, "
                    f"Val Acc: {val_acc:.4f}"
                )

        return {
            "best_val_acc": best_val_acc,
            "train_losses": self.train_losses,
            "val_losses": self.val_losses,
            "train_accuracies": self.train_accuracies,
            "val_accuracies": self.val_accuracies,
        }
