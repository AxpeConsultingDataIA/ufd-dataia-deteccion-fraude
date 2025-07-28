"""
Script used fot the model training. The script performs the training
in the main method and then uses the functions model_fn and predict_fn
for the inference.
"""

# 1. Import necessary libraries
import argparse
import os
import json
import torch
import torch.nn as nn
import torch.nn.functional as F
import pandas as pd
import numpy as np
from sklearn.metrics import classification_report, roc_auc_score, f1_score
import logging
import pickle
from datetime import datetime

# Importar código funcional
from functional_code import (
    HeterogeneousFraudGNNSageMaker,
    HeterogeneousFraudTrainerSageMaker,
    HeterogeneousEvaluator,
    FRAUD_CLASSES,
    NODE_DIMS_ADV,
)

# 2. Define global variables
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# 3. Functions and classes
def parse_args():
    """
    Parse SageMaker arguments.
    """
    parser = argparse.ArgumentParser()

    # Model hyperparams
    parser.add_argument("--hidden-dim", type=int, default=128)
    parser.add_argument("--num-layers", type=int, default=4)
    parser.add_argument("--learning-rate", type=float, default=0.001)
    parser.add_argument("--epochs", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--dropout", type=float, default=0.3)
    parser.add_argument("--weight-decay", type=float, default=0.01)
    parser.add_argument("--patience", type=int, default=15)

    # Sagemaker paths
    parser.add_argument("--model-dir", type=str, default=os.environ.get("SM_MODEL_DIR"))
    parser.add_argument("--train", type=str, default=os.environ.get("SM_CHANNEL_TRAIN"))
    parser.add_argument(
        "--validation", type=str, default=os.environ.get("SM_CHANNEL_VALIDATION")
    )
    parser.add_argument("--test", type=str, default=os.environ.get("SM_CHANNEL_TEST"))

    # Additional params
    parser.add_argument("--debug", action="store_true", help="Modo debug")
    parser.add_argument(
        "--local-mode", action="store_true", help="Modo local para desarrollo"
    )

    return parser.parse_args()


def load_data(train_path, val_path, test_path):
    """
    Load training, validation and test data
    """

    logger.info("📂 Loading datasets...")

    train_data = torch.load(os.path.join(train_path, "train_data.pt"))
    val_data = torch.load(os.path.join(val_path, "validation_data.pt"))
    test_data = torch.load(os.path.join(test_path, "test_data.pt"))

    logger.info("✅ Datasets loaded")

    return train_data, val_data, test_data


def create_masks(hetero_data, train_indices, val_indices, test_indices, device):
    """
    Creates masks for training, validation and test.
    """

    num_contadores = hetero_data["contador"].x.size(0)

    train_mask = torch.zeros(num_contadores, dtype=torch.bool, device=device)
    val_mask = torch.zeros(num_contadores, dtype=torch.bool, device=device)
    test_mask = torch.zeros(num_contadores, dtype=torch.bool, device=device)

    # Boolean value == True in order to know where the train, val and test data is:
    train_mask[train_indices] = True
    val_mask[val_indices] = True
    test_mask[test_indices] = True

    return train_mask, val_mask, test_mask


def update_node_dimensions(hetero_data):
    """
    Update node's dimension based on real data.
    """

    for node_type in hetero_data.node_types:
        actual_dim = hetero_data[node_type].x.size(1)
        NODE_DIMS_ADV[node_type] = actual_dim
        logger.info(f"   {node_type}: {actual_dim} características")


def save_model_artifacts(model, model_dir, metrics, metadata):
    """
    Save the model and necessary artefacts for SageMaker.
    """
    # We create the model_dir in case it does not exist
    os.makedirs(model_dir, exist_ok=True)

    # Save the model's status
    torch.save(model.state_dict(), os.path.join(model_dir, "model.pth"))

    # Save the model's metadata
    model_info = {
        "model_type": "HeterogeneousFraudGNN",
        "framework": "pytorch",
        "framework_version": "1.12.0",
        "hidden_dim": model.hidden_dim,
        "num_classes": model.num_classes,
        "num_layers": model.num_layers,
        "dropout": model.dropout,
        "node_types": list(metadata[0]),
        "edge_types": [list(et) for et in metadata[1]],
        "training_timestamp": datetime.now().isoformat(),
        "fraud_classes": FRAUD_CLASSES,
    }

    with open(os.path.join(model_dir, "model_info.json"), "w") as f:
        json.dump(model_info, f, indent=2)

    # Guardar métricas finales
    with open(os.path.join(model_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    # Guardar dimensiones de nodos para inferencia
    with open(os.path.join(model_dir, "node_dimensions.json"), "w") as f:
        json.dump(NODE_DIMS_ADV, f, indent=2)

    logger.info("💾 Artefacts of the model saved")


def evaluate_model_performance(model, hetero_data, test_mask, device):
    """
    Detailed evaluation of the model in the test set.
    """

    logger.info("📊 evaluating the model in the test set...")

    model.eval()
    with torch.no_grad():
        out = model(hetero_data.x_dict, hetero_data.edge_index_dict)
        contador_labels = hetero_data["contador"].y

        # Predicciones
        pred = out[test_mask].argmax(dim=1)
        y_true = contador_labels[test_mask].cpu().numpy()
        y_pred = pred.cpu().numpy()
        y_prob = torch.softmax(out[test_mask], dim=1).cpu().numpy()

        # Calcular métricas
        test_acc = (pred == contador_labels[test_mask]).float().mean().item()
        test_f1 = f1_score(y_true, y_pred, average="weighted")

        # ROC AUC para clasificación multiclase
        try:
            test_roc_auc = roc_auc_score(y_true, y_prob, multi_class="ovr")
        except Exception as e:
            logger.warning(f"We couldn't calculate the ROC AUC value: {e}")
            test_roc_auc = 0.0

        # Classification report:
        class_names = ["Normal", "Fraude", "Irregularidad"]
        classification_rep = classification_report(
            y_true, y_pred, target_names=class_names, output_dict=True
        )

        # Log metrics for SageMaker:
        print(f"Test Accuracy: {test_acc:.4f}")
        print(f"Test F1: {test_f1:.4f}")
        print(f"Test ROC AUC: {test_roc_auc:.4f}")

        # Show the final report:
        logger.info("📋 Classification report:")
        logger.info(f"   Accuracy: {test_acc:.4f}")
        logger.info(f"   F1 Score (weighted): {test_f1:.4f}")
        logger.info(f"   ROC AUC: {test_roc_auc:.4f}")

        for class_name, metrics in classification_rep.items():
            if isinstance(metrics, dict) and class_name in class_names:
                logger.info(f"   {class_name}:")
                logger.info(f"     Precision: {metrics['precision']:.4f}")
                logger.info(f"     Recall: {metrics['recall']:.4f}")
                logger.info(f"     F1-Score: {metrics['f1-score']:.4f}")

    return {
        "test_accuracy": test_acc,
        "test_f1": test_f1,
        "test_roc_auc": test_roc_auc,
        "classification_report": classification_rep,
        "predictions": y_pred.tolist(),
        "true_labels": y_true.tolist(),
        "probabilities": y_prob.tolist(),
    }


def main():
    """
    Main training function
    """

    logger.info("🚀 Starting the GNN heterogeneous model training...")

    # Parse the args
    args = parse_args()

    # Configure the device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info(f"🔧 Device using: {device}")

    try:
        # Load train, test and val data
        train_data, val_data, test_data = load_data(
            args.train, args.validation, args.test
        )

        # Get the components
        hetero_data = train_data["hetero_data"].to(device)
        train_indices = train_data["indices"]
        val_indices = val_data["indices"]
        test_indices = test_data["indices"]

        # Create masks
        train_mask, val_mask, test_mask = create_masks(
            hetero_data, train_indices, val_indices, test_indices, device
        )

        # Update node's dimensions
        logger.info("🔧 Updating nodes dimensions:")
        update_node_dimensions(hetero_data)

        # Create the model
        logger.info("🧠 Creating GNN heterogeneous model...")
        model = HeterogeneousFraudGNNSageMaker(
            metadata=hetero_data.metadata(),
            hidden_dim=args.hidden_dim,
            num_classes=len(FRAUD_CLASSES),
            num_layers=args.num_layers,
            dropout=args.dropout,
        )

        total_params = sum(p.numel() for p in model.parameters())
        logger.info(f"📊 Params of the model: {total_params:,}")

        # Train the model
        logger.info("🎓 Starting the training phase...")
        trainer = HeterogeneousFraudTrainerSageMaker(
            model, device, args.learning_rate, args.weight_decay
        )

        history = trainer.train(
            hetero_data,
            train_mask,
            val_mask,
            epochs=args.epochs,
            patience=args.patience,
        )

        # Load the best model
        best_model_path = os.path.join(args.model_dir, "best_model.pth")
        if os.path.exists(best_model_path):
            model.load_state_dict(torch.load(best_model_path, map_location=device))
            logger.info("✅ Best model loaded from checkpoint")

        # Final evaluation
        evaluator = HeterogeneousEvaluator(model, device)
        test_results = evaluator.evaluate_detailed(hetero_data, test_mask)

        # Combine metrics
        final_metrics = {
            **history,
            **test_results,
            "model_parameters": total_params,
            "training_args": vars(args),
        }

        # Save the artefacts
        save_model_artifacts(
            model, args.model_dir, final_metrics, hetero_data.metadata()
        )

        logger.info("🎉 Training completed!")
        logger.info(f"   Best accuracy validation: {history['best_val_acc']:.4f}")
        logger.info(f"   Test accuracy: {test_results['test_accuracy']:.4f}")
        logger.info(f"   Test F1 score: {test_results['test_f1']:.4f}")

    except Exception as e:
        logger.error(f"❌ Error during the training phase: {e}")
        raise


# Now we will define the inference functions for sagemaker:
def model_fn(model_dir):
    """
    Load the model for inference in SageMaker
    """

    logger.info(f"📂 Loading the model from: {model_dir}")

    try:
        # Load model's information
        with open(os.path.join(model_dir, "model_info.json"), "r") as f:
            model_info = json.load(f)

        # Load node's dimensions
        with open(os.path.join(model_dir, "node_dimensions.json"), "r") as f:
            node_dims = json.load(f)

        # update global dimensions
        NODE_DIMS_ADV.update(node_dims)

        # Configure the device:
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Create the model
        ######################## CLAUDE ######################
        # Nota: En un escenario real, necesitarías recrear la metadata
        # Aquí usamos una metadata simplificada
        ######################## CLAUDE ######################
        mock_metadata = (
            model_info["node_types"],
            [tuple(et) for et in model_info["edge_types"]],
        )

        model = HeterogeneousFraudGNNSageMaker(
            metadata=mock_metadata,
            hidden_dim=model_info["hidden_dim"],
            num_classes=model_info["num_classes"],
            num_layers=model_info["num_layers"],
            dropout=model_info["dropout"],
        )

        # Load model's weights
        model_state = torch.load(
            os.path.join(model_dir, "model.pth"), map_location=device
        )
        model.load_state_dict(model_state)
        model.eval()

        logger.info("✅ Model loaded for inference")
        return model.to(device)

    except Exception as e:
        logger.error(f"❌ Error when loading the model: {e}")
        raise


def input_fn(request_body, content_type):
    """
    Process the input for inference
    """

    logger.info(f"📥 Processing the input of type: {content_type}")

    if content_type == "application/json":
        try:
            input_data = json.loads(request_body)
            logger.info("🔄 Transforming data into HeteroData format...")

            if "nodes" not in input_data or "edges" not in input_data:
                raise ValueError(
                    "Input data must be a dictionary containing keys 'nodes' and 'edges'"
                )

            processed_data = {"nodes": {}, "edges": input_data.get("edges", {})}

            for node_type, node_list in input_data["nodes"].items():
                if isinstance(node_list, list) and len(node_list) > 0:
                    # Convert the node_list into a data frame
                    processed_data["nodes"][node_type] = pd.DataFrame(node_list)
                    logger.info(f"   📊 {node_type}: {len(node_list)} nodos")
                else:
                    processed_data["nodes"][node_type] = pd.DataFrame()

            # We use our HeterogeneousGraphProcessor in order to create the graph
            processor = HeterogeneousGraphProcessorSageMaker()
            hetero_data = processor.create_heterogeneous_graph(processed_data)

            logger.info("✅ Input processed")

            return {
                "hetero_data": hetero_data,
                "processor": processor,  # Save the processor for a possible future use
            }

        except Exception as e:
            logger.error(f"❌ Error processing the input: {e}")
            raise ValueError(f"Error procesing the input from the JSON: {e}")

    else:
        raise ValueError(f"❌ Content type not soported: {content_type}")


def predict_fn(input_data, model):
    """
    Make the prediction using the loaded model.

    Parameters:
        - input_data: Processed data by input_fn
        - model: Model returned by model_fn
    """

    logger.info("🔮 Making the predictions...")

    try:
        device = next(model.parameters()).device

        # Get the HeteroData processed
        hetero_data = input_data["hetero_data"].to(device)

        # Make the preds
        model.eval()
        with torch.no_grad():
            # Forward pass of the model
            output = model(hetero_data.x_dict, hetero_data.edge_index_dict)

            # Obtain preds and probabilities
            probabilities = torch.softmax(output, dim=1)
            predictions = output.argmax(dim=1)

            # Transform into lists for the json serialization
            pred_list = predictions.cpu().numpy().tolist()
            prob_list = probabilities.cpu().numpy().tolist()

            # Map the predictions and labels
            class_names = {0: "NORMAL", 1: "FRAUDE", 2: "IRREGULARIDAD"}
            predicted_labels = [class_names[pred] for pred in pred_list]

            logger.info("✅ Prediction completed")
            logger.info(f"   📊 Predictions: {len(pred_list)} nodes processed")

            return {
                "predictions": pred_list,
                "predicted_labels": predicted_labels,
                "probabilities": prob_list,
                "fraud_scores": [
                    prob[1] + prob[2] for prob in prob_list
                ],  # Fraude + Irregularidad
                "model_version": "1.0",
                "num_nodes_processed": len(pred_list),
            }

    except Exception as e:
        logger.error(f"❌ Error during the predictions: {e}")
        raise


def output_fn(prediction, accept):
    """
    Format the output of the prediction.

    Parameters:
        - prediction: The resultant prediction returned by predict_fn
        - accept: expected MIME type for the response
    """

    if accept == "application/json":
        return json.dumps(prediction), accept

    raise ValueError(f"❌ Type of output not supported: {accept}")


if __name__ == "__main__":
    main()

# ===============================
# src/train_step/model_fn_predict_fn.py
# ===============================


# ===============================
# src/train_step/train_entrypoint.py
# ===============================

"""
Fichero con las librerías necesarias para dicho script
"""

# Este archivo define las dependencias y configuraciones necesarias

import sys
import os

# Agregar rutas necesarias al PATH
sys.path.append("/opt/ml/code")
sys.path.append("/opt/ml/processing/code")

# Configuraciones de entorno
os.environ["PYTHONPATH"] = "/opt/ml/code:/opt/ml/processing/code"

# Configuraciones para PyTorch Geometric
os.environ["TORCH_GEOMETRIC_FORCE_CPU"] = "0"  # Permitir GPU si está disponible

# Importar y ejecutar el script principal
if __name__ == "__main__":
    from training_and_prediction import main

    main()
