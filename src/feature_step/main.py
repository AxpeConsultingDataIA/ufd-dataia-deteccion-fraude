"""
Script para lectura de datos y generación de features.
Este se ejecutará en su propio contenedor dentro de las
plataformas de entrenamiento e inferencia.
El código incluido en este script es puramente técnico y
no necesita ser cambiado.
"""

# 1. Import necessary libraries
import argparse
import os
import pandas as pd
import numpy as np
import torch
import json
import logging
from datetime import datetime

from functional_code import (
    Neo4jDataLoader,
    HeterogeneousGraphProcessorSageMaker,
)

# 2. Define global variables
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRAUD_CLASSES = {"NORMAL": 0, "FRAUDE": 1, "IRREGULARIDAD": 2}


# 3. Functions
def parse_args():
    """
    Parse arguments of SageMaker
    """
    parser = argparse.ArgumentParser()

    # Connection params for SageMaker
    parser.add_argument(
        "--neo4j-uri", type=str, required=True, help="URI de conexión a Neo4j"
    )
    parser.add_argument(
        "--neo4j-username", type=str, required=True, help="Usuario de Neo4j"
    )
    parser.add_argument(
        "--neo4j-password", type=str, required=True, help="Contraseña de Neo4j"
    )

    # Processing params
    parser.add_argument(
        "--train-split",
        type=float,
        default=0.6,
        help="Proporción de datos para entrenamiento",
    )
    parser.add_argument(
        "--val-split",
        type=float,
        default=0.2,
        help="Proporción de datos para validación",
    )

    # SageMaker paths
    parser.add_argument(
        "--output-data-dir",
        type=str,
        default=os.environ.get("SM_OUTPUT_DATA_DIR", "/opt/ml/processing"),
    )

    return parser.parse_args()


def create_output_directories(base_path):
    """
    Creates output directories
    """
    directories = {
        "train": os.path.join(base_path, "train"),
        "validation": os.path.join(base_path, "validation"),
        "test": os.path.join(base_path, "test"),
        "report": os.path.join(base_path, "report"),
    }

    for dir_path in directories.values():
        os.makedirs(dir_path, exist_ok=True)

    return directories


def generate_data_report(
    raw_data, train_indices, val_indices, test_indices, output_path
):
    """
    Generates a report of the processed data.
    """

    report = {
        "processing_timestamp": datetime.now().isoformat(),
        "data_summary": {
            "total_nodes": sum(len(nodes) for nodes in raw_data["nodes"].values()),
            "total_edges": sum(len(edges) for edges in raw_data["edges"].values()),
            "node_types": list(raw_data["nodes"].keys()),
            "edge_types": list(raw_data["edges"].keys()),
        },
        "split_summary": {
            "train_size": len(train_indices),
            "validation_size": len(val_indices),
            "test_size": len(test_indices),
            "train_percentage": len(train_indices)
            / (len(train_indices) + len(val_indices) + len(test_indices))
            * 100,
            "validation_percentage": len(val_indices)
            / (len(train_indices) + len(val_indices) + len(test_indices))
            * 100,
            "test_percentage": len(test_indices)
            / (len(train_indices) + len(val_indices) + len(test_indices))
            * 100,
        },
        "fraud_distribution": {},
    }

    # Analyze the fraud distribution if exists
    if (
        "contador" in raw_data["nodes"]
        and "label" in raw_data["nodes"]["contador"].columns
    ):
        label_counts = raw_data["nodes"]["contador"]["label"].value_counts()
        report["fraud_distribution"] = label_counts.to_dict()

    # Info by node type
    for node_type, node_df in raw_data["nodes"].items():
        report[f"{node_type}_features"] = {
            "count": len(node_df),
            "columns": list(node_df.columns),
            "feature_count": len(node_df.columns),
        }

    # Save the report
    with open(os.path.join(output_path, "data_report.json"), "w") as f:
        json.dump(report, f, indent=2)

    logger.info("📊 Reporte de datos generado")
    return report


def main():
    """
    Main function for the features processing step.
    """

    logger.info("🚀 Starting the features processing from Neo4j...")

    args = parse_args()

    # First of all we create the output dirs
    dirs = create_output_directories(args.output_data_dir)

    try:
        # 1. Connection to Neo4j
        logger.info("🔌 Connecting to Neo4j...")
        loader = Neo4jDataLoader(
            args.neo4j_uri, args.neo4j_username, args.neo4j_password
        )

        raw_data = loader.load_heterogeneous_data()
        loader.close()

        logger.info(f"✅ Data loaded from Neo4j")

        # 2. Process the format to PyTorch Geometric
        logger.info("🔄 Processing the data to PyTorch Geometric...")
        processor = HeterogeneousGraphProcessorSageMaker()
        hetero_data = processor.create_heterogeneous_graph(raw_data)

        # 3. Split the data
        logger.info("📊 Splitting the data into train/validation/test...")
        num_contadores = hetero_data["contador"].x.size(0)
        indices = torch.randperm(num_contadores)

        train_size = int(args.train_split * num_contadores)
        val_size = int(args.val_split * num_contadores)

        train_indices = indices[:train_size]
        val_indices = indices[train_size : train_size + val_size]
        test_indices = indices[train_size + val_size :]

        # 4. Guardar datasets
        logger.info("💾 Saving processed datasets...")

        # Training dataset
        torch.save(
            {
                "hetero_data": hetero_data,
                "indices": train_indices,
                "raw_data": raw_data,
                "metadata": {
                    "split_type": "train",
                    "node_feature_dims": {
                        nt: hetero_data[nt].x.size(1) for nt in hetero_data.node_types
                    },
                    "edge_types": hetero_data.edge_types,
                },
            },
            os.path.join(dirs["train"], "train_data.pt"),
        )

        # Validation dataset
        torch.save(
            {
                "hetero_data": hetero_data,
                "indices": val_indices,
                "raw_data": raw_data,
                "metadata": {
                    "split_type": "validation",
                    "node_feature_dims": {
                        nt: hetero_data[nt].x.size(1) for nt in hetero_data.node_types
                    },
                    "edge_types": hetero_data.edge_types,
                },
            },
            os.path.join(dirs["validation"], "validation_data.pt"),
        )

        # Test dataset
        torch.save(
            {
                "hetero_data": hetero_data,
                "indices": test_indices,
                "raw_data": raw_data,
                "metadata": {
                    "split_type": "test",
                    "node_feature_dims": {
                        nt: hetero_data[nt].x.size(1) for nt in hetero_data.node_types
                    },
                    "edge_types": hetero_data.edge_types,
                },
            },
            os.path.join(dirs["test"], "test_data.pt"),
        )

        # 5. Save artefacts from processing
        logger.info("🔧 Saving artefacts from processing...")
        processor.save_preprocessing_artifacts(dirs["train"])

        # 6. Generate the report
        logger.info("📋 Generating the report...")
        report = generate_data_report(
            raw_data, train_indices, val_indices, test_indices, dirs["report"]
        )

        logger.info("✅ Features processing done")
        logger.info(f"   - Training: {len(train_indices)} data")
        logger.info(f"   - Validation: {len(val_indices)} data")
        logger.info(f"   - Test: {len(test_indices)} data")

    except Exception as e:
        logger.error(f"❌ Error when processing the features: {e}")
        raise


if __name__ == "__main__":
    main()
