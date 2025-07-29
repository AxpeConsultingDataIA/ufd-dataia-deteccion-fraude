"""
Script in charge of generating the sagemaker training pipeline.
Este pipeline usará tanto los ficheros
feature/main.py como training_and_prediction.py en los
respectivos steps de cálculo de features y de
entrenamiento.
"""

# 1. Import necessary libraries
import boto3
import argparse
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.pipeline_context import PipelineSession
import logging

from functional_code import (
    FraudDetectionPipelineConfig,
    create_processing_step,
    create_training_step,
    create_evaluation_step,
    create_model_registration_step,
)

# 2. Define global variables
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# 3. Functions
def parse_args():
    """
    Parse the arguments that come from the command line
    """
    parser = argparse.ArgumentParser(description="Pipeline de detección de fraude")

    # Basic configuration
    parser.add_argument(
        "--pipeline-name",
        type=str,
        default="fraud-detection-hetero-graph",
        help="Nombre del pipeline",
    )
    parser.add_argument(
        "--role-arn", type=str, required=True, help="ARN del rol de SageMaker"
    )
    parser.add_argument(
        "--bucket", type=str, required=True, help="Bucket S3 para artefactos"
    )
    parser.add_argument("--region", type=str, default="us-east-1", help="Región AWS")

    # Neo4j params
    parser.add_argument("--neo4j-uri", type=str, required=True, help="URI de Neo4j")
    parser.add_argument(
        "--neo4j-username", type=str, required=True, help="Usuario de Neo4j"
    )
    parser.add_argument(
        "--neo4j-password", type=str, required=True, help="Contraseña de Neo4j"
    )

    # Model params
    parser.add_argument(
        "--hidden-dim", type=int, default=128, help="Dimensión oculta del modelo"
    )
    parser.add_argument("--num-layers", type=int, default=4, help="Número de capas")
    parser.add_argument(
        "--learning-rate", type=float, default=0.001, help="Tasa de aprendizaje"
    )
    parser.add_argument("--epochs", type=int, default=100, help="Número de épocas")

    # Infra params
    parser.add_argument(
        "--processing-instance-type",
        type=str,
        default="ml.m5.xlarge",
        help="Tipo de instancia para procesamiento",
    )
    parser.add_argument(
        "--training-instance-type",
        type=str,
        default="ml.g4dn.xlarge",
        help="Tipo de instancia para entrenamiento",
    )

    # Actions
    parser.add_argument(
        "--action",
        type=str,
        # create -> Creates the pipeline definition in sagemaker
        # execute -> Executes the pipeline with specific params
        # monitor -> Monitors an execution
        # delete -> deletes the pipeline
        choices=["create", "execute", "monitor", "delete"],
        default="create",
        help="Acción a realizar",
    )
    parser.add_argument(
        "--execution-arn", type=str, help="ARN de ejecución para monitoreo"
    )

    return parser.parse_args()


class FraudDetectionPipelineManager:
    """
    This class is incharge of the management of the whole pipeline
    """

    def __init__(self, config):
        self.config = config
        self.sagemaker_session = PipelineSession()
        self.sagemaker_client = boto3.client("sagemaker", region_name=config.region)

        # Configure s3 paths
        self.base_s3_path = f"s3://{config.bucket}/fraud-detection-pipeline"
        self.data_path = f"{self.base_s3_path}/data"
        self.code_path = f"{self.base_s3_path}/code"
        self.model_path = f"{self.base_s3_path}/models"

        logger.info(f"📁 Pipeline configured with bucket: {config.bucket}")
        logger.info(f"📁 Base path S3: {self.base_s3_path}")

    def create_pipeline(self):
        """
        Function that creates the complete pipeline for sagemaker
        """

        logger.info("🏗️ Creating the pipeline for SageMaker...")

        # Crear pasos del pipeline
        step_process = create_processing_step(self.config, self.sagemaker_session)
        step_train = create_training_step(
            self.config, self.sagemaker_session, step_process
        )
        step_eval = create_evaluation_step(
            self.config, self.sagemaker_session, step_train
        )
        step_register = create_model_registration_step(
            self.config, step_train, step_eval
        )

        # Create the pipeline
        pipeline = Pipeline(
            name=self.config.pipeline_name,
            parameters=[
                self.config.neo4j_uri,
                self.config.neo4j_username,
                self.config.neo4j_password,
                self.config.hidden_dim,
                self.config.num_layers,
                self.config.learning_rate,
                self.config.epochs,
                self.config.processing_instance_type,
                self.config.training_instance_type,
                self.config.accuracy_threshold,
            ],
            steps=[step_process, step_train, step_eval, step_register],
            sagemaker_session=self.sagemaker_session,
        )

        logger.info("✅ Pipeline created succesfully")
        return pipeline

    def execute_pipeline(self, pipeline, parameters=None):
        """
        Execute the pipeline
        """

        logger.info("🚀 Executing the pipeline...")

        # Create or update the pipeline
        pipeline.upsert(role_arn=self.config.role_arn)

        # Params by default
        if parameters is None:
            parameters = {
                "Neo4jUri": self.config.neo4j_uri.default_value,
                "Neo4jUsername": self.config.neo4j_username.default_value,
                "Neo4jPassword": self.config.neo4j_password.default_value,
                "HiddenDim": self.config.hidden_dim.default_value,
                "NumLayers": self.config.num_layers.default_value,
                "LearningRate": self.config.learning_rate.default_value,
                "Epochs": self.config.epochs.default_value,
            }

        # Execute the pipeline
        execution = pipeline.start(parameters=parameters)

        logger.info(f"✅ Pipeline executing: {execution.arn}")
        return execution

    def monitor_execution(self, execution_arn=None):
        """
        Monitor the pipeline execution
        """

        if execution_arn is None:
            # Get the most recent execution
            executions = self.sagemaker_client.list_pipeline_executions(
                PipelineName=self.config.pipeline_name, MaxResults=1
            )

            if not executions["PipelineExecutionSummaries"]:
                logger.error("❌ We couldn't find executions of the pipeline")
                return None

            execution_arn = executions["PipelineExecutionSummaries"][0][
                "PipelineExecutionArn"
            ]

        logger.info(f"🔍 Monitor the execution: {execution_arn}")

        # Get the status of the execution
        response = self.sagemaker_client.describe_pipeline_execution(
            PipelineExecutionArn=execution_arn
        )

        status = response["PipelineExecutionStatus"]
        logger.info(f"📊 Estado actual: {status}")

        # Get the step status
        steps_response = self.sagemaker_client.list_pipeline_execution_steps(
            PipelineExecutionArn=execution_arn
        )

        logger.info("📋 Step status:")
        for step in steps_response["PipelineExecutionSteps"]:
            step_name = step["StepName"]
            step_status = step["StepStatus"]

            # Add emojis in terms of the status
            if step_status == "Succeeded":
                emoji = "✅"
            elif step_status == "Failed":
                emoji = "❌"
            elif step_status == "Executing":
                emoji = "🔄"
            else:
                emoji = "⏳"

            logger.info(f"  {emoji} {step_name}: {step_status}")

            # Show details of the error if exists
            if step_status == "Failed" and "FailureReason" in step:
                logger.error(f"    Error: {step['FailureReason']}")

        return status

    def delete_pipeline(self):
        """
        Function in charge of deleting the pipeline
        """

        logger.info("🗑️ Deleting the pipeline...")

        try:
            self.sagemaker_client.delete_pipeline(
                PipelineName=self.config.pipeline_name
            )
            logger.info("✅ Pipeline deleted succesfully")
        except Exception as e:
            logger.error(f"❌ Error deleting the pipeline: {e}")

    def generate_execution_report(self, execution_arn=None):
        """
        Generate the execution report of the pipeline
        """

        if execution_arn is None:
            executions = self.sagemaker_client.list_pipeline_executions(
                PipelineName=self.config.pipeline_name, MaxResults=1
            )
            execution_arn = executions["PipelineExecutionSummaries"][0][
                "PipelineExecutionArn"
            ]

        logger.info("📊 Generating the execution report...")

        # Get info from the execution
        execution_info = self.sagemaker_client.describe_pipeline_execution(
            PipelineExecutionArn=execution_arn
        )

        steps_info = self.sagemaker_client.list_pipeline_execution_steps(
            PipelineExecutionArn=execution_arn
        )

        report = {
            "pipeline_name": self.config.pipeline_name,
            "execution_arn": execution_arn,
            "status": execution_info["PipelineExecutionStatus"],
            "start_time": (
                execution_info.get("CreationTime", "").isoformat()
                if execution_info.get("CreationTime")
                else None
            ),
            "end_time": (
                execution_info.get("LastModifiedTime", "").isoformat()
                if execution_info.get("LastModifiedTime")
                else None
            ),
            "steps": [],
            "summary": {
                "total_steps": len(steps_info["PipelineExecutionSteps"]),
                "succeeded_steps": 0,
                "failed_steps": 0,
                "executing_steps": 0,
            },
        }

        for step in steps_info["PipelineExecutionSteps"]:
            step_info = {
                "name": step["StepName"],
                "status": step["StepStatus"],
                "start_time": (
                    step.get("StartTime", "").isoformat()
                    if step.get("StartTime")
                    else None
                ),
                "end_time": (
                    step.get("EndTime", "").isoformat() if step.get("EndTime") else None
                ),
            }

            # Update contadores
            if step["StepStatus"] == "Succeeded":
                report["summary"]["succeeded_steps"] += 1
            elif step["StepStatus"] == "Failed":
                report["summary"]["failed_steps"] += 1
                step_info["failure_reason"] = step.get("FailureReason", "Unknown error")
            elif step["StepStatus"] == "Executing":
                report["summary"]["executing_steps"] += 1

            report["steps"].append(step_info)

        logger.info("✅ Report generated succesfully")
        return report


def main():
    """
    Main script function
    """

    args = parse_args()

    # Create the configuration
    config = FraudDetectionPipelineConfig(
        pipeline_name=args.pipeline_name,
        role_arn=args.role_arn,
        bucket=args.bucket,
        region=args.region,
        neo4j_uri=args.neo4j_uri,
        neo4j_username=args.neo4j_username,
        neo4j_password=args.neo4j_password,
        hidden_dim=args.hidden_dim,
        num_layers=args.num_layers,
        learning_rate=args.learning_rate,
        epochs=args.epochs,
        processing_instance_type=args.processing_instance_type,
        training_instance_type=args.training_instance_type,
    )

    # Create manager
    pipeline_manager = FraudDetectionPipelineManager(config)

    try:
        if args.action == "create":
            # Create the pipeline
            pipeline = pipeline_manager.create_pipeline()
            logger.info(f"✅ Pipeline '{args.pipeline_name}' creado exitosamente")

        elif args.action == "execute":
            # Create and execute the pipeline
            pipeline = pipeline_manager.create_pipeline()
            execution = pipeline_manager.execute_pipeline(pipeline)
            logger.info(f"🚀 Pipeline executing: {execution.arn}")

        elif args.action == "monitor":
            # Monitor the execution
            status = pipeline_manager.monitor_execution(args.execution_arn)
            if status:
                logger.info(f"📊 Pipeline status: {status}")

                # Generate the report
                if status in ["Succeeded", "Failed", "Stopped"]:
                    report = pipeline_manager.generate_execution_report(
                        args.execution_arn
                    )
                    logger.info("📋 Execution report:")
                    logger.info(f"  Total steps: {report['summary']['total_steps']}")
                    logger.info(f"  Succeded: {report['summary']['succeeded_steps']}")
                    logger.info(f"  Failed: {report['summary']['failed_steps']}")

        elif args.action == "delete":
            # Delete the pipeline
            pipeline_manager.delete_pipeline()

    except Exception as e:
        logger.error(f"❌ Error launching the action '{args.action}': {e}")
        raise


if __name__ == "__main__":
    main()
