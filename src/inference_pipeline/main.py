"""
Script in order to generate the sagemaker batch inference pipeline.
This pipeline will have a parameter in order to store the prediction
in the datalake or a given bucket of s3.
"""

# 1. Import necessary libraries
import boto3
import sagemaker
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.workflow.parameters import ParameterString, ParameterInteger
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.transformer import Transformer
from sagemaker.model import Model
import json
import os
import logging
from datetime import datetime
import argparse

# Importar código funcional
from functional_code import (
    InferencePipelineConfig,
    create_data_preparation_step,
    create_batch_transform_step,
    create_results_processing_step,
    upload_inference_data_to_s3,
)

# 2. Define global variables
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# 3. Functions/Classes
def parse_args():
    """
    Parse the arguments from the command line
    """

    parser = argparse.ArgumentParser(
        description="Pipeline de inferencia batch para detección de fraude"
    )

    # Basic configuration
    parser.add_argument(
        "--pipeline-name",
        type=str,
        default="fraud-detection-inference-batch",
        help="Nombre del pipeline de inferencia",
    )
    parser.add_argument(
        "--role-arn", type=str, required=True, help="ARN del rol de SageMaker"
    )
    parser.add_argument(
        "--bucket", type=str, required=True, help="Bucket S3 para artefactos"
    )
    parser.add_argument("--region", type=str, default="us-east-1", help="Región AWS")

    # Model configuration
    parser.add_argument(
        "--model-package-arn", type=str, required=True, help="ARN del modelo registrado"
    )
    parser.add_argument(
        "--model-name", type=str, help="Nombre del modelo (si no se usa package)"
    )

    # Data configuration
    parser.add_argument(
        "--input-data-path",
        type=str,
        required=True,
        help="Ruta S3 con datos de entrada",
    )
    parser.add_argument(
        "--output-data-path", type=str, required=True, help="Ruta S3 para resultados"
    )
    parser.add_argument(
        "--datalake-path", type=str, help="Ruta del datalake para entorno productivo"
    )

    # Neo4j params for real time data
    parser.add_argument(
        "--neo4j-uri", type=str, help="URI de Neo4j para datos adicionales"
    )
    parser.add_argument("--neo4j-username", type=str, help="Usuario de Neo4j")
    parser.add_argument("--neo4j-password", type=str, help="Contraseña de Neo4j")

    # Infra configuration
    parser.add_argument(
        "--processing-instance-type",
        type=str,
        default="ml.m5.xlarge",
        help="Tipo de instancia para procesamiento",
    )
    parser.add_argument(
        "--transform-instance-type",
        type=str,
        default="ml.m5.large",
        help="Tipo de instancia para transformación",
    )
    parser.add_argument(
        "--transform-instance-count",
        type=int,
        default=1,
        help="Número de instancias para transformación",
    )

    # Environment configuration
    parser.add_argument(
        "--environment",
        type=str,
        choices=["test", "prod"],
        default="test",
        help="Entorno de despliegue",
    )

    # Actions
    parser.add_argument(
        "--action",
        type=str,
        choices=["create", "execute", "monitor", "delete"],
        default="create",
        help="Acción a realizar",
    )
    parser.add_argument(
        "--execution-arn", type=str, help="ARN de ejecución para monitoreo"
    )

    return parser.parse_args()


class InferencePipelineManager:
    """
    Batch inference pipeline manager
    """

    def __init__(self, config):
        self.config = config
        self.sagemaker_session = PipelineSession()
        self.sagemaker_client = boto3.client("sagemaker", region_name=config.region)

        # Configure s3 paths
        self.base_s3_path = f"s3://{config.bucket}/fraud-inference-pipeline"
        self.code_path = f"{self.base_s3_path}/code"
        self.temp_path = f"{self.base_s3_path}/temp"

        logger.info(f"📁 Inference pipeline configured")
        logger.info(f"📁 Bucket: {config.bucket}")
        logger.info(f"📁 Environment: {config.environment}")

    def create_pipeline(self):
        """
        This function creates the batch inference pipeline
        """

        logger.info("🏗️ Creating the batch inference pipeline...")

        # Create the steps of the pipeline
        step_prepare = create_data_preparation_step(self.config, self.sagemaker_session)
        step_transform = create_batch_transform_step(
            self.config, self.sagemaker_session, step_prepare
        )
        step_process_results = create_results_processing_step(
            self.config, self.sagemaker_session, step_transform
        )

        # Create the pipeline
        pipeline = Pipeline(
            name=self.config.pipeline_name,
            parameters=[
                self.config.input_data_path,
                self.config.output_data_path,
                self.config.processing_instance_type,
                self.config.transform_instance_type,
                self.config.transform_instance_count,
                self.config.environment_param,
            ],
            steps=[step_prepare, step_transform, step_process_results],
            sagemaker_session=self.sagemaker_session,
        )

        logger.info("✅ Inference pipeline created succesfully")
        return pipeline

    def execute_pipeline(self, pipeline, parameters=None):
        """
        function in charge of executing the inference pipeline
        """

        logger.info("🚀 Executing the inference pipeline...")

        # Create or update the pipeline
        pipeline.upsert(role_arn=self.config.role_arn)

        # By default params
        if parameters is None:
            parameters = {
                "InputDataPath": self.config.input_data_path.default_value,
                "OutputDataPath": self.config.output_data_path.default_value,
                "ProcessingInstanceType": self.config.processing_instance_type.default_value,
                "TransformInstanceType": self.config.transform_instance_type.default_value,
                "TransformInstanceCount": self.config.transform_instance_count.default_value,
                "Environment": self.config.environment_param.default_value,
            }

        # Execute the pipeline
        execution = pipeline.start(parameters=parameters)

        logger.info(f"✅ Executing the inference pipeline: {execution.arn}")
        return execution

    def monitor_execution(self, execution_arn=None):
        """
        Monitor the inference pipeline execution
        """

        if execution_arn is None:
            # Get the latest execution
            executions = self.sagemaker_client.list_pipeline_executions(
                PipelineName=self.config.pipeline_name, MaxResults=1
            )

            if not executions["PipelineExecutionSummaries"]:
                logger.error("❌ We couldn't find pipeline executions")
                return None

            execution_arn = executions["PipelineExecutionSummaries"][0][
                "PipelineExecutionArn"
            ]

        logger.info(f"🔍 Monitoring the inference execution: {execution_arn}")

        # Get the status of the execution
        response = self.sagemaker_client.describe_pipeline_execution(
            PipelineExecutionArn=execution_arn
        )

        status = response["PipelineExecutionStatus"]
        logger.info(f"📊 Actual status: {status}")

        # Get the step status
        steps_response = self.sagemaker_client.list_pipeline_execution_steps(
            PipelineExecutionArn=execution_arn
        )

        logger.info("📋 Inference steps status:")
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

            # MShow error details in case it exists
            if step_status == "Failed" and "FailureReason" in step:
                logger.error(f"    Error: {step['FailureReason']}")

        return status

    def get_inference_results(self, execution_arn=None):
        """
        Get the inference results
        """

        logger.info("📊 Getting the inference results...")

        # Set the output path in terms of the envirnonment
        if self.config.environment == "prod" and self.config.datalake_path:
            output_path = self.config.datalake_path
        else:
            output_path = self.config.output_data_path.default_value

        logger.info(f"📁 Available results in: {output_path}")

        return {
            "output_path": output_path,
            "execution_arn": execution_arn,
            "status": "completed",
        }


def main():
    """
    Main function of the inference script
    """

    args = parse_args()

    # Create config
    config = InferencePipelineConfig(
        pipeline_name=args.pipeline_name,
        role_arn=args.role_arn,
        bucket=args.bucket,
        region=args.region,
        model_package_arn=args.model_package_arn,
        model_name=args.model_name,
        input_data_path=args.input_data_path,
        output_data_path=args.output_data_path,
        datalake_path=args.datalake_path,
        neo4j_uri=args.neo4j_uri,
        neo4j_username=args.neo4j_username,
        neo4j_password=args.neo4j_password,
        processing_instance_type=args.processing_instance_type,
        transform_instance_type=args.transform_instance_type,
        transform_instance_count=args.transform_instance_count,
        environment=args.environment,
    )

    # Create the manager
    pipeline_manager = InferencePipelineManager(config)

    try:
        if args.action == "create":
            # Create the pipeline
            pipeline = pipeline_manager.create_pipeline()
            logger.info(
                f"✅ Inference pipeline '{args.pipeline_name}' created succeesfully"
            )

        elif args.action == "execute":
            # Create and execute the pipeline
            pipeline = pipeline_manager.create_pipeline()
            execution = pipeline_manager.execute_pipeline(pipeline)
            logger.info(f"🚀 Executing the inference pipeline: {execution.arn}")

        elif args.action == "monitor":
            # Monitor the execution
            status = pipeline_manager.monitor_execution(args.execution_arn)
            if status:
                logger.info(f"📊 Pipeline status: {status}")

                # Get the results if it is completed
                if status == "Succeeded":
                    results = pipeline_manager.get_inference_results(args.execution_arn)
                    logger.info(f"📊 Results available in: {results['output_path']}")

        elif args.action == "delete":
            # Delete pipeline
            try:
                pipeline_manager.sagemaker_client.delete_pipeline(
                    PipelineName=args.pipeline_name
                )
                logger.info("✅ Inference pipeline deleted succeesfully")
            except Exception as e:
                logger.error(f"❌ Error deleting the pipeline: {e}")

    except Exception as e:
        logger.error(f"❌ Error executing the action '{args.action}': {e}")
        raise


if __name__ == "__main__":
    main()
