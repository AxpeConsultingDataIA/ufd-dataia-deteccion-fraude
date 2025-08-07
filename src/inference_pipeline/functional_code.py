"""
Functional code that supports the main inference pipeline
code
"""

# 1. Import necessary libraries
import boto3
from sagemaker.workflow.steps import ProcessingStep, TransformStep
from sagemaker.workflow.parameters import ParameterString, ParameterInteger
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.transformer import Transformer
from sagemaker.model import Model
from sagemaker.pytorch import PyTorchModel
import logging
from datetime import datetime

# 2. Define global variables
logger = logging.getLogger(__name__)


# 3. Class/functions
class InferencePipelineConfig:
    """
    Batch inference pipeline configuration
    """

    def __init__(
        self,
        pipeline_name,
        role_arn,
        bucket,
        region,
        model_package_arn,
        input_data_path,
        output_data_path,
        model_name=None,
        datalake_path=None,
        neo4j_uri=None,
        neo4j_username=None,
        neo4j_password=None,
        processing_instance_type="ml.m5.xlarge",
        transform_instance_type="ml.m5.large",
        transform_instance_count=1,
        environment="test",
    ):

        self.pipeline_name = pipeline_name
        self.role_arn = role_arn
        self.bucket = bucket
        self.region = region
        self.model_package_arn = model_package_arn
        self.model_name = model_name
        self.datalake_path = datalake_path
        self.environment = environment

        # Data prams
        self.input_data_path = ParameterString(
            name="InputDataPath", default_value=input_data_path
        )
        self.output_data_path = ParameterString(
            name="OutputDataPath", default_value=output_data_path
        )

        # Neo4j params (Optional)
        if neo4j_uri:
            self.neo4j_uri = ParameterString(name="Neo4jUri", default_value=neo4j_uri)
            self.neo4j_username = ParameterString(
                name="Neo4jUsername", default_value=neo4j_username or "neo4j"
            )
            self.neo4j_password = ParameterString(
                name="Neo4jPassword", default_value=neo4j_password or ""
            )
        else:
            self.neo4j_uri = None
            self.neo4j_username = None
            self.neo4j_password = None

        # Infra params
        self.processing_instance_type = ParameterString(
            name="ProcessingInstanceType", default_value=processing_instance_type
        )
        self.transform_instance_type = ParameterString(
            name="TransformInstanceType", default_value=transform_instance_type
        )
        self.transform_instance_count = ParameterInteger(
            name="TransformInstanceCount", default_value=transform_instance_count
        )

        # Environment params
        self.environment_param = ParameterString(
            name="Environment", default_value=environment
        )

        # s3 paths:
        self.base_s3_path = f"s3://{bucket}/fraud-inference-pipeline"
        self.code_path = f"{self.base_s3_path}/code"
        self.temp_path = f"{self.base_s3_path}/temp"


def create_data_preparation_step(config, sagemaker_session):
    """
    Create the data preparation step for the inference
    """

    logger.info("🔧 Creating the data preparation step...")

    # Processor in order to prepare the data:
    sklearn_processor = SKLearnProcessor(
        framework_version="1.0-1",
        instance_type=config.processing_instance_type,
        instance_count=1,
        base_job_name="fraud-inference-data-prep",
        sagemaker_session=sagemaker_session,
        role=config.role_arn,
    )

    # Inputs of the step
    inputs = [
        ProcessingInput(
            source=config.input_data_path, destination="/opt/ml/processing/input"
        ),
        ProcessingInput(source=config.code_path, destination="/opt/ml/processing/code"),
    ]

    # Outputs of the step
    outputs = [
        ProcessingOutput(
            output_name="prepared-data",
            source="/opt/ml/processing/output",
            destination=f"{config.temp_path}/prepared_data",
        ),
        ProcessingOutput(
            output_name="data-manifest",
            source="/opt/ml/processing/manifest",
            destination=f"{config.temp_path}/manifest",
        ),
    ]

    # Job args
    job_arguments = [
        "--input-path",
        "/opt/ml/processing/input",
        "--output-path",
        "/opt/ml/processing/output",
        "--manifest-path",
        "/opt/ml/processing/manifest",
        "--environment",
        config.environment_param,
    ]

    # Add Neo4j params if they are available
    if config.neo4j_uri:
        job_arguments.extend(
            [
                "--neo4j-uri",
                config.neo4j_uri,
                "--neo4j-username",
                config.neo4j_username,
                "--neo4j-password",
                config.neo4j_password,
            ]
        )

    step_prepare = ProcessingStep(
        name="PrepareInferenceData",
        processor=sklearn_processor,
        inputs=inputs,
        outputs=outputs,
        code="inference/prepare_data.py",
        job_arguments=job_arguments,
    )

    logger.info("✅ Data preparation step created")
    return step_prepare


def create_batch_transform_step(config, sagemaker_session, data_preparation_step):
    """
    Create the batch transformation step
    """

    logger.info("🔧 Creating the batch transformation step...")

    # Crear modelo desde el registro de modelos
    if config.model_package_arn:
        # Usar modelo registrado
        model = Model(
            model_data=config.model_package_arn,
            role=config.role_arn,
            sagemaker_session=sagemaker_session,
        )
    else:
        # Use specific model (requires more configuration)
        logger.warning("Using specific model - Ensure that it is correctly configured")
        model = PyTorchModel(
            model_data=f"s3://{config.bucket}/models/{config.model_name}/model.tar.gz",
            role=config.role_arn,
            entry_point="inference.py",
            framework_version="1.12.0",
            py_version="py38",
            sagemaker_session=sagemaker_session,
        )

    # Create the transformer
    transformer = Transformer(
        model_name=(
            model.name
            if hasattr(model, "name")
            else f"fraud-model-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        ),
        instance_type=config.transform_instance_type,
        instance_count=config.transform_instance_count,
        output_path=config.output_data_path,
        sagemaker_session=sagemaker_session,
        accept="application/json",
        content_type="application/json",
        strategy="MultiRecord",
        assemble_with="Line",
    )

    # Create the transformation step
    step_transform = TransformStep(
        name="BatchTransformFraudDetection",
        transformer=transformer,
        inputs=data_preparation_step.properties.ProcessingOutputConfig.Outputs[
            "prepared-data"
        ].S3Output.S3Uri,
    )

    logger.info("✅ Batch transformation step created")
    return step_transform


def create_results_processing_step(config, sagemaker_session, transform_step):
    """
    Create the results processing step.
    """

    logger.info("🔧 Creating the results processing step...")

    # Processor in order to process the results
    sklearn_processor = SKLearnProcessor(
        framework_version="1.0-1",
        instance_type=config.processing_instance_type,
        instance_count=1,
        base_job_name="fraud-inference-results",
        sagemaker_session=sagemaker_session,
        role=config.role_arn,
    )

    # Set the output path
    final_output_path = config.output_data_path
    if config.environment == "prod" and config.datalake_path:
        final_output_path = ParameterString(
            name="FinalOutputPath", default_value=config.datalake_path
        )

    step_results = ProcessingStep(
        name="ProcessInferenceResults",
        processor=sklearn_processor,
        inputs=[
            ProcessingInput(
                source=transform_step.properties.TransformOutput.S3OutputPath,
                destination="/opt/ml/processing/predictions",
            ),
            ProcessingInput(
                source=config.code_path, destination="/opt/ml/processing/code"
            ),
        ],
        outputs=[
            ProcessingOutput(
                output_name="final-results",
                source="/opt/ml/processing/output/results",
                destination=final_output_path,
            ),
            ProcessingOutput(
                output_name="summary-report",
                source="/opt/ml/processing/output/report",
                destination=f"{config.base_s3_path}/reports",
            ),
        ],
        code="inference/process_results.py",
        job_arguments=[
            "--predictions-path",
            "/opt/ml/processing/predictions",
            "--output-path",
            "/opt/ml/processing/output",
            "--environment",
            config.environment_param,
        ],
    )

    logger.info("✅ Results processing step created")
    return step_results


def upload_inference_data_to_s3(
    local_data_path, bucket, s3_prefix="fraud-inference-pipeline/data"
):
    """
    Upload the inference data to s3
    """

    logger.info("📤 Uploading the inference data to s3...")

    s3_client = boto3.client("s3")

    import os

    if os.path.isfile(local_data_path):
        # Unique file
        filename = os.path.basename(local_data_path)
        s3_key = f"{s3_prefix}/{filename}"

        try:
            s3_client.upload_file(local_data_path, bucket, s3_key)
            logger.info(f"  ✅ {filename} -> s3://{bucket}/{s3_key}")
            return f"s3://{bucket}/{s3_key}"
        except Exception as e:
            logger.error(f"  ❌ Error subiendo {filename}: {e}")
            raise

    elif os.path.isdir(local_data_path):
        # Directory completed
        uploaded_files = []
        for root, dirs, files in os.walk(local_data_path):
            for file in files:
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, local_data_path)
                s3_key = f"{s3_prefix}/{relative_path}"

                try:
                    s3_client.upload_file(local_file, bucket, s3_key)
                    logger.info(f"  ✅ {relative_path} -> s3://{bucket}/{s3_key}")
                    uploaded_files.append(f"s3://{bucket}/{s3_key}")
                except Exception as e:
                    logger.error(f"  ❌ Error uploading {relative_path}: {e}")

        logger.info(f"✅ {len(uploaded_files)} files uploaded to s3")
        return f"s3://{bucket}/{s3_prefix}/"

    else:
        raise ValueError(f"Not valid path: {local_data_path}")


def validate_inference_data(data_path):
    """
    Validate input data for inference
    """

    logger.info("🔍 Validating input data for inference...")

    # Aquí implementarías validaciones específicas
    # Por ejemplo, verificar formato, esquema, etc.

    validations = []

    # Basic validations
    if not data_path:
        validations.append("❌ Data path is mandatory")

    if data_path and not data_path.startswith("s3://"):
        validations.append("❌ The data must be in s3")

    if validations:
        for validation in validations:
            logger.error(validation)
        raise ValueError("Inference data is invalid")

    logger.info("✅ Inference data validated")


def create_inference_manifest(data_path, output_path):
    """
    Create the inference manifest
    """

    logger.info("📋 Creating the inference manifest...")

    s3_client = boto3.client("s3")

    # Parse s3 paths
    bucket = data_path.replace("s3://", "").split("/")[0]
    prefix = "/".join(data_path.replace("s3://", "").split("/")[1:])

    # List files
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    manifest = []
    for obj in response.get("Contents", []):
        if obj["Key"].endswith((".json", ".jsonl")):
            manifest.append(f"s3://{bucket}/{obj['Key']}")

    # Save the manifest
    manifest_content = "\n".join(manifest)
    manifest_key = f"{output_path.replace('s3://', '').split('/', 1)[1]}/manifest.txt"

    s3_client.put_object(
        Bucket=bucket, Key=manifest_key, Body=manifest_content, ContentType="text/plain"
    )

    logger.info(f"✅ Manifest created with {len(manifest)} files")
    return f"s3://{bucket}/{manifest_key}"


def monitor_batch_transform_job(job_name, sagemaker_client):
    """
    Monitor the transformation job
    """

    logger.info(f"🔍 Monitoring the transformation job: {job_name}")

    while True:
        response = sagemaker_client.describe_transform_job(TransformJobName=job_name)

        status = response["TransformJobStatus"]

        if status == "Completed":
            logger.info("✅ Transformation job completed")
            break
        elif status == "Failed":
            logger.error(
                f"❌ Transformation job failed: {response.get('FailureReason', 'Unknown error')}"
            )
            break
        elif status == "InProgress":
            logger.info("🔄 Transformation job in progress...")

        import time

        time.sleep(30)

    return status
