"""
Functional code that feeds the main.py from the train_pipeline.
Here we will be defining each step of the pipeline.
"""

# 1. Import necessary libraries
import boto3
from sagemaker.workflow.steps import ProcessingStep, TrainingStep
from sagemaker.workflow.step_collections import RegisterModel
from sagemaker.workflow.conditions import ConditionGreaterThanOrEqualTo
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.functions import JsonGet
from sagemaker.workflow.parameters import (
    ParameterInteger,
    ParameterString,
    ParameterFloat,
)
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.pytorch import PyTorch
from sagemaker.model_metrics import MetricsSource, ModelMetrics
import logging

# 2. Define global variables
logger = logging.getLogger(__name__)


# 3. Functions/Classes
class FraudDetectionPipelineConfig:
    """
    Fraud detection pipeline configuration
    """

    def __init__(
        self,
        pipeline_name,
        role_arn,
        bucket,
        region,
        neo4j_uri,
        neo4j_username,
        neo4j_password,
        hidden_dim=128,
        num_layers=4,
        learning_rate=0.001,
        epochs=100,
        processing_instance_type="ml.m5.xlarge",
        training_instance_type="ml.g4dn.xlarge",
    ):

        self.pipeline_name = pipeline_name
        self.role_arn = role_arn
        self.bucket = bucket
        self.region = region

        # Neo4j params
        self.neo4j_uri = ParameterString(name="Neo4jUri", default_value=neo4j_uri)
        self.neo4j_username = ParameterString(
            name="Neo4jUsername", default_value=neo4j_username
        )
        self.neo4j_password = ParameterString(
            name="Neo4jPassword", default_value=neo4j_password
        )

        # Model params
        self.hidden_dim = ParameterInteger(name="HiddenDim", default_value=hidden_dim)
        self.num_layers = ParameterInteger(name="NumLayers", default_value=num_layers)
        self.learning_rate = ParameterFloat(
            name="LearningRate", default_value=learning_rate
        )
        self.epochs = ParameterInteger(name="Epochs", default_value=epochs)

        # Infra params
        self.processing_instance_type = ParameterString(
            name="ProcessingInstanceType", default_value=processing_instance_type
        )
        self.training_instance_type = ParameterString(
            name="TrainingInstanceType", default_value=training_instance_type
        )

        # Umbral para registro del modelo
        self.accuracy_threshold = ParameterFloat(
            name="AccuracyThreshold", default_value=0.85
        )

        # s3 paths
        self.base_s3_path = f"s3://{bucket}/fraud-detection-pipeline"
        self.data_path = f"{self.base_s3_path}/data"
        self.code_path = f"{self.base_s3_path}/code"
        self.model_path = f"{self.base_s3_path}/models"


def create_processing_step(config, sagemaker_session):
    """
    Creates the processing step
    """

    logger.info("🔧 Creating the processing step...")

    # Sklearn processor
    sklearn_processor = SKLearnProcessor(
        framework_version="1.0-1",
        instance_type=config.processing_instance_type,
        instance_count=1,
        base_job_name="fraud-data-processing",
        sagemaker_session=sagemaker_session,
        role=config.role_arn,
    )

    step_process = ProcessingStep(
        name="ProcessNeo4jData",
        processor=sklearn_processor,
        inputs=[
            ProcessingInput(
                source=config.code_path, destination="/opt/ml/processing/code"
            )
        ],
        outputs=[
            ProcessingOutput(
                output_name="train-data",
                source="/opt/ml/processing/train",
                destination=f"{config.data_path}/train",
            ),
            ProcessingOutput(
                output_name="validation-data",
                source="/opt/ml/processing/validation",
                destination=f"{config.data_path}/validation",
            ),
            ProcessingOutput(
                output_name="test-data",
                source="/opt/ml/processing/test",
                destination=f"{config.data_path}/test",
            ),
            ProcessingOutput(
                output_name="data-report",
                source="/opt/ml/processing/report",
                destination=f"{config.data_path}/report",
            ),
        ],
        code="feature_step/main.py",
        job_arguments=[
            "--neo4j-uri",
            config.neo4j_uri,
            "--neo4j-username",
            config.neo4j_username,
            "--neo4j-password",
            config.neo4j_password,
        ],
    )

    logger.info("✅ Processing step created")
    return step_process


def create_training_step(config, sagemaker_session, processing_step):
    """
    Create the training step
    """

    logger.info("🔧 Creating the training step...")

    # PyTorch estimator
    pytorch_estimator = PyTorch(
        entry_point="train_step/training_and_prediction.py",
        source_dir=config.code_path,
        role=config.role_arn,
        instance_type=config.training_instance_type,
        instance_count=1,
        framework_version="1.12.0",
        py_version="py38",
        hyperparameters={
            "hidden-dim": config.hidden_dim,
            "num-layers": config.num_layers,
            "learning-rate": config.learning_rate,
            "epochs": config.epochs,
        },
        sagemaker_session=sagemaker_session,
        metric_definitions=[
            {"Name": "train:loss", "Regex": "Train Loss: ([0-9\\.]+)"},
            {
                "Name": "validation:accuracy",
                "Regex": "Validation Accuracy: ([0-9\\.]+)",
            },
            {"Name": "validation:f1", "Regex": "Validation F1: ([0-9\\.]+)"},
            {"Name": "test:accuracy", "Regex": "Test Accuracy: ([0-9\\.]+)"},
            {"Name": "test:roc_auc", "Regex": "Test ROC AUC: ([0-9\\.]+)"},
        ],
        # Configure in order to use GPU
        use_spot_instances=False,
        max_run=7200,  # 2 hours max
        environment={
            "PYTHONPATH": "/opt/ml/code",
            "SAGEMAKER_REQUIREMENTS": "requirements.txt",
        },
    )

    step_train = TrainingStep(
        name="TrainHeteroGNN",
        estimator=pytorch_estimator,
        inputs={
            "train": processing_step.properties.ProcessingOutputConfig.Outputs[
                "train-data"
            ].S3Output.S3Uri,
            "validation": processing_step.properties.ProcessingOutputConfig.Outputs[
                "validation-data"
            ].S3Output.S3Uri,
            "test": processing_step.properties.ProcessingOutputConfig.Outputs[
                "test-data"
            ].S3Output.S3Uri,
        },
    )

    logger.info("✅ Training step created")
    return step_train


def create_evaluation_step(config, sagemaker_session, training_step):
    """
    Evaluation step
    """

    logger.info("🔧 CCreating the evaluating step...")

    evaluation_processor = SKLearnProcessor(
        framework_version="1.0-1",
        instance_type="ml.m5.xlarge",
        instance_count=1,
        base_job_name="fraud-model-evaluation",
        sagemaker_session=sagemaker_session,
        role=config.role_arn,
    )

    step_eval = ProcessingStep(
        name="EvaluateModel",
        processor=evaluation_processor,
        inputs=[
            ProcessingInput(
                source=training_step.properties.ModelArtifacts.S3ModelArtifacts,
                destination="/opt/ml/processing/model",
            ),
            ProcessingInput(
                source=config.code_path, destination="/opt/ml/processing/code"
            ),
        ],
        outputs=[
            ProcessingOutput(
                output_name="evaluation-report",
                source="/opt/ml/processing/evaluation",
                destination=f"{config.base_s3_path}/evaluation",
            )
        ],
        code="evaluation/evaluate_model.py",
    )

    logger.info("✅ Evaluating step created")
    return step_eval


def create_model_registration_step(config, training_step, evaluation_step):
    """
    Create the model registration step
    """

    logger.info("🔧 Creating the model registration step...")

    # Model metrics
    model_metrics = ModelMetrics(
        model_statistics=MetricsSource(
            s3_uri=f"{evaluation_step.properties.ProcessingOutputConfig.Outputs['evaluation-report'].S3Output.S3Uri}/evaluation.json",
            content_type="application/json",
        )
    )

    # Model registry
    register_model_step = RegisterModel(
        name="RegisterFraudModel",
        estimator=training_step.estimator,
        model_data=training_step.properties.ModelArtifacts.S3ModelArtifacts,
        content_types=["application/json"],
        response_types=["application/json"],
        inference_instances=["ml.t2.medium", "ml.m5.large"],
        transform_instances=["ml.m5.large"],
        model_package_group_name="fraud-detection-hetero-models",
        approval_status="PendingManualApproval",
        model_metrics=model_metrics,
        description="Modelo GNN heterogéneo para detección de fraude en contadores eléctricos",
    )

    # # Condition for creating an automatic registry
    cond_gte = ConditionGreaterThanOrEqualTo(
        left=JsonGet(
            step_name=evaluation_step.name,
            property_file="evaluation.json",
            json_path="accuracy.value",
        ),
        right=config.accuracy_threshold,
    )

    step_cond = ConditionStep(
        name="CheckAccuracyCondition",
        conditions=[cond_gte],
        if_steps=[register_model_step],
        else_steps=[],
    )

    logger.info("✅ Model registry step completed")
    return step_cond


def upload_source_code_to_s3(
    bucket, local_code_path, s3_prefix="fraud-detection-pipeline/code"
):
    """
    Upload the source code to s3
    """

    logger.info("📤 Uploading the source code to s3...")

    s3_client = boto3.client("s3")

    import os

    for root, dirs, files in os.walk(local_code_path):
        for file in files:
            if file.endswith(".py") or file.endswith(".txt"):
                local_file = os.path.join(root, file)
                relative_path = os.path.relpath(local_file, local_code_path)
                s3_key = f"{s3_prefix}/{relative_path}"

                try:
                    s3_client.upload_file(local_file, bucket, s3_key)
                    logger.info(f"  ✅ {relative_path} -> s3://{bucket}/{s3_key}")
                except Exception as e:
                    logger.error(f"  ❌ Error subiendo {relative_path}: {e}")

    logger.info("✅ Uploaded the source code to s3")


def validate_pipeline_parameters(config):
    """
    Validate pipeline's params
    """

    logger.info("🔍 Validating pipeline's params...")

    validations = []

    # Validate mandatory params
    if not config.pipeline_name:
        validations.append("❌ Pipeline's name is mandatory")

    if not config.role_arn:
        validations.append("❌ Rol's ARN is mandatory")

    if not config.bucket:
        validations.append("❌ s3 Bucket is mandatory")

    # Validar parámetros de Neo4j
    if not config.neo4j_uri.default_value:
        validations.append("❌ Neo4j URI is mandatory")

    if not config.neo4j_username.default_value:
        validations.append("❌ Neo4j user is mandatory")

    if not config.neo4j_password.default_value:
        validations.append("❌ Neo4j password is mandatory")

    # Validte model's params
    if config.hidden_dim.default_value <= 0:
        validations.append("❌ Hidden dim must be > 0")

    if config.epochs.default_value <= 0:
        validations.append("❌ Number of epochs must be > 0")

    if config.learning_rate.default_value <= 0:
        validations.append("❌ Learning rate must be > 0")

    if validations:
        for validation in validations:
            logger.error(validation)
        raise ValueError("Pipeline params not valid")

    logger.info("✅ Params of the pipeline validated")


def create_pipeline_execution_report(sagemaker_client, pipeline_name, execution_arn):
    """
    Create detailed pipeline execution report
    """

    logger.info("📊 Creating execution report...")

    try:
        # Info of the execution
        execution_info = sagemaker_client.describe_pipeline_execution(
            PipelineExecutionArn=execution_arn
        )

        # Info of the steps
        steps_info = sagemaker_client.list_pipeline_execution_steps(
            PipelineExecutionArn=execution_arn
        )

        report = {
            "pipeline_name": pipeline_name,
            "execution_arn": execution_arn,
            "status": execution_info["PipelineExecutionStatus"],
            "creation_time": (
                execution_info.get("CreationTime", "").isoformat()
                if execution_info.get("CreationTime")
                else None
            ),
            "last_modified_time": (
                execution_info.get("LastModifiedTime", "").isoformat()
                if execution_info.get("LastModifiedTime")
                else None
            ),
            "steps": [],
        }

        for step in steps_info["PipelineExecutionSteps"]:
            step_report = {
                "step_name": step["StepName"],
                "step_status": step["StepStatus"],
                "start_time": (
                    step.get("StartTime", "").isoformat()
                    if step.get("StartTime")
                    else None
                ),
                "end_time": (
                    step.get("EndTime", "").isoformat() if step.get("EndTime") else None
                ),
            }

            if step["StepStatus"] == "Failed" and "FailureReason" in step:
                step_report["failure_reason"] = step["FailureReason"]

            report["steps"].append(step_report)

        logger.info("✅ Execution report created")
        return report

    except Exception as e:
        logger.error(f"❌ Error when creating the execution report: {e}")
        return None
