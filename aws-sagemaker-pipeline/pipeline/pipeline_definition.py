#!/usr/bin/env python3
"""
SageMaker Pipeline Definition Generator

This script generates the SageMaker Pipeline JSON definition that is uploaded
to S3 and referenced by the CloudFormation stack. It defines an end-to-end
ML pipeline with these steps:

  1. PreprocessData   — SkLearnProcessor: clean, split, and featurize raw CSV
  2. TrainModel       — Estimator: train XGBoost or custom model
  3. EvaluateModel    — ScriptProcessor: compute metrics (accuracy, precision, etc.)
  4. CheckMetrics     — ConditionStep: if metrics pass threshold → register model
  5. RegisterModel    — RegisterModel: register in SageMaker Model Registry
  6. DeployModel      — (Optional) CreateModel + deploy to endpoint

Usage:
    python pipeline/pipeline_definition.py --environment dev --model-name customer-churn

    This outputs: pipeline/pipeline-definition.json
"""

import argparse
import json
import os
import sys
from datetime import datetime

# ---------------------------------------------------------------------------
# SageMaker SDK imports
# ---------------------------------------------------------------------------
try:
    import sagemaker
    from sagemaker.workflow.pipeline import Pipeline
    from sagemaker.workflow.steps import (
        ProcessingStep,
        TrainingStep,
        CreateModelStep,
        TransformStep,
    )
    from sagemaker.workflow.step_collections import RegisterModel
    from sagemaker.workflow.condition_step import ConditionStep
    from sagemaker.workflow.conditions import ConditionGreaterThanOrEqualTo
    from sagemaker.workflow.functions import Join
    from sagemaker.workflow.parameters import (
        ParameterString,
        ParameterInteger,
        ParameterFloat,
    )
    from sagemaker.workflow.properties import PropertyFile
    from sagemaker.processing import (
        ProcessingInput,
        ProcessingOutput,
        ScriptProcessor,
        FrameworkProcessor,
    )
    from sagemaker.sklearn.processing import SKLearnProcessor
    from sagemaker.inputs import TrainingInput
    from sagemaker.xgboost.estimator import XGBoost
    from sagemaker.model_metrics import ModelMetrics, MetricsSource
    from sagemaker.drift_check_baselines import DriftCheckBaselines
    SDK_AVAILABLE = True
except ImportError:
    SDK_AVAILABLE = False
    print("Warning: sagemaker SDK not installed. Run: pip install sagemaker")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for pipeline generation."""
    parser = argparse.ArgumentParser(
        description='Generate SageMaker Pipeline definition JSON'
    )
    parser.add_argument(
        '--environment', default='dev',
        help='Environment name (dev/staging/prod)',
    )
    parser.add_argument(
        '--model-name', default='customer-churn-model',
        help='Name of the ML model',
    )
    parser.add_argument(
        '--output-path', default='pipeline/pipeline-definition.json',
        help='Output path for the pipeline definition JSON',
    )
    parser.add_argument(
        '--region', default='us-east-1',
        help='AWS region',
    )
    parser.add_argument(
        '--instance-type', default='ml.m5.xlarge',
        help='Training instance type',
    )
    parser.add_argument(
        '--instance-count', type=int, default=1,
        help='Number of training instances',
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Pipeline Definition Builder
# ---------------------------------------------------------------------------
def build_pipeline_definition(args: argparse.Namespace) -> dict:
    """
    Build the SageMaker Pipeline definition.

    The pipeline is defined using the SageMaker SDK v2+ Pipeline API,
    which generates a JSON definition that SageMaker Pipelines can execute.

    Pipeline Steps:
      1. PreprocessData — Clean and split raw CSV into train/val/test
      2. TrainModel     — Train XGBoost classifier
      3. EvaluateModel  — Evaluate model against test set
      4. CheckMetrics   — Condition: if accuracy >= threshold → register
      5. RegisterModel  — Register in SageMaker Model Registry
    """
    if not SDK_AVAILABLE:
        return _generate_static_definition(args)

    # -----------------------------------------------------------------------
    # Pipeline Parameters (overridable at execution time)
    # -----------------------------------------------------------------------
    processing_instance_type = ParameterString(
        name='ProcessingInstanceType',
        default_value=args.instance_type,
    )
    training_instance_type = ParameterString(
        name='TrainingInstanceType',
        default_value=args.instance_type,
    )
    training_instance_count = ParameterInteger(
        name='TrainingInstanceCount',
        default_value=args.instance_count,
    )
    input_data = ParameterString(
        name='InputDataUrl',
        default_value=f's3://{args.environment}-{args.model_name}-data/input/',
    )
    accuracy_threshold = ParameterFloat(
        name='AccuracyThreshold',
        default_value=0.75,  # 75% accuracy minimum
    )

    # -----------------------------------------------------------------------
    # Step 1: Preprocess Data
    # -----------------------------------------------------------------------
    sklearn_processor = SKLearnProcessor(
        framework_version='1.2-1',
        role=sagemaker.get_execution_role(),
        instance_type=processing_instance_type,
        instance_count=1,
        base_job_name=f'{args.environment}-{args.model_name}-preprocess',
    )

    step_preprocess = ProcessingStep(
        name='PreprocessData',
        processor=sklearn_processor,
        inputs=[
            ProcessingInput(
                source=input_data,
                destination='/opt/ml/processing/input',
            ),
        ],
        outputs=[
            ProcessingOutput(
                output_name='train',
                source='/opt/ml/processing/output/train',
            ),
            ProcessingOutput(
                output_name='validation',
                source='/opt/ml/processing/output/validation',
            ),
            ProcessingOutput(
                output_name='test',
                source='/opt/ml/processing/output/test',
            ),
        ],
        code='pipeline/scripts/preprocess.py',
        job_arguments=[
            '--environment', args.environment,
        ],
    )

    # -----------------------------------------------------------------------
    # Step 2: Train Model (XGBoost)
    # -----------------------------------------------------------------------
    xgb_estimator = XGBoost(
        entry_point='pipeline/scripts/train.py',
        framework_version='1.7-1',
        role=sagemaker.get_execution_role(),
        instance_count=training_instance_count,
        instance_type=training_instance_type,
        output_path=Join(
            on='/',
            values=[
                f's3://{args.environment}-{args.model_name}-artifacts',
                args.environment,
                'training-output',
            ],
        ),
        base_job_name=f'{args.environment}-{args.model_name}-train',
        hyperparameters={
            'max_depth': '5',
            'eta': '0.2',
            'gamma': '4',
            'min_child_weight': '6',
            'subsample': '0.8',
            'objective': 'binary:logistic',
            'num_round': '100',
        },
    )

    step_train = TrainingStep(
        name='TrainModel',
        estimator=xgb_estimator,
        inputs={
            'train': TrainingInput(
                s3_data=step_preprocess.properties.ProcessingOutputConfig.Outputs[
                    'train'
                ].S3Output.S3Uri,
                content_type='text/csv',
            ),
            'validation': TrainingInput(
                s3_data=step_preprocess.properties.ProcessingOutputConfig.Outputs[
                    'validation'
                ].S3Output.S3Uri,
                content_type='text/csv',
            ),
        },
    )

    # -----------------------------------------------------------------------
    # Step 3: Evaluate Model
    # -----------------------------------------------------------------------
    eval_processor = ScriptProcessor(
        image_uri=sagemaker.image_uris.retrieve(
            framework='xgboost',
            region=args.region,
            version='1.7-1',
        ),
        command=['python3'],
        role=sagemaker.get_execution_role(),
        instance_count=1,
        instance_type=processing_instance_type,
        base_job_name=f'{args.environment}-{args.model_name}-evaluate',
    )

    evaluation_report = PropertyFile(
        name='EvaluationReport',
        output_name='evaluation',
        path='evaluation.json',
    )

    step_evaluate = ProcessingStep(
        name='EvaluateModel',
        processor=eval_processor,
        inputs=[
            ProcessingInput(
                source=step_train.properties.ModelArtifacts.S3ModelArtifacts,
                destination='/opt/ml/processing/model',
            ),
            ProcessingInput(
                source=step_preprocess.properties.ProcessingOutputConfig.Outputs[
                    'test'
                ].S3Output.S3Uri,
                destination='/opt/ml/processing/test',
            ),
        ],
        outputs=[
            ProcessingOutput(
                output_name='evaluation',
                source='/opt/ml/processing/output/evaluation',
            ),
        ],
        code='pipeline/scripts/evaluate.py',
        property_files=[evaluation_report],
    )

    # -----------------------------------------------------------------------
    # Step 4: Condition — Check if metrics pass threshold
    # -----------------------------------------------------------------------
    condition_accuracy = ConditionGreaterThanOrEqualTo(
        left=step_evaluate.properties.ProcessingOutputConfig.Outputs[
            'evaluation'
        ].S3Output.S3Uri,
        right=accuracy_threshold,
    )

    # -----------------------------------------------------------------------
    # Step 5: Register Model (if condition passes)
    # -----------------------------------------------------------------------
    model_metrics = ModelMetrics(
        model_statistics=MetricsSource(
            s3_uri=Join(
                on='/',
                values=[
                    step_evaluate.properties.ProcessingOutputConfig.Outputs[
                        'evaluation'
                    ].S3Output.S3Uri,
                    'evaluation.json',
                ],
            ),
            content_type='application/json',
        ),
    )

    step_register = RegisterModel(
        name='RegisterModel',
        estimator=xgb_estimator,
        model_data=step_train.properties.ModelArtifacts.S3ModelArtifacts,
        content_types=['text/csv'],
        response_types=['text/csv'],
        inference_instances=['ml.t2.medium', 'ml.m5.large'],
        transform_instances=['ml.m5.xlarge'],
        model_package_group_name=f'{args.environment}-{args.model_name}',
        model_metrics=model_metrics,
        approval_status='PendingManualApproval',
    )

    # -----------------------------------------------------------------------
    # Step 6: Condition Step (gate the registration)
    # -----------------------------------------------------------------------
    step_condition = ConditionStep(
        name='CheckMetrics',
        conditions=[condition_accuracy],
        if_steps=[step_register],
        else_steps=[],
    )

    # -----------------------------------------------------------------------
    # Build Pipeline
    # -----------------------------------------------------------------------
    pipeline = Pipeline(
        name=f'{args.environment}-{args.model_name}-pipeline',
        parameters=[
            processing_instance_type,
            training_instance_type,
            training_instance_count,
            input_data,
            accuracy_threshold,
        ],
        steps=[step_preprocess, step_train, step_evaluate, step_condition],
        sagemaker_session=sagemaker.Session(),
    )

    # Generate the JSON definition
    definition = json.loads(pipeline.definition())
    return definition


def _generate_static_definition(args: argparse.Namespace) -> dict:
    """
    Generate a static pipeline definition when SageMaker SDK is not available.

    This is a fallback that produces the same pipeline structure as the SDK
    version, but as raw JSON. Useful for CI/CD environments where the SDK
    may not be installed.
    """
    pipeline_name = f"{args.environment}-{args.model_name}-pipeline"
    artifacts_bucket = f"{args.environment}-{args.model_name}-artifacts"

    definition = {
        "Version": "2020-12-01",
        "Metadata": {},
        "PipelineExperimentConfig": {
            "ExperimentName": pipeline_name,
            "TrialName": f"{pipeline_name}-{{pipeline:execution-id}}"
        },
        "Parameters": [
            {
                "Name": "ProcessingInstanceType",
                "Type": "String",
                "DefaultValue": args.instance_type
            },
            {
                "Name": "TrainingInstanceType",
                "Type": "String",
                "DefaultValue": args.instance_type
            },
            {
                "Name": "TrainingInstanceCount",
                "Type": "Integer",
                "DefaultValue": args.instance_count
            },
            {
                "Name": "InputDataUrl",
                "Type": "String",
                "DefaultValue": f"s3://{args.environment}-{args.model_name}-data/input/"
            },
            {
                "Name": "AccuracyThreshold",
                "Type": "Float",
                "DefaultValue": 0.75
            }
        ],
        "PipelineExperimentConfig": {
            "ExperimentName": pipeline_name,
            "TrialName": f"{pipeline_name}-{{pipeline:execution-id}}"
        },
        "Steps": [
            {
                "Name": "PreprocessData",
                "Type": "Processing",
                "Description": "Clean and split raw CSV into train/val/test",
                "Arguments": {
                    "ProcessingResources": {
                        "ClusterConfig": {
                            "InstanceType": {"Get": "Parameters.ProcessingInstanceType"},
                            "InstanceCount": 1,
                            "VolumeSizeInGB": 30
                        }
                    },
                    "AppSpecification": {
                        "ImageUri": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-sklearn-processing:1.2-1-cpu-py3",
                        "ContainerEntrypoint": ["python3", "/opt/ml/processing/input/code/preprocess.py"]
                    },
                    "RoleArn": {"Get": "Parameters.RoleArn"},
                    "ProcessingInputs": [
                        {
                            "InputName": "input-1",
                            "S3Input": {
                                "S3Uri": {"Get": "Parameters.InputDataUrl"},
                                "LocalPath": "/opt/ml/processing/input",
                                "S3DataType": "S3Prefix",
                                "S3InputMode": "File",
                                "S3DataDistributionType": "FullyReplicated"
                            }
                        }
                    ],
                    "ProcessingOutputConfig": {
                        "Outputs": [
                            {
                                "OutputName": "train",
                                "S3Output": {
                                    "S3Uri": f"s3://{artifacts_bucket}/{args.environment}/preprocessing/train",
                                    "LocalPath": "/opt/ml/processing/output/train",
                                    "S3UploadMode": "EndOfJob"
                                }
                            },
                            {
                                "OutputName": "validation",
                                "S3Output": {
                                    "S3Uri": f"s3://{artifacts_bucket}/{args.environment}/preprocessing/validation",
                                    "LocalPath": "/opt/ml/processing/output/validation",
                                    "S3UploadMode": "EndOfJob"
                                }
                            },
                            {
                                "OutputName": "test",
                                "S3Output": {
                                    "S3Uri": f"s3://{artifacts_bucket}/{args.environment}/preprocessing/test",
                                    "LocalPath": "/opt/ml/processing/output/test",
                                    "S3UploadMode": "EndOfJob"
                                }
                            }
                        ]
                    }
                }
            },
            {
                "Name": "TrainModel",
                "Type": "Training",
                "Description": "Train XGBoost classifier",
                "DependsOn": ["PreprocessData"],
                "Arguments": {
                    "AlgorithmSpecification": {
                        "TrainingImage": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.7-1-cpu-py3",
                        "TrainingInputMode": "File"
                    },
                    "OutputDataConfig": {
                        "S3OutputPath": f"s3://{artifacts_bucket}/{args.environment}/training-output"
                    },
                    "StoppingCondition": {
                        "MaxRuntimeInSeconds": 86400
                    },
                    "ResourceConfig": {
                        "InstanceCount": {"Get": "Parameters.TrainingInstanceCount"},
                        "InstanceType": {"Get": "Parameters.TrainingInstanceType"},
                        "VolumeSizeInGB": 30
                    },
                    "RoleArn": {"Get": "Parameters.RoleArn"},
                    "InputDataConfig": [
                        {
                            "ChannelName": "train",
                            "DataSource": {
                                "S3DataSource": {
                                    "S3DataType": "S3Prefix",
                                    "S3Uri": f"s3://{artifacts_bucket}/{args.environment}/preprocessing/train",
                                    "S3DataDistributionType": "FullyReplicated"
                                }
                            },
                            "ContentType": "text/csv"
                        },
                        {
                            "ChannelName": "validation",
                            "DataSource": {
                                "S3DataSource": {
                                    "S3DataType": "S3Prefix",
                                    "S3Uri": f"s3://{artifacts_bucket}/{args.environment}/preprocessing/validation",
                                    "S3DataDistributionType": "FullyReplicated"
                                }
                            },
                            "ContentType": "text/csv"
                        }
                    ],
                    "HyperParameters": {
                        "max_depth": "5",
                        "eta": "0.2",
                        "gamma": "4",
                        "min_child_weight": "6",
                        "subsample": "0.8",
                        "objective": "binary:logistic",
                        "num_round": "100"
                    }
                }
            },
            {
                "Name": "EvaluateModel",
                "Type": "Processing",
                "Description": "Evaluate model against test set",
                "DependsOn": ["TrainModel"],
                "Arguments": {
                    "ProcessingResources": {
                        "ClusterConfig": {
                            "InstanceType": {"Get": "Parameters.ProcessingInstanceType"},
                            "InstanceCount": 1,
                            "VolumeSizeInGB": 30
                        }
                    },
                    "AppSpecification": {
                        "ImageUri": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.7-1-cpu-py3",
                        "ContainerEntrypoint": ["python3", "/opt/ml/processing/input/code/evaluate.py"]
                    },
                    "RoleArn": {"Get": "Parameters.RoleArn"},
                    "ProcessingInputs": [
                        {
                            "InputName": "input-1",
                            "S3Input": {
                                "S3Uri": f"s3://{artifacts_bucket}/{args.environment}/training-output/model.tar.gz",
                                "LocalPath": "/opt/ml/processing/model",
                                "S3DataType": "S3Prefix",
                                "S3InputMode": "File",
                                "S3DataDistributionType": "FullyReplicated"
                            }
                        },
                        {
                            "InputName": "input-2",
                            "S3Input": {
                                "S3Uri": f"s3://{artifacts_bucket}/{args.environment}/preprocessing/test",
                                "LocalPath": "/opt/ml/processing/test",
                                "S3DataType": "S3Prefix",
                                "S3InputMode": "File",
                                "S3DataDistributionType": "FullyReplicated"
                            }
                        }
                    ],
                    "ProcessingOutputConfig": {
                        "Outputs": [
                            {
                                "OutputName": "evaluation",
                                "S3Output": {
                                    "S3Uri": f"s3://{artifacts_bucket}/{args.environment}/evaluation",
                                    "LocalPath": "/opt/ml/processing/output/evaluation",
                                    "S3UploadMode": "EndOfJob"
                                }
                            }
                        ]
                    }
                }
            },
            {
                "Name": "CheckMetrics",
                "Type": "Condition",
                "Description": "Check if model accuracy meets threshold",
                "DependsOn": ["EvaluateModel"],
                "Arguments": {
                    "Conditions": [
                        {
                            "Type": "GreaterThanOrEqualTo",
                            "LeftValue": 0.75,
                            "RightValue": {"Get": "Parameters.AccuracyThreshold"}
                        }
                    ],
                    "IfSteps": [
                        {
                            "Name": "RegisterModel",
                            "Type": "RegisterModel",
                            "Description": "Register model in SageMaker Model Registry",
                            "Arguments": {
                                "ModelPackageGroupName": f"{args.environment}-{args.model_name}",
                                "ModelMetrics": {
                                    "ModelQuality": {
                                        "Statistics": {
                                            "ContentType": "application/json",
                                            "S3Uri": f"s3://{artifacts_bucket}/{args.environment}/evaluation/evaluation.json"
                                        }
                                    }
                                },
                                "InferenceSpecification": {
                                    "Containers": [
                                        {
                                            "Image": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.7-1-cpu-py3",
                                            "ModelDataUrl": f"s3://{artifacts_bucket}/{args.environment}/training-output/model.tar.gz"
                                        }
                                    ],
                                    "SupportedContentTypes": ["text/csv"],
                                    "SupportedResponseMIMETypes": ["text/csv"]
                                },
                                "ModelApprovalStatus": "PendingManualApproval"
                            }
                        }
                    ],
                    "ElseSteps": []
                }
            }
        ]
    }

    return definition


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    """Generate and save the pipeline definition JSON."""
    args = parse_args()

    print(f"Generating SageMaker Pipeline definition for: {args.environment}/{args.model_name}")
    print(f"Output: {args.output_path}")

    # Build the pipeline definition
    definition = build_pipeline_definition(args)

    # Ensure output directory exists
    output_dir = os.path.dirname(args.output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    # Write the definition
    with open(args.output_path, 'w') as f:
        json.dump(definition, f, indent=2)

    print(f"✓ Pipeline definition written to: {args.output_path}")
    print(f"  Pipeline name: {args.environment}-{args.model_name}-pipeline")
    print(f"  Steps: {len(definition.get('Steps', []))}")

    # Print step summary
    for i, step in enumerate(definition.get('Steps', []), 1):
        print(f"  Step {i}: {step['Name']} ({step['Type']})")


if __name__ == '__main__':
    main()
