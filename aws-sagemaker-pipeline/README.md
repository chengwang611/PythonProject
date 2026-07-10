# AWS SageMaker ML Pipeline - Automated ML Workflow

An end-to-end **machine learning pipeline** on AWS SageMaker that automatically retrains models when new data arrives. Triggered by S3 events → SQS → Lambda → SageMaker Pipeline.

## Architecture

```
┌─────────────────┐     S3 Event     ┌──────────────────┐     SQS Message     ┌──────────────────────┐
│  S3 Data Bucket │ ───────────────► │   SQS Queue      │ ──────────────────► │    Lambda            │
│ (Training Data) │                  │ (Trigger Queue)  │                     │  (Pipeline Trigger)  │
└─────────────────┘                  └──────────────────┘                     └──────────┬───────────┘
                                                                                         │
                                                                                         │ StartPipelineExecution
                                                                                         ▼
                                                                              ┌──────────────────────┐
                                                                              │  SageMaker Pipeline   │
                                                                              │                      │
                                                                              │  ┌────────────────┐  │
                                                                              │  │ PreprocessData │  │
                                                                              │  │ (SKLearn)      │  │
                                                                              │  └───────┬────────┘  │
                                                                              │          │           │
                                                                              │  ┌───────▼────────┐  │
                                                                              │  │  TrainModel    │  │
                                                                              │  │  (XGBoost)     │  │
                                                                              │  └───────┬────────┘  │
                                                                              │          │           │
                                                                              │  ┌───────▼────────┐  │
                                                                              │  │ EvaluateModel  │  │
                                                                              │  └───────┬────────┘  │
                                                                              │          │           │
                                                                              │  ┌───────▼────────┐  │
                                                                              │  │  CheckMetrics  │  │
                                                                              │  │  (Condition)   │  │
                                                                              │  └───┬───────┬───┘  │
                                                                              │  Pass│       │Fail │
                                                                              │  ┌───▼───┐   │     │
                                                                              │  │Register│   │     │
                                                                              │  │ Model  │   └─────┤
                                                                              │  └───────┘         │
                                                                              └──────────────────────┘
```

### Pipeline Steps

| Step | Type | Description |
|------|------|-------------|
| **PreprocessData** | Processing (SKLearn) | Load CSV, clean missing values, encode categories, split into train/val/test |
| **TrainModel** | Training (XGBoost) | Train XGBoost classifier with hyperparameters, early stopping |
| **EvaluateModel** | Processing (Script) | Compute accuracy, precision, recall, F1, ROC-AUC on test set |
| **CheckMetrics** | Condition | If accuracy >= threshold (default: 75%) → register model |
| **RegisterModel** | RegisterModel | Register in SageMaker Model Registry with `PendingManualApproval` |

## Project Structure

```
aws-sagemaker-pipeline/
├── .github/workflows/
│   └── deploy-sagemaker-pipeline.yml   # GitHub Actions CI/CD
├── cloudformation/
│   └── main-stack.yaml                 # Main CloudFormation stack
├── pipeline/
│   ├── pipeline_definition.py          # Pipeline definition generator
│   ├── pipeline-definition.json        # Generated pipeline JSON (gitignored)
│   └── scripts/
│       ├── preprocess.py               # Data preprocessing script
│       ├── train.py                    # XGBoost training script
│       └── evaluate.py                 # Model evaluation script
├── lambda/
│   ├── sagemaker_trigger.py            # Lambda function (SQS → Pipeline)
│   └── requirements.txt                # Lambda dependencies
├── scripts/
│   └── deploy.sh                       # One-command deployment script
├── config.example.yaml                 # Example configuration
├── Makefile                            # Local development commands
└── README.md                           # This file
```

## Prerequisites

- **AWS CLI** installed and configured (`aws configure`)
- **Python 3.12+** with `pip`
- **AWS Account** with permissions to create:
  - S3 buckets, SQS queues, Lambda functions, SageMaker pipelines
  - IAM roles and policies
  - CloudFormation stacks

## Build and Deploy on AWS

### Step 1: Clone and configure

```bash
cd aws-sagemaker-pipeline
cp config.example.yaml config.dev.yaml
# Edit config.dev.yaml with your values
```

### Step 2: Generate the pipeline definition

The SageMaker pipeline definition is generated from Python code:

```bash
make generate-pipeline ENVIRONMENT=dev MODEL_NAME=customer-churn-model
# Or manually:
pip install sagemaker
python pipeline/pipeline_definition.py \
    --environment dev \
    --model-name customer-churn-model \
    --output-path pipeline/pipeline-definition.json
```

This generates `pipeline/pipeline-definition.json` with all 5 pipeline steps.

### Step 3: Build the Lambda package

```bash
make package-lambda ENVIRONMENT=dev
# Or manually:
cd lambda
pip install -r requirements.txt -t ./package
cp sagemaker_trigger.py ./package/
cd package && zip -r9 ../sagemaker-trigger-dev.zip . && cd ..
rm -rf package
cd ..
```

### Step 4: Create artifact buckets and upload

```bash
export AWS_REGION=us-east-1
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# Create buckets
for bucket in "my-sagemaker-lambda-artifacts" "my-sagemaker-artifacts" "my-sagemaker-cfn-templates"; do
  aws s3 mb "s3://${bucket}" --region "$AWS_REGION" 2>/dev/null || true
done

# Upload Lambda
aws s3 cp lambda/sagemaker-trigger-dev.zip \
  s3://my-sagemaker-lambda-artifacts/lambda/sagemaker-trigger-dev.zip

# Upload pipeline definition and scripts
aws s3 cp pipeline/pipeline-definition.json \
  s3://my-sagemaker-artifacts/pipeline/pipeline-definition.json
aws s3 cp pipeline/scripts/preprocess.py \
  s3://my-sagemaker-artifacts/pipeline/scripts/preprocess.py
aws s3 cp pipeline/scripts/train.py \
  s3://my-sagemaker-artifacts/pipeline/scripts/train.py
aws s3 cp pipeline/scripts/evaluate.py \
  s3://my-sagemaker-artifacts/pipeline/scripts/evaluate.py

# Upload CloudFormation template
aws s3 cp cloudformation/main-stack.yaml \
  s3://my-sagemaker-cfn-templates/sagemaker-pipeline/main-stack.yaml
```

### Step 5: Deploy CloudFormation

```bash
aws cloudformation deploy \
  --template-file cloudformation/main-stack.yaml \
  --stack-name sagemaker-pipeline-dev \
  --capabilities CAPABILITY_NAMED_IAM CAPABILITY_IAM \
  --parameter-overrides \
    EnvironmentName=dev \
    ModelName=customer-churn-model \
    LambdaS3Bucket=my-sagemaker-lambda-artifacts \
    PipelineArtifactsBucket=my-sagemaker-artifacts \
    DataBucketName=my-sagemaker-data \
    GithubRepoOwner=your-github-username \
    GithubRepoName=your-repo-name
```

### Step 6: Test the pipeline

```bash
# Get the data bucket name
DATA_BUCKET=$(aws cloudformation describe-stacks --stack-name sagemaker-pipeline-dev \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" --output text)

# Upload sample training data
python -c "
import pandas as pd
import numpy as np
np.random.seed(42)
n = 1000
data = pd.DataFrame({
    'feature1': np.random.normal(0, 1, n),
    'feature2': np.random.normal(0, 1, n),
    'feature3': np.random.normal(0, 1, n),
    'target': np.random.binomial(1, 0.3, n),
})
data.to_csv('/tmp/training-data.csv', index=False)
"
aws s3 cp /tmp/training-data.csv "s3://${DATA_BUCKET}/training/training-data.csv"

# Watch Lambda logs
aws logs tail /aws/lambda/dev-sagemaker-trigger --follow

# Monitor pipeline execution
aws sagemaker list-pipeline-executions \
  --pipeline-name dev-customer-churn-model-pipeline \
  --query "PipelineExecutionSummaries[0:5].[PipelineExecutionArn,PipelineExecutionStatus,StartTime]" \
  --output table
```

## Alternative: One-Command Deployment

```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh dev
```

## Alternative: Deploy with Make

```bash
export AWS_REGION=us-east-1
export LAMBDA_BUCKET=my-sagemaker-lambda-artifacts
export PIPELINE_ARTIFACTS_BUCKET=my-sagemaker-artifacts
export DATA_BUCKET_NAME=my-sagemaker-data
export GITHUB_OWNER=your-github-username
export GITHUB_REPO=your-repo-name

make deploy ENVIRONMENT=dev
```

## GitHub Actions CI/CD

### Set up GitHub OIDC

1. Deploy the stack once manually (see above)
2. The stack creates an IAM role `{env}-github-deploy-role` for GitHub OIDC
3. In your GitHub repository, add these **secrets**:

| Secret | Description |
|--------|-------------|
| `DEPLOY_ROLE_ARN` | ARN of the GitHub deploy role (from stack outputs) |
| `LAMBDA_ARTIFACTS_BUCKET` | S3 bucket for Lambda ZIPs |
| `PIPELINE_ARTIFACTS_BUCKET` | S3 bucket for pipeline artifacts |
| `DATA_BUCKET_NAME` | Name prefix for data bucket |
| `CFN_TEMPLATES_BUCKET` | S3 bucket for CloudFormation templates |

Push to `main` to trigger automated validation, pipeline generation, packaging, and deployment.

## CloudFormation Stack Details

The stack creates:

| Resource | Description |
|----------|-------------|
| **S3 Buckets** | Data (training data upload), Artifacts (pipeline definition + scripts) |
| **SQS Queue** | Training trigger queue with DLQ |
| **Lambda Function** | SQS consumer that starts pipeline executions |
| **SageMaker Pipeline** | ML workflow: preprocessing → training → evaluation → registration |
| **IAM Roles** | Lambda execution role, SageMaker execution role, GitHub OIDC deploy role |
| **CloudWatch Logs** | Lambda log group |

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `EnvironmentName` | `dev` | Environment (dev/staging/prod) |
| `ModelName` | `customer-churn-model` | ML model name |
| `TrainingInstanceType` | `ml.m5.xlarge` | Training instance type |
| `TrainingInstanceCount` | `1` | Number of training instances |
| `ProcessingInstanceType` | `ml.m5.xlarge` | Processing instance type |
| `InferenceInstanceType` | `ml.m5.large` | Inference endpoint instance type |
| `LambdaMemorySize` | `256` | Lambda memory (MB) |
| `LambdaTimeout` | `120` | Lambda timeout (seconds) |

## Pipeline Definition

The pipeline is defined in [`pipeline/pipeline_definition.py`](aws-sagemaker-pipeline/pipeline/pipeline_definition.py) using the SageMaker SDK v2+ Pipeline API.

### How It Works

1. **SDK Mode** (default): Uses `sagemaker.workflow.pipeline.Pipeline` to build the DAG programmatically, then calls `pipeline.definition()` to generate JSON.

2. **Static Fallback**: If the SageMaker SDK is not installed, generates the same pipeline as raw JSON. This ensures CI/CD environments without the SDK can still produce valid definitions.

### Pipeline Parameters

These can be overridden at execution time:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ProcessingInstanceType` | `ml.m5.xlarge` | Instance for preprocessing/evaluation |
| `TrainingInstanceType` | `ml.m5.xlarge` | Instance for training |
| `TrainingInstanceCount` | `1` | Number of training instances |
| `InputDataUrl` | S3 path | Location of training data |
| `AccuracyThreshold` | `0.75` | Minimum accuracy to register model |

## Training Scripts

### Preprocessing ([`preprocess.py`](aws-sagemaker-pipeline/pipeline/scripts/preprocess.py))

- Loads CSV data from S3
- Drops duplicates, fills missing values
- Label encodes categorical features
- Splits into train (70%), validation (10%), test (20%)
- Saves as CSV (target as first column for XGBoost)

### Training ([`train.py`](aws-sagemaker-pipeline/pipeline/scripts/train.py))

- Trains XGBoost classifier with configurable hyperparameters
- Uses early stopping on validation set
- Saves model as `model.xgb` + `metadata.json`

### Evaluation ([`evaluate.py`](aws-sagemaker-pipeline/pipeline/scripts/evaluate.py))

- Loads trained model from `model.tar.gz`
- Computes: accuracy, precision, recall, F1, ROC-AUC, confusion matrix
- Saves `evaluation.json` for the ConditionStep and Model Registry

## Model Registry

When the model passes the accuracy threshold, it's registered in SageMaker Model Registry:

```
SageMaker → Model Registry → dev-customer-churn-model
  ├── Version 1 (PendingManualApproval)
  ├── Version 2 (PendingManualApproval)
  └── ...
```

From the registry, you can:
- **Approve** a model version for production deployment
- **Deploy** to a SageMaker endpoint
- **Set up** automatic deployment pipelines

## Monitoring

```bash
# Lambda logs
aws logs tail /aws/lambda/dev-sagemaker-trigger --follow

# Pipeline executions
aws sagemaker list-pipeline-executions \
  --pipeline-name dev-customer-churn-model-pipeline \
  --query "PipelineExecutionSummaries[0:5].[PipelineExecutionArn,PipelineExecutionStatus,StartTime]" \
  --output table

# Pipeline execution details
aws sagemaker describe-pipeline-execution \
  --pipeline-execution-arn <arn>

# Pipeline execution steps
aws sagemaker list-pipeline-execution-steps \
  --pipeline-execution-arn <arn> \
  --query "PipelineExecutionSteps[0:5].[StepName,StepStatus,StartTime,EndTime]" \
  --output table

# Model Registry
aws sagemaker list-model-packages \
  --model-package-group-name dev-customer-churn-model \
  --query "ModelPackageSummaryList[0:5].[ModelPackageArn,ModelApprovalStatus,CreationTime]" \
  --output table
```

## Clean Up

```bash
# Delete the CloudFormation stack
aws cloudformation delete-stack --stack-name sagemaker-pipeline-dev
aws cloudformation wait stack-delete-complete --stack-name sagemaker-pipeline-dev

# Optionally delete artifact buckets
aws s3 rb "s3://my-sagemaker-lambda-artifacts" --force
aws s3 rb "s3://my-sagemaker-artifacts" --force
aws s3 rb "s3://my-sagemaker-cfn-templates" --force
```

## Cost Considerations

| Resource | Cost | Notes |
|----------|------|-------|
| **SageMaker Processing** | ~$0.10-0.50 per run | Preprocessing + evaluation |
| **SageMaker Training** | ~$0.20-1.00 per run | XGBoost on ml.m5.xlarge |
| **SageMaker Pipeline** | Free | No additional cost for pipeline orchestration |
| **Lambda** | ~$0.000001 per invocation | Negligible |
| **SQS** | ~$0.0000004 per message | Negligible |
| **S3 Storage** | ~$0.023/GB/month | Training data + artifacts |
| **Total per training run** | **~$0.30-1.50** | Depends on data size and instance type |

## License

MIT
