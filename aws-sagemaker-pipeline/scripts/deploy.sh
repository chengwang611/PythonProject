#!/usr/bin/env bash
# ============================================================================
# deploy.sh - Deploy the AWS SageMaker ML Pipeline
# ============================================================================
# Usage:
#   ./scripts/deploy.sh dev          # Deploy to dev
#   ./scripts/deploy.sh staging      # Deploy to staging
#   ./scripts/deploy.sh prod         # Deploy to prod
#
# This script:
#   1. Validates CloudFormation + Python syntax
#   2. Generates the SageMaker pipeline definition JSON
#   3. Packages the Lambda function
#   4. Uploads all artifacts to S3
#   5. Deploys the CloudFormation stack
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

ENVIRONMENT="${1:-dev}"
MODEL_NAME="${MODEL_NAME:-customer-churn-model}"
AWS_REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="sagemaker-pipeline-${ENVIRONMENT}"
CONFIG_FILE="config.${ENVIRONMENT}.yaml"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
info "Deploying SageMaker ML Pipeline to environment: ${ENVIRONMENT}"

command -v aws >/dev/null 2>&1 || error "AWS CLI is required"
command -v python3 >/dev/null 2>&1 || error "Python 3 is required"

aws sts get-caller-identity >/dev/null 2>&1 || error "AWS credentials not configured"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
info "AWS Account: ${ACCOUNT_ID} | Region: ${AWS_REGION}"

# ---------------------------------------------------------------------------
# Step 1: Validate
# ---------------------------------------------------------------------------
info "Step 1/6: Validating templates..."
pip install -q cfn-lint sagemaker 2>/dev/null || true
cfn-lint cloudformation/main-stack.yaml || warn "cfn-lint issues (non-fatal)"
python3 -m py_compile lambda/sagemaker_trigger.py
python3 -m py_compile pipeline/scripts/preprocess.py
python3 -m py_compile pipeline/scripts/train.py
python3 -m py_compile pipeline/scripts/evaluate.py

# ---------------------------------------------------------------------------
# Step 2: Generate pipeline definition
# ---------------------------------------------------------------------------
info "Step 2/6: Generating SageMaker pipeline definition..."
python3 pipeline/pipeline_definition.py \
    --environment "$ENVIRONMENT" \
    --model-name "$MODEL_NAME" \
    --output-path pipeline/pipeline-definition.json

# ---------------------------------------------------------------------------
# Step 3: Package Lambda
# ---------------------------------------------------------------------------
info "Step 3/6: Packaging Lambda function..."
cd lambda
pip install -q -r requirements.txt -t ./package
cp sagemaker_trigger.py ./package/
cd package
zip -r9 "../sagemaker-trigger-${ENVIRONMENT}.zip" . >/dev/null
cd ..
rm -rf package
cd ..
info "Lambda packaged: lambda/sagemaker-trigger-${ENVIRONMENT}.zip"

# ---------------------------------------------------------------------------
# Step 4: Upload artifacts
# ---------------------------------------------------------------------------
info "Step 4/6: Uploading artifacts to S3..."

LAMBDA_BUCKET="${LAMBDA_ARTIFACTS_BUCKET:-sagemaker-pipeline-lambda-artifacts}"
PIPELINE_ARTIFACTS_BUCKET="${PIPELINE_ARTIFACTS_BUCKET:-sagemaker-pipeline-artifacts}"
DATA_BUCKET_NAME="${DATA_BUCKET_NAME:-sagemaker-pipeline-data}"
CFN_TEMPLATES_BUCKET="${CFN_TEMPLATES_BUCKET:-sagemaker-pipeline-cfn-templates}"

for bucket in "$LAMBDA_BUCKET" "$PIPELINE_ARTIFACTS_BUCKET" "$CFN_TEMPLATES_BUCKET"; do
    if ! aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
        info "Creating bucket: ${bucket}"
        aws s3 mb "s3://${bucket}" --region "$AWS_REGION"
    fi
done

# Upload Lambda
aws s3 cp "lambda/sagemaker-trigger-${ENVIRONMENT}.zip" \
    "s3://${LAMBDA_BUCKET}/lambda/sagemaker-trigger-${ENVIRONMENT}.zip"

# Upload pipeline definition
aws s3 cp pipeline/pipeline-definition.json \
    "s3://${PIPELINE_ARTIFACTS_BUCKET}/pipeline/pipeline-definition.json"

# Upload pipeline scripts
aws s3 cp pipeline/scripts/preprocess.py \
    "s3://${PIPELINE_ARTIFACTS_BUCKET}/pipeline/scripts/preprocess.py"
aws s3 cp pipeline/scripts/train.py \
    "s3://${PIPELINE_ARTIFACTS_BUCKET}/pipeline/scripts/train.py"
aws s3 cp pipeline/scripts/evaluate.py \
    "s3://${PIPELINE_ARTIFACTS_BUCKET}/pipeline/scripts/evaluate.py"

# Upload CloudFormation template
aws s3 cp cloudformation/main-stack.yaml \
    "s3://${CFN_TEMPLATES_BUCKET}/sagemaker-pipeline/main-stack.yaml"

# ---------------------------------------------------------------------------
# Step 5: Deploy CloudFormation
# ---------------------------------------------------------------------------
info "Step 5/6: Deploying CloudFormation stack..."

GITHUB_REPO_OWNER="${GITHUB_REPO_OWNER:-your-org}"
GITHUB_REPO_NAME="${GITHUB_REPO_NAME:-your-repo}"

aws cloudformation deploy \
    --template-file cloudformation/main-stack.yaml \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_NAMED_IAM CAPABILITY_IAM \
    --region "$AWS_REGION" \
    --parameter-overrides \
        EnvironmentName="$ENVIRONMENT" \
        ModelName="$MODEL_NAME" \
        LambdaS3Bucket="$LAMBDA_BUCKET" \
        PipelineArtifactsBucket="$PIPELINE_ARTIFACTS_BUCKET" \
        DataBucketName="$DATA_BUCKET_NAME" \
        GithubRepoOwner="$GITHUB_REPO_OWNER" \
        GithubRepoName="$GITHUB_REPO_NAME" \
        TrainingInstanceType="${TRAINING_INSTANCE_TYPE:-ml.m5.xlarge}" \
        TrainingInstanceCount="${TRAINING_INSTANCE_COUNT:-1}" \
        ProcessingInstanceType="${PROCESSING_INSTANCE_TYPE:-ml.m5.xlarge}" \
        InferenceInstanceType="${INFERENCE_INSTANCE_TYPE:-ml.m5.large}" \
        InferenceInstanceCount="${INFERENCE_INSTANCE_COUNT:-1}" \
        LambdaMemorySize="${LAMBDA_MEMORY_SIZE:-256}" \
        LambdaTimeout="${LAMBDA_TIMEOUT:-120}" \
        LogRetentionDays="${LOG_RETENTION_DAYS:-30}" \
        SqsVisibilityTimeout="${SQS_VISIBILITY_TIMEOUT:-300}" \
    --tags \
        Environment="$ENVIRONMENT" \
        Project="SageMaker-ML-Pipeline" \
        ManagedBy="deploy.sh"

# ---------------------------------------------------------------------------
# Step 6: Verify
# ---------------------------------------------------------------------------
info "Step 6/6: Verifying deployment..."

echo ""
echo "============================================"
echo "  SageMaker ML Pipeline Deployment Summary"
echo "  Environment: ${ENVIRONMENT}"
echo "  Stack: ${STACK_NAME}"
echo "============================================"

aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$AWS_REGION" \
    --query "Stacks[0].Outputs" \
    --output table

echo ""
info "Deployment complete!"
echo ""
echo "Next steps:"
echo "  1. Upload training data: aws s3 cp data.csv s3://${DATA_BUCKET_NAME}-${ACCOUNT_ID}-${ENVIRONMENT}/training/"
echo "  2. Check Lambda logs: aws logs tail /aws/lambda/${ENVIRONMENT}-sagemaker-trigger --follow"
echo "  3. Monitor pipeline: aws sagemaker list-pipeline-executions --pipeline-name ${ENVIRONMENT}-${MODEL_NAME}-pipeline"
echo "  4. View in console: https://${AWS_REGION}.console.aws.amazon.com/sagemaker/home?region=${AWS_REGION}#/pipelines"
echo ""
