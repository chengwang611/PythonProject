#!/usr/bin/env bash
# ============================================================================
# deploy.sh - Deploy the AWS ETL Pipeline
# ============================================================================
# Usage:
#   ./scripts/deploy.sh dev          # Deploy to dev
#   ./scripts/deploy.sh staging      # Deploy to staging
#   ./scripts/deploy.sh prod         # Deploy to prod
#
# Prerequisites:
#   - AWS CLI installed and configured
#   - jq installed (for JSON parsing)
#   - Python 3.12+ with pip
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ENVIRONMENT="${1:-dev}"
AWS_REGION="${AWS_REGION:-us-east-1}"
STACK_NAME="etl-pipeline-${ENVIRONMENT}"
CONFIG_FILE="config.${ENVIRONMENT}.yaml"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
info "Deploying ETL Pipeline to environment: ${ENVIRONMENT}"

command -v aws >/dev/null 2>&1 || error "AWS CLI is required but not installed"
command -v python3 >/dev/null 2>&1 || error "Python 3 is required but not installed"
command -v jq >/dev/null 2>&1 || warn "jq is recommended but not installed"

# Check AWS credentials
aws sts get-caller-identity >/dev/null 2>&1 || error "AWS credentials not configured"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
info "AWS Account: ${ACCOUNT_ID}"
info "Region: ${AWS_REGION}"

# ---------------------------------------------------------------------------
# Load configuration
# ---------------------------------------------------------------------------
if [ -f "$CONFIG_FILE" ]; then
    info "Loading configuration from ${CONFIG_FILE}"
    # Simple YAML parser (bash-friendly)
    eval $(python3 -c "
import yaml
with open('${CONFIG_FILE}') as f:
    cfg = yaml.safe_load(f)
for k, v in cfg.items():
    if isinstance(v, str):
        print(f'{k.upper()}=\"{v}\"')
    elif isinstance(v, (int, float)):
        print(f'{k.upper()}={v}')
    elif v is None:
        print(f'{k.upper()}=\"\"')
")
else
    warn "No config file found at ${CONFIG_FILE}, using defaults"
    warn "Copy config.example.yaml to ${CONFIG_FILE} and customize"
fi

# ---------------------------------------------------------------------------
# Step 1: Validate templates
# ---------------------------------------------------------------------------
info "Step 1/5: Validating CloudFormation templates..."
pip install -q cfn-lint 2>/dev/null || true
cfn-lint cloudformation/main-stack.yaml || warn "cfn-lint found issues (non-fatal)"

info "Validating Lambda code..."
python3 -m py_compile lambda/emr_trigger.py

info "Validating PySpark job..."
python3 -m py_compile emr-jobs/csv_etl_job.py

# ---------------------------------------------------------------------------
# Step 2: Package Lambda
# ---------------------------------------------------------------------------
info "Step 2/5: Packaging Lambda function..."
cd lambda
pip install -q -r requirements.txt -t ./package
cp emr_trigger.py ./package/
cd package
zip -r9 "../emr-trigger-${ENVIRONMENT}.zip" . >/dev/null
cd ..
rm -rf package
cd ..
info "Lambda packaged: lambda/emr-trigger-${ENVIRONMENT}.zip"

# ---------------------------------------------------------------------------
# Step 3: Upload artifacts to S3
# ---------------------------------------------------------------------------
info "Step 3/5: Uploading artifacts to S3..."

# Determine bucket names (use config values or defaults)
LAMBDA_BUCKET="${LAMBDA_ARTIFACTS_BUCKET:-etl-pipeline-lambda-artifacts}"
EMR_ARTIFACTS_BUCKET="${EMR_ARTIFACTS_BUCKET:-etl-pipeline-emr-artifacts}"
DATA_BUCKET_NAME="${DATA_BUCKET_NAME:-etl-pipeline-data}"
PROCESSED_BUCKET_NAME="${PROCESSED_BUCKET_NAME:-etl-pipeline-processed}"
EMR_LOG_BUCKET="${EMR_LOG_BUCKET:-etl-pipeline-emr-logs}"
CFN_TEMPLATES_BUCKET="${CFN_TEMPLATES_BUCKET:-etl-pipeline-cfn-templates}"

# Create buckets if they don't exist (for first-time deployment)
for bucket in "$LAMBDA_BUCKET" "$EMR_ARTIFACTS_BUCKET" "$CFN_TEMPLATES_BUCKET"; do
    if ! aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
        info "Creating bucket: ${bucket}"
        aws s3 mb "s3://${bucket}" --region "$AWS_REGION"
    fi
done

# Upload Lambda ZIP
aws s3 cp "lambda/emr-trigger-${ENVIRONMENT}.zip" \
    "s3://${LAMBDA_BUCKET}/lambda/emr-trigger-${ENVIRONMENT}.zip"

# Upload PySpark job
aws s3 cp emr-jobs/csv_etl_job.py \
    "s3://${EMR_ARTIFACTS_BUCKET}/emr-jobs/csv_etl_job.py"

# Upload CloudFormation template
aws s3 cp cloudformation/main-stack.yaml \
    "s3://${CFN_TEMPLATES_BUCKET}/etl-pipeline/main-stack.yaml"

# ---------------------------------------------------------------------------
# Step 4: Deploy CloudFormation stack
# ---------------------------------------------------------------------------
info "Step 4/5: Deploying CloudFormation stack..."

GITHUB_REPO_OWNER="${GITHUB_REPO_OWNER:-your-org}"
GITHUB_REPO_NAME="${GITHUB_REPO_NAME:-your-repo}"

aws cloudformation deploy \
    --template-file cloudformation/main-stack.yaml \
    --stack-name "$STACK_NAME" \
    --capabilities CAPABILITY_NAMED_IAM CAPABILITY_IAM \
    --region "$AWS_REGION" \
    --parameter-overrides \
        EnvironmentName="$ENVIRONMENT" \
        LambdaS3Bucket="$LAMBDA_BUCKET" \
        EmrLogBucket="$EMR_LOG_BUCKET" \
        EmrArtifactsBucket="$EMR_ARTIFACTS_BUCKET" \
        DataBucketName="$DATA_BUCKET_NAME" \
        ProcessedBucketName="$PROCESSED_BUCKET_NAME" \
        GithubRepoOwner="$GITHUB_REPO_OWNER" \
        GithubRepoName="$GITHUB_REPO_NAME" \
        VpcId="${VPC_ID:-}" \
        SubnetId="${SUBNET_ID:-}" \
        EmrReleaseLabel="${EMR_RELEASE_LABEL:-emr-7.5.0}" \
        EmrInstanceType="${EMR_CORE_INSTANCE_TYPE:-m5.xlarge}" \
        EmrCoreInstanceCount="${EMR_CORE_INSTANCE_COUNT:-2}" \
        LambdaMemorySize="${LAMBDA_MEMORY_SIZE:-256}" \
        LambdaTimeout="${LAMBDA_TIMEOUT:-120}" \
        LogRetentionDays="${LOG_RETENTION_DAYS:-30}" \
        SqsVisibilityTimeout="${SQS_VISIBILITY_TIMEOUT:-300}" \
        EmrJobTimeout="${EMR_JOB_TIMEOUT:-3600}" \
    --tags \
        Environment="$ENVIRONMENT" \
        Project="ETL-Pipeline" \
        ManagedBy="deploy.sh"

# ---------------------------------------------------------------------------
# Step 5: Verify deployment
# ---------------------------------------------------------------------------
info "Step 5/5: Verifying deployment..."

echo ""
echo "============================================"
echo "  ETL Pipeline Deployment Summary"
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
echo "  1. Upload a CSV file to the data bucket to trigger the pipeline"
echo "  2. Check Lambda logs: aws logs tail /aws/lambda/${ENVIRONMENT}-emr-trigger --follow"
echo "  3. Monitor EMR steps in the AWS Console"
echo ""
