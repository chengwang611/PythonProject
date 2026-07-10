# Makefile for AWS SageMaker ML Pipeline
# Provides local development and deployment commands

.PHONY: help validate generate-pipeline package deploy destroy clean

SHELL := /bin/bash
ENVIRONMENT ?= dev
MODEL_NAME ?= customer-churn-model
AWS_REGION ?= us-east-1

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ============================================================================
# Validation
# ============================================================================

validate: validate-cfn validate-lambda validate-scripts validate-pipeline ## Run all validations

validate-cfn: ## Validate CloudFormation templates
	@echo "Validating CloudFormation templates..."
	@pip install -q cfn-lint
	@cfn-lint cloudformation/*.yaml
	@echo "✓ CloudFormation templates valid"

validate-lambda: ## Validate Lambda Python syntax
	@echo "Validating Lambda code..."
	@python -m py_compile lambda/sagemaker_trigger.py
	@echo "✓ Lambda code valid"

validate-scripts: ## Validate pipeline scripts
	@echo "Validating pipeline scripts..."
	@python -m py_compile pipeline/scripts/preprocess.py
	@python -m py_compile pipeline/scripts/train.py
	@python -m py_compile pipeline/scripts/evaluate.py
	@echo "✓ Pipeline scripts valid"

validate-pipeline: ## Generate and validate pipeline definition
	@echo "Generating pipeline definition..."
	@pip install -q sagemaker 2>/dev/null || true
	@python pipeline/pipeline_definition.py \
		--environment $(ENVIRONMENT) \
		--model-name $(MODEL_NAME) \
		--output-path pipeline/pipeline-definition.json
	@echo "✓ Pipeline definition generated"

# ============================================================================
# Pipeline Definition Generation
# ============================================================================

generate-pipeline: ## Generate the SageMaker pipeline definition JSON
	@echo "Generating SageMaker pipeline definition..."
	@pip install -q sagemaker 2>/dev/null || true
	@python pipeline/pipeline_definition.py \
		--environment $(ENVIRONMENT) \
		--model-name $(MODEL_NAME) \
		--output-path pipeline/pipeline-definition.json
	@echo "✓ Pipeline definition: pipeline/pipeline-definition.json"

# ============================================================================
# Packaging
# ============================================================================

package: package-lambda package-pipeline ## Package all artifacts

package-lambda: ## Package Lambda function into ZIP
	@echo "Packaging Lambda function..."
	@cd lambda && \
		pip install -r requirements.txt -t ./package && \
		cp sagemaker_trigger.py ./package/ && \
		cd package && \
		zip -r9 ../sagemaker-trigger-$(ENVIRONMENT).zip . && \
		cd .. && \
		rm -rf package
	@echo "✓ Lambda packaged: lambda/sagemaker-trigger-$(ENVIRONMENT).zip"

package-pipeline: generate-pipeline ## Package pipeline artifacts
	@echo "✓ Pipeline definition ready: pipeline/pipeline-definition.json"
	@echo "✓ Pipeline scripts ready: pipeline/scripts/"

# ============================================================================
# Deployment
# ============================================================================

deploy: validate package ## Deploy full stack to AWS
	@echo "Deploying SageMaker ML Pipeline to $(ENVIRONMENT)..."
	@aws cloudformation deploy \
		--template-file cloudformation/main-stack.yaml \
		--stack-name sagemaker-pipeline-$(ENVIRONMENT) \
		--capabilities CAPABILITY_NAMED_IAM CAPABILITY_IAM \
		--parameter-overrides \
			EnvironmentName=$(ENVIRONMENT) \
			ModelName=$(MODEL_NAME) \
			LambdaS3Bucket=$(LAMBDA_BUCKET) \
			PipelineArtifactsBucket=$(PIPELINE_ARTIFACTS_BUCKET) \
			DataBucketName=$(DATA_BUCKET_NAME) \
			GithubRepoOwner=$(GITHUB_OWNER) \
			GithubRepoName=$(GITHUB_REPO) \
		--tags \
			Environment=$(ENVIRONMENT) \
			Project=SageMaker-ML-Pipeline \
			ManagedBy=Makefile
	@echo "✓ Deployment complete"

deploy-lambda: package-lambda ## Deploy only the Lambda function
	@echo "Uploading Lambda ZIP to S3..."
	@aws s3 cp lambda/sagemaker-trigger-$(ENVIRONMENT).zip \
		s3://$(LAMBDA_BUCKET)/lambda/sagemaker-trigger-$(ENVIRONMENT).zip
	@echo "Updating Lambda function code..."
	@aws lambda update-function-code \
		--function-name $(ENVIRONMENT)-sagemaker-trigger \
		--s3-bucket $(LAMBDA_BUCKET) \
		--s3-key lambda/sagemaker-trigger-$(ENVIRONMENT).zip
	@echo "✓ Lambda deployed"

deploy-pipeline: generate-pipeline ## Deploy pipeline definition and scripts to S3
	@echo "Uploading pipeline artifacts..."
	@aws s3 cp pipeline/pipeline-definition.json \
		s3://$(PIPELINE_ARTIFACTS_BUCKET)/pipeline/pipeline-definition.json
	@aws s3 cp pipeline/scripts/preprocess.py \
		s3://$(PIPELINE_ARTIFACTS_BUCKET)/pipeline/scripts/preprocess.py
	@aws s3 cp pipeline/scripts/train.py \
		s3://$(PIPELINE_ARTIFACTS_BUCKET)/pipeline/scripts/train.py
	@aws s3 cp pipeline/scripts/evaluate.py \
		s3://$(PIPELINE_ARTIFACTS_BUCKET)/pipeline/scripts/evaluate.py
	@echo "✓ Pipeline artifacts deployed"

# ============================================================================
# Destroy
# ============================================================================

destroy: ## Destroy the CloudFormation stack
	@echo "WARNING: This will delete the stack sagemaker-pipeline-$(ENVIRONMENT)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
		echo; \
		if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
			aws cloudformation delete-stack \
				--stack-name sagemaker-pipeline-$(ENVIRONMENT); \
			aws cloudformation wait stack-delete-complete \
				--stack-name sagemaker-pipeline-$(ENVIRONMENT); \
			echo "✓ Stack deleted"; \
		else \
			echo "Aborted"; \
		fi

# ============================================================================
# Utility
# ============================================================================

outputs: ## Show CloudFormation stack outputs
	@aws cloudformation describe-stacks \
		--stack-name sagemaker-pipeline-$(ENVIRONMENT) \
		--query "Stacks[0].Outputs" \
		--output table

events: ## Show recent CloudFormation stack events
	@aws cloudformation describe-stack-events \
		--stack-name sagemaker-pipeline-$(ENVIRONMENT) \
		--query "StackEvents[0:20].[Timestamp,ResourceStatus,ResourceType,LogicalResourceId]" \
		--output table

test: ## Test the pipeline by uploading sample training data
	@echo "Uploading sample training data to trigger pipeline..."
	@echo "feature1,feature2,feature3,target" > /tmp/sample-training.csv
	@echo "1.2,3.4,5.6,0" >> /tmp/sample-training.csv
	@echo "2.3,4.5,6.7,1" >> /tmp/sample-training.csv
	@echo "3.4,5.6,7.8,0" >> /tmp/sample-training.csv
	@aws s3 cp /tmp/sample-training.csv \
		s3://$(DATA_BUCKET_NAME)-$(AWS_ACCOUNT_ID)-$(ENVIRONMENT)/training/sample-training.csv
	@rm /tmp/sample-training.csv
	@echo "✓ Sample data uploaded. Check Lambda logs for pipeline trigger."

logs: ## Tail Lambda function logs
	@aws logs tail /aws/lambda/$(ENVIRONMENT)-sagemaker-trigger --follow

pipeline-runs: ## List recent SageMaker pipeline executions
	@aws sagemaker list-pipeline-executions \
		--pipeline-name $(ENVIRONMENT)-$(MODEL_NAME)-pipeline \
		--max-results 10 \
		--query "PipelineExecutionSummaries[0:5].[PipelineExecutionArn,PipelineExecutionStatus,StartTime]" \
		--output table

clean: ## Clean build artifacts
	@rm -f lambda/sagemaker-trigger-*.zip
	@rm -f pipeline/pipeline-definition.json
	@echo "✓ Cleaned build artifacts"
