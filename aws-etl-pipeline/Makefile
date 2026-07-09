# Makefile for AWS ETL Pipeline
# Provides local development and deployment commands

.PHONY: help validate package deploy destroy clean

SHELL := /bin/bash
ENVIRONMENT ?= dev
AWS_REGION ?= us-east-1

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ============================================================================
# Validation
# ============================================================================

validate: validate-cfn validate-lambda validate-pyspark ## Run all validations

validate-cfn: ## Validate CloudFormation templates
	@echo "Validating CloudFormation templates..."
	@pip install -q cfn-lint
	@cfn-lint cloudformation/*.yaml
	@echo "✓ CloudFormation templates valid"

validate-lambda: ## Validate Lambda Python syntax
	@echo "Validating Lambda code..."
	@python -m py_compile lambda/emr_trigger.py
	@echo "✓ Lambda code valid"

validate-pyspark: ## Validate PySpark job syntax
	@echo "Validating PySpark job..."
	@python -m py_compile emr-jobs/csv_etl_job.py
	@echo "✓ PySpark job valid"

# ============================================================================
# Packaging
# ============================================================================

package: package-lambda package-emr ## Package all artifacts

package-lambda: ## Package Lambda function into ZIP
	@echo "Packaging Lambda function..."
	@cd lambda && \
		pip install -r requirements.txt -t ./package && \
		cp emr_trigger.py ./package/ && \
		cd package && \
		zip -r9 ../emr-trigger-$(ENVIRONMENT).zip . && \
		cd .. && \
		rm -rf package
	@echo "✓ Lambda packaged: lambda/emr-trigger-$(ENVIRONMENT).zip"

package-emr: ## Package PySpark job (copy only, no ZIP needed)
	@echo "✓ PySpark job ready: emr-jobs/csv_etl_job.py"

# ============================================================================
# Deployment
# ============================================================================

deploy: validate package ## Deploy full stack to AWS
	@echo "Deploying ETL pipeline to $(ENVIRONMENT)..."
	@aws cloudformation deploy \
		--template-file cloudformation/main-stack.yaml \
		--stack-name etl-pipeline-$(ENVIRONMENT) \
		--capabilities CAPABILITY_NAMED_IAM CAPABILITY_IAM \
		--parameter-overrides \
			EnvironmentName=$(ENVIRONMENT) \
			LambdaS3Bucket=$(LAMBDA_BUCKET) \
			EmrLogBucket=$(EMR_LOG_BUCKET) \
			EmrArtifactsBucket=$(EMR_ARTIFACTS_BUCKET) \
			DataBucketName=$(DATA_BUCKET_NAME) \
			ProcessedBucketName=$(PROCESSED_BUCKET_NAME) \
			GithubRepoOwner=$(GITHUB_OWNER) \
			GithubRepoName=$(GITHUB_REPO) \
			VpcId=$(VPC_ID) \
			SubnetId=$(SUBNET_ID) \
		--tags \
			Environment=$(ENVIRONMENT) \
			Project=ETL-Pipeline \
			ManagedBy=Makefile
	@echo "✓ Deployment complete"

deploy-lambda: package-lambda ## Deploy only the Lambda function
	@echo "Uploading Lambda ZIP to S3..."
	@aws s3 cp lambda/emr-trigger-$(ENVIRONMENT).zip \
		s3://$(LAMBDA_BUCKET)/lambda/emr-trigger-$(ENVIRONMENT).zip
	@echo "Updating Lambda function code..."
	@aws lambda update-function-code \
		--function-name $(ENVIRONMENT)-emr-trigger \
		--s3-bucket $(LAMBDA_BUCKET) \
		--s3-key lambda/emr-trigger-$(ENVIRONMENT).zip
	@echo "✓ Lambda deployed"

deploy-emr-job: ## Deploy PySpark job to S3
	@echo "Uploading PySpark job..."
	@aws s3 cp emr-jobs/csv_etl_job.py \
		s3://$(EMR_ARTIFACTS_BUCKET)/emr-jobs/csv_etl_job.py
	@echo "✓ PySpark job deployed"

# ============================================================================
# Destroy
# ============================================================================

destroy: ## Destroy the CloudFormation stack
	@echo "WARNING: This will delete the stack etl-pipeline-$(ENVIRONMENT)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
		echo; \
		if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
			aws cloudformation delete-stack \
				--stack-name etl-pipeline-$(ENVIRONMENT); \
			aws cloudformation wait stack-delete-complete \
				--stack-name etl-pipeline-$(ENVIRONMENT); \
			echo "✓ Stack deleted"; \
		else \
			echo "Aborted"; \
		fi

# ============================================================================
# Utility
# ============================================================================

outputs: ## Show CloudFormation stack outputs
	@aws cloudformation describe-stacks \
		--stack-name etl-pipeline-$(ENVIRONMENT) \
		--query "Stacks[0].Outputs" \
		--output table

events: ## Show recent CloudFormation stack events
	@aws cloudformation describe-stack-events \
		--stack-name etl-pipeline-$(ENVIRONMENT) \
		--query "StackEvents[0:20].[Timestamp,ResourceStatus,ResourceType,LogicalResourceId]" \
		--output table

test-s3-event: ## Test the pipeline by uploading a sample CSV
	@echo "Uploading test CSV to trigger pipeline..."
	@echo "col1,col2,col3\n1,2,3\n4,5,6" > /tmp/test-etl.csv
	@aws s3 cp /tmp/test-etl.csv \
		s3://$(DATA_BUCKET_NAME)-$(AWS_ACCOUNT_ID)-$(ENVIRONMENT)/test/test-etl.csv
	@rm /tmp/test-etl.csv
	@echo "✓ Test CSV uploaded. Check Lambda logs for processing."

logs: ## Tail Lambda function logs
	@aws logs tail /aws/lambda/$(ENVIRONMENT)-emr-trigger --follow

clean: ## Clean build artifacts
	@rm -f lambda/emr-trigger-*.zip
	@echo "✓ Cleaned build artifacts"
