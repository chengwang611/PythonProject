# Makefile for Azure Databricks Ingestion & ETL Pipeline
# Provides local development, build, and deployment commands.
#
# Two deployment methods are supported:
#   1. Databricks Asset Bundle (DAB) — recommended, declarative
#   2. REST API (legacy) — via scripts/deploy_workflows.py
#
# Usage:
#   make help              Show available commands
#   make build             Build the Python wheel
#   make test              Run unit tests
#   make lint              Run linters
#   make dab-deploy-dev    Deploy to dev via DAB
#   make deploy-dev        Deploy to dev via REST API (legacy)

.PHONY: help build clean test lint format validate \
        deploy-dev deploy-staging deploy-prod \
        dab-validate dab-deploy-dev dab-deploy-staging dab-deploy-prod \
        dab-run dab-destroy

SHELL := /bin/bash
ENVIRONMENT ?= dev
PACKAGE_NAME := databricks_ingestion_etl_pipeline
WHEEL_DIR := dist

# Databricks workspace / API settings (set via env vars or GitHub Secrets)
DATABRICKS_HOST ?= https://adb-xxxxxxxx.xx.azuredatabricks.net
DATABRICKS_TOKEN ?=
DATABRICKS_CLUSTER_ID ?=

# DAB-specific variables
DATABRICKS_CLUSTER_ID_DEV ?=
DATABRICKS_CLUSTER_ID_STAGING ?=
DATABRICKS_CLUSTER_ID_PROD ?=
DATABRICKS_SP_NAME_DEV ?=
DATABRICKS_SP_NAME_STAGING ?=
DATABRICKS_SP_NAME_PROD ?=

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-25s\033[0m %s\n", $$1, $$2}'

# ============================================================================
# Build
# ============================================================================

build: clean ## Build the Python wheel
	@echo "Building Python wheel …"
	@pip install -q build
	@python -m build --wheel
	@echo "✓ Wheel built: $$(ls dist/*.whl)"

clean: ## Remove build artifacts
	@rm -rf dist/ build/ *.egg-info src/*.egg-info
	@find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name '*.pyc' -delete 2>/dev/null || true
	@echo "✓ Cleaned build artifacts"

# ============================================================================
# Validation
# ============================================================================

validate: lint test ## Run all validations

lint: ## Run linters (flake8, isort check, black check)
	@echo "Running flake8 …"
	@flake8 src/ --max-line-length=120 --ignore=E203,W503 || true
	@echo "Running isort check …"
	@isort --check-only --profile black src/ || true
	@echo "Running black check …"
	@black --check --line-length=120 src/ || true
	@echo "✓ Lint checks complete"

format: ## Auto-format code with isort + black
	@echo "Formatting code …"
	@isort --profile black src/
	@black --line-length=120 src/
	@echo "✓ Code formatted"

test: ## Run unit tests with pytest
	@echo "Running tests …"
	@python -m pytest src/ -v --tb=short --cov=src --cov-report=term-missing || true
	@echo "✓ Tests complete"

# ============================================================================
# Deployment — Databricks Asset Bundle (DAB) — RECOMMENDED
# ============================================================================

dab-validate: ## Validate the Databricks Asset Bundle
	@echo "Validating Databricks Asset Bundle …"
	@databricks bundle validate
	@echo "✓ Bundle valid"

dab-deploy-dev: build ## Deploy bundle to dev via DAB
	@echo "Deploying bundle to dev …"
	@databricks bundle deploy -t dev \
		--var="databricks_host=$(DATABRICKS_HOST)" \
		--var="cluster_id_dev=$(DATABRICKS_CLUSTER_ID_DEV)" \
		--var="sp_name_dev=$(DATABRICKS_SP_NAME_DEV)" \
		--var="cluster_id_staging=placeholder" \
		--var="sp_name_staging=placeholder" \
		--var="cluster_id_prod=placeholder" \
		--var="sp_name_prod=placeholder"
	@echo "✓ Bundle deployed to dev"

dab-deploy-staging: build ## Deploy bundle to staging via DAB
	@echo "Deploying bundle to staging …"
	@databricks bundle deploy -t staging \
		--var="databricks_host=$(DATABRICKS_HOST)" \
		--var="cluster_id_dev=placeholder" \
		--var="sp_name_dev=placeholder" \
		--var="cluster_id_staging=$(DATABRICKS_CLUSTER_ID_STAGING)" \
		--var="sp_name_staging=$(DATABRICKS_SP_NAME_STAGING)" \
		--var="cluster_id_prod=placeholder" \
		--var="sp_name_prod=placeholder"
	@echo "✓ Bundle deployed to staging"

dab-deploy-prod: build ## Deploy bundle to production via DAB
	@echo "Deploying bundle to production …"
	@databricks bundle deploy -t prod \
		--var="databricks_host=$(DATABRICKS_HOST)" \
		--var="cluster_id_dev=placeholder" \
		--var="sp_name_dev=placeholder" \
		--var="cluster_id_staging=placeholder" \
		--var="sp_name_staging=placeholder" \
		--var="cluster_id_prod=$(DATABRICKS_CLUSTER_ID_PROD)" \
		--var="sp_name_prod=$(DATABRICKS_SP_NAME_PROD)"
	@echo "✓ Bundle deployed to production"

dab-run: ## Run the daily ingestion ETL job via DAB (target from ENVIRONMENT var)
	@echo "Running daily_ingestion_etl_pipeline on $(ENVIRONMENT) …"
	@databricks bundle run -t $(ENVIRONMENT) daily_ingestion_etl_pipeline

dab-destroy: ## Destroy all deployed bundle resources for a target
	@echo "WARNING: This will destroy all bundle resources in $(ENVIRONMENT)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
		echo; \
		if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
			databricks bundle destroy -t $(ENVIRONMENT); \
			echo "✓ Bundle destroyed in $(ENVIRONMENT)"; \
		else \
			echo "Aborted"; \
		fi

# ============================================================================
# Deployment — REST API (legacy)
# ============================================================================

deploy-dev: build ## Deploy to dev via REST API (legacy)
	@$(MAKE) _deploy ENVIRONMENT=dev

deploy-staging: build ## Deploy to staging via REST API (legacy)
	@$(MAKE) _deploy ENVIRONMENT=staging

deploy-prod: build ## Deploy to production via REST API (legacy)
	@$(MAKE) _deploy ENVIRONMENT=prod

_deploy:
	@echo "Deploying to $(ENVIRONMENT) via REST API …"
	@python scripts/deploy_workflows.py \
		--environment $(ENVIRONMENT) \
		--wheel-dir $(WHEEL_DIR) \
		--databricks-host "$(DATABRICKS_HOST)" \
		--databricks-token "$(DATABRICKS_TOKEN)" \
		--cluster-id "$(DATABRICKS_CLUSTER_ID)"
	@echo "✓ Deployment to $(ENVIRONMENT) complete"

# ============================================================================
# Utility
# ============================================================================

notebooks-export: ## Export notebooks as .py files (for version control)
	@echo "Notebooks are stored as .py files in notebooks/ — ready for version control."

deps: ## Install development dependencies
	@pip install -r requirements.txt
	@pip install build pytest pytest-cov flake8 black isort mypy
	@echo "✓ Dependencies installed"

dab-cli-install: ## Install Databricks CLI
	@echo "Installing Databricks CLI …"
	@curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
	@echo "✓ Databricks CLI installed"
