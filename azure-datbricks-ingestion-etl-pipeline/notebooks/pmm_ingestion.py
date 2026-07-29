# Databricks notebook source
# ---------------------------------------------------------------------------
# PMM Ingestion Notebook
# ---------------------------------------------------------------------------
# Thin wrapper — all business logic lives in src/bank_etl/orchestrators/pmm_runner.py.
# Run this notebook interactively for testing / debugging.
#
# Parameters (set via job base_parameters or widget UI):
#   config_path  — path to pipeline_config.yaml in workspace
#   trade_date   — YYYY-MM-DD (defaults to yesterday)
#   endpoints    — comma-separated list of PMM endpoints (optional)
# ---------------------------------------------------------------------------

import json

dbutils.widgets.text("config_path", "/Workspace/Shared/pipeline_config.yaml", "Config file path")
dbutils.widgets.text("trade_date", "", "Trade date (YYYY-MM-DD, empty = yesterday)")
dbutils.widgets.text("endpoints", "", "Comma-separated PMM endpoints (empty = all configured)")

from bank_etl.orchestrators.pmm_runner import run

summary = run(
    config_path=dbutils.widgets.get("config_path"),
    trade_date=dbutils.widgets.get("trade_date"),
    endpoints=dbutils.widgets.get("endpoints"),
)

dbutils.notebook.exit(json.dumps(summary))
