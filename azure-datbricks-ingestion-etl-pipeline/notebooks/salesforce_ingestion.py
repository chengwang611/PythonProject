# Databricks notebook source
# ---------------------------------------------------------------------------
# Salesforce Ingestion Notebook
# ---------------------------------------------------------------------------
# Thin wrapper — all business logic lives in src/bank_etl/orchestrators/salesforce_runner.py.
# Run this notebook interactively for testing / debugging.
#
# Parameters (set via job base_parameters or widget UI):
#   config_path  — path to pipeline_config.yaml in workspace
#   trade_date   — YYYY-MM-DD (defaults to yesterday)
#   objects      — comma-separated list of SF objects (optional)
# ---------------------------------------------------------------------------

import json

dbutils.widgets.text("config_path", "/Workspace/Shared/pipeline_config.yaml", "Config file path")
dbutils.widgets.text("trade_date", "", "Trade date (YYYY-MM-DD, empty = yesterday)")
dbutils.widgets.text("objects", "", "Comma-separated SF objects (empty = all configured)")

from bank_etl.orchestrators.salesforce_runner import run

summary = run(
    config_path=dbutils.widgets.get("config_path"),
    trade_date=dbutils.widgets.get("trade_date"),
    objects=dbutils.widgets.get("objects"),
)

dbutils.notebook.exit(json.dumps(summary))