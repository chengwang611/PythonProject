# Databricks notebook source
# ---------------------------------------------------------------------------
# ETL Pipeline Notebook (Raw → Silver)
# ---------------------------------------------------------------------------
# Thin wrapper — all business logic lives in src/bank_etl/orchestrators/etl_runner.py.
# Run this notebook interactively for testing / debugging.
#
# Parameters (set via job base_parameters or widget UI):
#   config_path  — path to pipeline_config.yaml in workspace
#   trade_date   — YYYY-MM-DD (defaults to yesterday)
# ---------------------------------------------------------------------------

import json

import dbutils

dbutils.widgets.text("config_path", "/Workspace/Shared/pipeline_config.yaml", "Config file path")
dbutils.widgets.text("trade_date", "", "Trade date (YYYY-MM-DD, empty = yesterday)")

from bank_etl.orchestrators.etl_runner import run

summary = run(
    config_path=dbutils.widgets.get("config_path"),
    trade_date=dbutils.widgets.get("trade_date"),
)

dbutils.notebook.exit(json.dumps(summary))
