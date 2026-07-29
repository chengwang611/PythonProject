"""python_wheel_task entry point: Salesforce Ingestion.

Thin wrapper — all business logic lives in ``src/bank_etl/orchestrators/salesforce_runner.py``.
 
Usage (via Databricks python_wheel_task)::
 
    package_name: databricks_ingestion_etl_pipeline
    entry_point:  bank_etl.entry_points.salesforce_ingestion_entry:main
    parameters:
      - "--config_path"
      - "/Workspace/Shared/pipeline_config.yaml"
      - "--trade_date"
      - "{{job.parameter.trade_date}}"
"""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta

from bank_etl.orchestrators.salesforce_runner import run


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Salesforce Bulk API ingestion")
    parser.add_argument(
        "--config_path",
        default="/Workspace/Shared/pipeline_config.yaml",
        help="Path to pipeline_config.yaml in workspace",
    )
    parser.add_argument(
        "--trade_date",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="Trade date in YYYY-MM-DD format (default: yesterday)",
    )
    parser.add_argument(
        "--objects",
        default="",
        help="Comma-separated SF objects (empty = all configured)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Entry point for the Salesforce ingestion wheel task."""
    args = _parse_args(argv)

    summary = run(
        config_path=args.config_path,
        trade_date=args.trade_date,
        objects=args.objects,
    )

    # Databricks python_wheel_task captures stdout; print JSON for downstream
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
