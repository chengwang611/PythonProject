"""Inventory report processor example."""
from typing import Dict, Any
from ..base_processor import BaseReportProcessor


class InventoryReportProcessor(BaseReportProcessor):
    def __init__(self, spark, mssql_config, s3_client=None):
        super().__init__(spark, mssql_config, s3_client)
        # hard-coded queries unique to this processor
        self.queries = {
            "INVENTORY_SUM": "SELECT product_id, SUM(qty) as total_qty FROM inventory GROUP BY product_id",
            "INVENTORY_RECENT": "SELECT * FROM inventory WHERE updated_at > current_date - interval 7 days"
        }

    def default_steps(self):
        return [
            {"function": "execute_sql_by_name", "sql": "INVENTORY_SUM", "view_name": "inventory_summary"},
            {"function": "write_table", "view_name": "inventory_summary", "table_name": "inventory_summary"}
        ]

    def run_default(self):
        steps = self.default_steps()
        self.run(steps)

