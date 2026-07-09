"""Customer report processor example."""
from typing import Dict, Any
from ..base_processor import BaseReportProcessor


class CustomerReportProcessor(BaseReportProcessor):
    def __init__(self, spark, mssql_config, s3_client=None):
        super().__init__(spark, mssql_config, s3_client)
        self.queries = {
            "CUSTOMER_ACTIVE": "SELECT customer_id, name, status FROM customers WHERE status = 'ACTIVE'",
            "CUSTOMER_RECENT_ORDERS": "SELECT c.customer_id, o.order_id, o.amount FROM customers c JOIN orders o ON c.customer_id = o.customer_id WHERE o.order_date > current_date - interval 30 days"
        }

    def default_steps(self):
        return [
            {"function": "execute_sql_by_name", "sql": "CUSTOMER_ACTIVE", "view_name": "active_customers"},
            {"function": "execute_sql_by_name", "sql": "CUSTOMER_RECENT_ORDERS", "view_name": "recent_orders"},
            {"function": "write_table", "view_name": "active_customers", "table_name": "active_customers"}
        ]

    def run_default(self):
        steps = self.default_steps()
        self.run(steps)

