"""PMM REST API ingestion client.

PMM (Portfolio Management & Metrics) data is extracted via REST APIs
with cursor-based pagination.  This client wraps the generic
BaseRestClient with PMM-specific endpoint configuration.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from .auth import ApiKeyAuth
from .base_rest import BaseRestClient

logger = logging.getLogger(__name__)


class PmmApiClient:
    """Client for extracting data from the PMM REST API."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        page_size: int = 1000,
        max_pages: int = 500,
    ) -> None:
        auth = ApiKeyAuth(api_key)
        self.rest = BaseRestClient(
            base_url=base_url,
            headers=auth.get_headers(),
        )
        self.page_size = page_size
        self.max_pages = max_pages

    # ------------------------------------------------------------------
    # Endpoint extraction
    # ------------------------------------------------------------------
    def fetch_endpoint(
        self,
        endpoint: str,
        extra_params: Optional[Dict[str, Any]] = None,
        trade_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all records from a PMM endpoint.

        Args:
            endpoint: API path, e.g. '/v1/metrics'.
            extra_params: Additional query parameters.
            trade_date: Filter by trade date (YYYY-MM-DD).

        Returns:
            List of record dicts.
        """
        params: Dict[str, Any] = dict(extra_params or {})
        if trade_date:
            params["trade_date"] = trade_date

        logger.info("Fetching PMM endpoint: %s (trade_date=%s)", endpoint, trade_date)
        all_records: List[Dict[str, Any]] = []

        for chunk in self.rest.paginate_cursor(
            path=endpoint,
            params=params,
            cursor_param="cursor",
            next_cursor_key="next_cursor",
            max_pages=self.max_pages,
            results_key="items",
        ):
            all_records.extend(chunk)
            logger.debug("  … %d records so far", len(all_records))

        logger.info("Fetched %d total records from %s", len(all_records), endpoint)
        return all_records

    def fetch_all_endpoints(
        self,
        endpoints: List[str],
        trade_date: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch all configured PMM endpoints.

        Args:
            endpoints: List of API paths.
            trade_date: Filter by trade date.

        Returns:
            Dict mapping endpoint → list of record dicts.
        """
        results: Dict[str, List[Dict[str, Any]]] = {}
        for ep in endpoints:
            # Derive a clean table name from the endpoint path
            table_name = ep.strip("/").replace("/", "_").replace("-", "_")
            results[table_name] = self.fetch_endpoint(ep, trade_date=trade_date)
        return results
