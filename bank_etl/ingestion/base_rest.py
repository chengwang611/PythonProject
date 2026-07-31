"""Generic REST API pagination helper.

Provides a reusable base class for paginating REST APIs that return
JSON responses. Supports offset-based, page-based, and cursor-based
pagination strategies.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Iterator, List, Optional

import requests

logger = logging.getLogger(__name__)


class BaseRestClient:
    """Generic REST client with pluggable pagination."""

    def __init__(
        self,
        base_url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 60,
        max_retries: int = 3,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.headers = headers or {}
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        url = f"{self.base_url}/{path.lstrip('/')}"
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                logger.warning("Request attempt %d/%d failed: %s", attempt, self.max_retries, exc)
                if attempt == self.max_retries:
                    raise
        # unreachable
        raise RuntimeError("Unexpected retry exhaustion")

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        return self._request("GET", path, params=params)

    def post(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        return self._request("POST", path, params=params, json_body=json_body)

    # ------------------------------------------------------------------
    # Pagination strategies
    # ------------------------------------------------------------------
    def paginate_offset(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        offset_param: str = "offset",
        limit_param: str = "limit",
        page_size: int = 1000,
        max_pages: int = 500,
        results_key: Optional[str] = "items",
    ) -> Iterator[List[Dict[str, Any]]]:
        """Offset-based pagination.

        Yields lists of records (one list per page).
        """
        base_params = dict(params or {})
        for page in range(max_pages):
            q = dict(base_params)
            q[offset_param] = page * page_size
            q[limit_param] = page_size

            resp = self.get(path, params=q)
            data = resp.json()

            chunk = self._extract_items(data, results_key)
            if not chunk:
                break
            yield chunk

            # Stop if fewer records than page_size returned
            if len(chunk) < page_size:
                break

    def paginate_cursor(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        cursor_param: str = "cursor",
        next_cursor_key: str = "next_cursor",
        max_pages: int = 500,
        results_key: Optional[str] = "items",
    ) -> Iterator[List[Dict[str, Any]]]:
        """Cursor-based pagination.

        Yields lists of records (one list per page).
        """
        base_params = dict(params or {})
        cursor: Optional[str] = None

        for _ in range(max_pages):
            q = dict(base_params)
            if cursor:
                q[cursor_param] = cursor

            resp = self.get(path, params=q)
            data = resp.json()

            chunk = self._extract_items(data, results_key)
            if chunk:
                yield chunk

            cursor = data.get(next_cursor_key) if isinstance(data, dict) else None
            if not cursor:
                break

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_items(
        data: Any, results_key: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Extract a list of records from a response payload."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and results_key:
            items = data.get(results_key, [])
            return items if isinstance(items, list) else []
        return []
