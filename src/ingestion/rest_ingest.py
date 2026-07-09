"""Generic REST ingestion that pages through APIs and returns a pandas-like record list.

This module does not depend on Spark. It returns list[dict] that the caller can
convert to Spark DataFrame for writing to S3 as parquet.
"""
from typing import Dict, Any, List, Optional
import requests


class RestIngestor:
    def __init__(self, base_url: str, token_getter=None, headers: Optional[Dict[str, str]] = None):
        self.base_url = base_url.rstrip("/")
        self.token_getter = token_getter
        self.static_headers = headers or {}

    def _headers(self):
        headers = dict(self.static_headers)
        if self.token_getter:
            headers["Authorization"] = f"Bearer {self.token_getter()}"
        return headers

    def fetch_all(self, path: str, params: Optional[Dict[str, Any]] = None, page_param: str = "page", page_size_param: str = "pageSize") -> List[Dict[str, Any]]:
        # Very small pager that assumes the API returns JSON array in `items` key, and `next` boolean or next page index
        items: List[Dict[str, Any]] = []
        url = f"{self.base_url}/{path.lstrip('/')}"
        page = 1
        while True:
            q = dict(params or {})
            q[page_param] = page
            resp = requests.get(url, params=q, headers=self._headers())
            resp.raise_for_status()
            data = resp.json()
            # support both list returned or container with items
            chunk = data.get("items") if isinstance(data, dict) and "items" in data else data
            if not chunk:
                break
            items.extend(chunk)
            # simple termination checks
            # if dict with `next` flag
            if isinstance(data, dict) and data.get("next"):
                page += 1
                continue
            # if returned list, increment page until empty
            page += 1
        return items

