"""Salesforce Bulk API 2.0 ingestion client.

Uses the Bulk API 2.0 (REST-based) for efficient large-volume data
extraction.  Supports:
- Creating a bulk query job
- Polling for job completion
- Downloading results as JSON / CSV
- Converting results to a list of dicts for Spark DataFrame creation
"""

from __future__ import annotations

import csv
import io
import json
import logging
import time
from typing import Any, Dict, List, Optional

import requests

from .auth import OAuth2Client

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BULK_API_BASE = "/services/data/{api_version}/jobs/query"
POLL_INTERVAL_SECONDS = 5
MAX_POLL_ATTEMPTS = 360  # 30 minutes at 5s intervals


class SalesforceBulkClient:
    """Salesforce Bulk API 2.0 client for query-based extraction."""

    def __init__(
        self,
        instance_url: str,
        oauth_client: OAuth2Client,
        api_version: str = "v58.0",
    ) -> None:
        self.instance_url = instance_url.rstrip("/")
        self.oauth = oauth_client
        self.api_version = api_version
        self._base = BULK_API_BASE.format(api_version=api_version)

    # ------------------------------------------------------------------
    # Auth headers
    # ------------------------------------------------------------------
    def _headers(self, content_type: str = "application/json") -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.oauth.get_token()}",
            "Accept": "application/json",
            "Content-Type": content_type,
        }

    # ------------------------------------------------------------------
    # Job lifecycle
    # ------------------------------------------------------------------
    def create_query_job(self, soql: str, object_name: str = "") -> str:
        """Create a Bulk API 2.0 query job and return the job ID.

        Args:
            soql: SOQL query string.
            object_name: Salesforce object (e.g. 'Account').  Used for
                         the job label only.

        Returns:
            Bulk API job ID.
        """
        url = f"{self.instance_url}{self._base}"
        payload: Dict[str, Any] = {
            "operation": "query",
            "query": soql,
        }
        if object_name:
            payload["object"] = object_name

        logger.info("Creating Bulk API query job for %s", object_name or "custom query")
        resp = requests.post(url, json=payload, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        job_info = resp.json()
        job_id = job_info["id"]
        logger.info("Bulk API job created: %s", job_id)
        return job_id

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Return the current status of a Bulk API job."""
        url = f"{self.instance_url}{self._base}/{job_id}"
        resp = requests.get(url, headers=self._headers(), timeout=30)
        resp.raise_for_status()
        return resp.json()

    def wait_for_completion(
        self,
        job_id: str,
        poll_interval: int = POLL_INTERVAL_SECONDS,
        max_attempts: int = MAX_POLL_ATTEMPTS,
    ) -> Dict[str, Any]:
        """Poll until the job reaches a terminal state.

        Returns:
            Final job info dict.

        Raises:
            RuntimeError: If the job fails or times out.
        """
        for attempt in range(1, max_attempts + 1):
            info = self.get_job_status(job_id)
            state = info.get("state", "Unknown")

            logger.debug(
                "Job %s — attempt %d — state=%s — records=%s/%s",
                job_id,
                attempt,
                state,
                info.get("numberRecordsProcessed", 0),
                info.get("numberRecordsTotal", "?"),
            )

            if state in ("JobComplete",):
                logger.info("Job %s completed successfully.", job_id)
                return info
            if state in ("Failed", "Aborted"):
                error_msg = info.get("errorMessage", "Unknown error")
                raise RuntimeError(f"Bulk API job {job_id} failed: {error_msg}")

            time.sleep(poll_interval)

        raise TimeoutError(
            f"Bulk API job {job_id} did not complete within {max_attempts * poll_interval}s"
        )

    # ------------------------------------------------------------------
    # Result download
    # ------------------------------------------------------------------
    def download_results(self, job_id: str, locator: str = "") -> List[str]:
        """Download query results for a completed job.

        Salesforce returns results as CSV.  This method returns a list
        of CSV strings (one per result locator).

        Args:
            job_id: Bulk API job ID.
            locator: Result locator (empty = all results).

        Returns:
            List of raw CSV strings.
        """
        url = f"{self.instance_url}{self._base}/{job_id}/results"
        params: Dict[str, str] = {}
        if locator:
            params["locator"] = locator

        logger.info("Downloading results for job %s", job_id)
        resp = requests.get(
            url,
            params=params,
            headers=self._headers("text/csv"),
            timeout=120,
        )
        resp.raise_for_status()

        # The response may be a single CSV or a JSON manifest listing result IDs
        content_type = resp.headers.get("Content-Type", "")
        if "json" in content_type:
            # Manifest — download each result file
            manifest = resp.json()
            csv_parts: List[str] = []
            for result_ref in manifest if isinstance(manifest, list) else [manifest]:
                csv_parts.append(self.download_results(job_id, result_ref))
            return csv_parts

        return [resp.text]

    def download_results_json(self, job_id: str) -> List[Dict[str, Any]]:
        """Download results and parse CSV into list-of-dict records.

        This is the primary method for converting Bulk API output into
        a format suitable for Spark DataFrame creation.
        """
        csv_parts = self.download_results(job_id)
        records: List[Dict[str, Any]] = []

        for csv_text in csv_parts:
            if not csv_text.strip():
                continue
            reader = csv.DictReader(io.StringIO(csv_text))
            records.extend(reader)

        logger.info("Downloaded %d records from job %s", len(records), job_id)
        return records

    # ------------------------------------------------------------------
    # High-level: run query end-to-end
    # ------------------------------------------------------------------
    def run_query(
        self,
        soql: str,
        object_name: str = "",
    ) -> List[Dict[str, Any]]:
        """Create job → wait → download → return records.

        This is the main entry point for a single SOQL extraction.
        """
        job_id = self.create_query_job(soql, object_name)
        self.wait_for_completion(job_id)
        return self.download_results_json(job_id)

    def run_queries(
        self,
        queries: Dict[str, str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Run multiple SOQL queries (one per object) and return results.

        Args:
            queries: Dict mapping object_name → SOQL string.

        Returns:
            Dict mapping object_name → list of record dicts.
        """
        results: Dict[str, List[Dict[str, Any]]] = {}
        for obj_name, soql in queries.items():
            logger.info("Extracting %s ...", obj_name)
            results[obj_name] = self.run_query(soql, obj_name)
        return results
