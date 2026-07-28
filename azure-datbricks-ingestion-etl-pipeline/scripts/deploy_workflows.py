#!/usr/bin/env python3
"""Deploy pipeline artifacts to Databricks workspace.

This script:
1. Uploads the Python wheel to DBFS
2. Installs the wheel on the target cluster (or as a workspace library)
3. Imports notebooks to the workspace
4. Creates/updates Databricks Workflows (jobs) via REST API
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger("deploy_workflows")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ---------------------------------------------------------------------------
# Databricks REST API helpers
# ---------------------------------------------------------------------------


class DatabricksApi:
    """Minimal Databricks REST API client for deployment operations."""

    def __init__(self, host: str, token: str) -> None:
        self.host = host.rstrip("/")
        self.token = token
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    def _api_url(self, path: str) -> str:
        return f"{self.host}/api/2.0{path}"

    # -- Workspace ----------------------------------------------------------
    def import_workspace_item(
        self,
        path: str,
        content: str,
        language: str = "PYTHON",
        overwrite: bool = True,
        fmt: str = "SOURCE",
    ) -> Dict[str, Any]:
        """Import a file into the Databricks workspace."""
        url = self._api_url("/workspace/import")
        payload = {
            "path": path,
            "content": content,
            "language": language,
            "overwrite": overwrite,
            "format": fmt,
        }
        resp = self.session.post(url, json=payload, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def mkdirs(self, path: str) -> Dict[str, Any]:
        """Create workspace directories recursively."""
        url = self._api_url("/workspace/mkdirs")
        resp = self.session.post(url, json={"path": path}, timeout=30)
        resp.raise_for_status()
        return resp.json()

    # -- DBFS ---------------------------------------------------------------
    def dbfs_put(self, dbfs_path: str, local_path: str, overwrite: bool = True) -> None:
        """Upload a file to DBFS."""
        # Step 1: create handle
        create_url = self._api_url("/dbfs/create")
        create_resp = self.session.post(create_url, json={
            "path": dbfs_path,
            "overwrite": overwrite,
        }, timeout=30)
        create_resp.raise_for_status()
        handle = create_resp.json()["handle"]

        # Step 2: add block (single-block upload for files < 1MB)
        with open(local_path, "rb") as fh:
            data = fh.read()

        block_url = self._api_url("/dbfs/add-block")
        self.session.post(block_url, json={
            "handle": handle,
            "data": data.hex(),
        }, timeout=120)

        # Step 3: close
        close_url = self._api_url("/dbfs/close")
        self.session.post(close_url, json={"handle": handle}, timeout=30)

        logger.info("Uploaded %s → %s", local_path, dbfs_path)

    # -- Jobs / Workflows ---------------------------------------------------
    def create_job(self, job_def: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new job/workflow."""
        url = self._api_url("/jobs/create")
        resp = self.session.post(url, json=job_def, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def list_jobs(self, name_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """List jobs, optionally filtered by name."""
        url = self._api_url("/jobs/list")
        params: Dict[str, Any] = {}
        if name_filter:
            params["name"] = name_filter
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json().get("jobs", [])

    def update_job(self, job_id: int, job_def: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing job definition."""
        url = self._api_url("/jobs/reset")
        payload = {"job_id": job_id, "new_settings": job_def}
        resp = self.session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def upsert_job(self, job_name: str, job_def: Dict[str, Any]) -> Dict[str, Any]:
        """Create or update a job by name."""
        existing = self.list_jobs(name_filter=job_name)
        if existing:
            job_id = existing[0]["job_id"]
            logger.info("Updating existing job: %s (id=%s)", job_name, job_id)
            return self.update_job(job_id, job_def)
        else:
            logger.info("Creating new job: %s", job_name)
            return self.create_job(job_def)

    # -- Libraries ----------------------------------------------------------
    def install_library(self, cluster_id: str, dbfs_path: str) -> Dict[str, Any]:
        """Install a wheel library on a cluster."""
        url = self._api_url("/libraries/install")
        payload = {
            "cluster_id": cluster_id,
            "libraries": [{"whl": dbfs_path}],
        }
        resp = self.session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Deployment logic
# ---------------------------------------------------------------------------


def deploy_notebooks(api: DatabricksApi, notebooks_dir: str, workspace_base: str) -> None:
    """Import all .py notebook files into the Databricks workspace."""
    api.mkdirs(workspace_base)

    nb_dir = Path(notebooks_dir)
    for nb_file in nb_dir.glob("*.py"):
        content = nb_file.read_text(encoding="utf-8")
        ws_path = f"{workspace_base}/{nb_file.stem}"
        api.import_workspace_item(ws_path, content, language="PYTHON", overwrite=True)
        logger.info("Imported notebook: %s", ws_path)


def deploy_wheel(api: DatabricksApi, wheel_dir: str, dbfs_base: str) -> str:
    """Upload the latest wheel to DBFS and return its path."""
    wheel_files = list(Path(wheel_dir).glob("*.whl"))
    if not wheel_files:
        raise FileNotFoundError(f"No .whl files found in {wheel_dir}")

    # Pick the latest by modification time
    wheel_file = max(wheel_files, key=lambda p: p.stat().st_mtime)
    dbfs_path = f"{dbfs_base}/{wheel_file.name}"

    api.dbfs_put(dbfs_path, str(wheel_file), overwrite=True)
    return dbfs_path


def deploy_workflows(
    api: DatabricksApi,
    workflows_dir: str,
    environment: str,
    cluster_id: str,
) -> None:
    """Create or update Databricks Workflows from JSON definitions."""
    wf_dir = Path(workflows_dir)
    for wf_file in wf_dir.glob("*.json"):
        raw = wf_file.read_text(encoding="utf-8")
        # Replace placeholders
        raw = raw.replace("{{DATABRICKS_CLUSTER_ID}}", cluster_id)
        raw = raw.replace("{{ENVIRONMENT}}", environment)

        job_def = json.loads(raw)
        job_name = job_def["name"]

        api.upsert_job(job_name, job_def)
        logger.info("Workflow deployed: %s", job_name)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy Databricks pipeline artifacts")
    parser.add_argument("--environment", default="dev", help="Target environment")
    parser.add_argument("--wheel-dir", default="dist", help="Directory containing .whl files")
    parser.add_argument("--notebooks-dir", default="notebooks", help="Directory containing notebook .py files")
    parser.add_argument("--workflows-dir", default="workflows", help="Directory containing workflow JSON files")
    parser.add_argument("--databricks-host", required=True, help="Databricks workspace URL")
    parser.add_argument("--databricks-token", required=True, help="Databricks personal access token")
    parser.add_argument("--cluster-id", required=True, help="Target cluster ID")
    parser.add_argument("--workspace-base", default="/Workspace/Shared/notebooks", help="Workspace base path for notebooks")
    parser.add_argument("--dbfs-base", default="dbfs:/FileStore/wheels", help="DBFS base path for wheels")
    parser.add_argument("--skip-wheel", action="store_true", help="Skip wheel upload")
    parser.add_argument("--skip-notebooks", action="store_true", help="Skip notebook import")
    parser.add_argument("--skip-workflows", action="store_true", help="Skip workflow deployment")

    args = parser.parse_args()

    api = DatabricksApi(host=args.databricks_host, token=args.databricks_token)

    if not args.skip_wheel:
        wheel_path = deploy_wheel(api, args.wheel_dir, args.dbfs_base)
        logger.info("Wheel deployed: %s", wheel_path)

    if not args.skip_notebooks:
        deploy_notebooks(api, args.notebooks_dir, args.workspace_base)

    if not args.skip_workflows:
        deploy_workflows(api, args.workflows_dir, args.environment, args.cluster_id)

    logger.info("Deployment complete for environment: %s", args.environment)


if __name__ == "__main__":
    main()
