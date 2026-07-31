"""Configuration loader for the Databricks ingestion & ETL pipeline.

Reads from a YAML config file and Databricks secrets. On Databricks,
sensitive values are fetched via dbutils.secrets; locally they fall back
to environment variables or the YAML file.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


# ---------------------------------------------------------------------------
# Try to import dbutils (only available inside Databricks runtime)
# ---------------------------------------------------------------------------
try:
    from dbutils import DBUtils  # type: ignore[import-untyped]

    _dbutils: Optional[Any] = DBUtils()
except Exception:
    _dbutils = None


def _get_secret(scope: str, key: str, default: Optional[str] = None) -> Optional[str]:
    """Fetch a secret from Databricks secret scope, or fall back to env var."""
    if _dbutils is not None:
        try:
            return _dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            pass
    return os.getenv(key, default)


# ---------------------------------------------------------------------------
# Configuration data classes
# ---------------------------------------------------------------------------


@dataclass
class StorageConfig:
    """ADLS Gen2 / DBFS storage paths."""

    raw_container: str = "raw"
    storage_account: str = ""
    mount_point: str = "/mnt/datalake"
    raw_base_path: str = ""

    def __post_init__(self) -> None:
        if not self.raw_base_path:
            self.raw_base_path = (
                f"abfss://{self.raw_container}@{self.storage_account}"
                f".dfs.core.windows.net"
            )

    @property
    def salesforce_raw_path(self) -> str:
        return f"{self.raw_base_path}/raw/salesforce"

    @property
    def pmm_raw_path(self) -> str:
        return f"{self.raw_base_path}/raw/pmm"


@dataclass
class SalesforceConfig:
    """Salesforce connection configuration."""

    instance_url: str = "https://login.salesforce.com"
    api_version: str = "v58.0"
    client_id: str = ""
    client_secret: str = ""
    username: str = ""
    password: str = ""
    security_token: str = ""
    secret_scope: str = "salesforce-secrets"
    objects: List[str] = field(default_factory=lambda: ["Account", "Contact", "Opportunity"])

    @property
    def auth_url(self) -> str:
        return f"{self.instance_url}/services/oauth2/token"


@dataclass
class PmmConfig:
    """PMM REST API configuration."""

    base_url: str = "https://api.pmm.example.com"
    api_key: str = ""
    secret_scope: str = "pmm-secrets"
    endpoints: List[str] = field(
        default_factory=lambda: ["/v1/metrics", "/v1/transactions", "/v1/positions"]
    )
    page_size: int = 1000
    max_pages: int = 500


@dataclass
class UnityCatalogConfig:
    """Unity Catalog configuration for Delta tables."""

    catalog: str = "main"
    silver_schema: str = "silver"
    gold_schema: str = "gold"


@dataclass
class EtlConfig:
    """ETL pipeline configuration."""

    trade_date: Optional[str] = None
    validation_mode: str = "strict"  # strict | warn
    null_threshold_pct: float = 10.0
    dedup_keys: List[str] = field(default_factory=list)
    join_keys: List[str] = field(default_factory=lambda: ["account_id", "trade_date"])
    aggregation_columns: Dict[str, str] = field(default_factory=dict)
    silver_tables: List[str] = field(default_factory=list)


@dataclass
class PipelineConfig:
    """Top-level pipeline configuration aggregating all sub-configs."""

    storage: StorageConfig = field(default_factory=StorageConfig)
    salesforce: SalesforceConfig = field(default_factory=SalesforceConfig)
    pmm: PmmConfig = field(default_factory=PmmConfig)
    unity_catalog: UnityCatalogConfig = field(default_factory=UnityCatalogConfig)
    etl: EtlConfig = field(default_factory=EtlConfig)
    environment: str = "dev"

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------
    @classmethod
    def from_yaml(cls, path: str) -> "PipelineConfig":
        """Load configuration from a YAML file, resolving secrets."""
        with open(path, "r") as fh:
            raw: Dict[str, Any] = yaml.safe_load(fh) or {}

        return cls._from_dict(raw)

    @classmethod
    def _from_dict(cls, raw: Dict[str, Any]) -> "PipelineConfig":
        storage_raw = raw.get("storage", {})
        sf_raw = raw.get("salesforce", {})
        pmm_raw = raw.get("pmm", {})
        uc_raw = raw.get("unity_catalog", {})
        etl_raw = raw.get("etl", {})

        # Resolve secrets for Salesforce
        sf_scope = sf_raw.get("secret_scope", "salesforce-secrets")
        sf_raw.setdefault("client_id", _get_secret(sf_scope, "client_id") or "")
        sf_raw.setdefault("client_secret", _get_secret(sf_scope, "client_secret") or "")
        sf_raw.setdefault("username", _get_secret(sf_scope, "username") or "")
        sf_raw.setdefault("password", _get_secret(sf_scope, "password") or "")
        sf_raw.setdefault("security_token", _get_secret(sf_scope, "security_token") or "")

        # Resolve secrets for PMM
        pmm_scope = pmm_raw.get("secret_scope", "pmm-secrets")
        pmm_raw.setdefault("api_key", _get_secret(pmm_scope, "api_key") or "")

        return cls(
            storage=StorageConfig(**storage_raw),
            salesforce=SalesforceConfig(**sf_raw),
            pmm=PmmConfig(**pmm_raw),
            unity_catalog=UnityCatalogConfig(**uc_raw),
            etl=EtlConfig(**etl_raw),
            environment=raw.get("environment", "dev"),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def resolve_trade_date(self) -> str:
        """Return trade_date as YYYY-MM-DD; defaults to yesterday."""
        if self.etl.trade_date:
            return self.etl.trade_date
        return (date.today() - timedelta(days=1)).isoformat()

    @property
    def silver_table_prefix(self) -> str:
        return f"{self.unity_catalog.catalog}.{self.unity_catalog.silver_schema}"


# ---------------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------------
_config: Optional[PipelineConfig] = None


def load_config(config_path: Optional[str] = None) -> PipelineConfig:
    """Load (and cache) the pipeline configuration."""
    global _config
    if _config is not None:
        return _config

    if config_path is None:
        config_path = os.getenv(
            "PIPELINE_CONFIG_PATH",
            str(Path(__file__).resolve().parent.parent / "config.yaml"),
        )

    _config = PipelineConfig.from_yaml(config_path)
    return _config


def get_config() -> PipelineConfig:
    """Return the cached configuration (must call load_config first)."""
    if _config is None:
        raise RuntimeError("Configuration not loaded. Call load_config() first.")
    return _config
