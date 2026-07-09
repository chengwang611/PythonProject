"""Ingestion package for REST and Salesforce data ingestion.

Modules:
- auth: OAuth2 client helper
- s3_util: write DataFrame to S3 as parquet
- salesforce_ingest: Salesforce-specific ingestion logic
- rest_ingest: generic REST ingestion logic
- runner: example runner
"""

# Import submodules so they are visible when importing the package
from . import auth, s3_util, salesforce_ingest, rest_ingest, runner

__all__ = ["auth", "s3_util", "salesforce_ingest", "rest_ingest", "runner"]
