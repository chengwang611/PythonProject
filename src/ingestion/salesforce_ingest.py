"""Salesforce ingestion helper using OAuth2 and the REST API.

This is a lightweight client that supports querying Salesforce sobjects via REST
and saving snapshots as parquet.
"""
from typing import Dict, Any, List
from .auth import OAuth2Client
import requests


class SalesforceIngestor:
    def __init__(self, instance_url: str, oauth_client: OAuth2Client):
        self.instance_url = instance_url.rstrip("/")
        self.oauth = oauth_client

    def _headers(self):
        return {"Authorization": f"Bearer {self.oauth.get_token()}", "Accept": "application/json"}

    def query(self, soql: str) -> Dict[str, Any]:
        url = f"{self.instance_url}/services/data/v52.0/query"
        resp = requests.get(url, params={"q": soql}, headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    def query_all(self, soql: str) -> List[Dict[str, Any]]:
        result = self.query(soql)
        records = result.get("records", [])
        # handle nextRecordsUrl paging
        while not result.get("done") and result.get("nextRecordsUrl"):
            next_url = f"{self.instance_url}{result['nextRecordsUrl']}"
            resp = requests.get(next_url, headers=self._headers())
            resp.raise_for_status()
            result = resp.json()
            records.extend(result.get("records", []))
        return records

