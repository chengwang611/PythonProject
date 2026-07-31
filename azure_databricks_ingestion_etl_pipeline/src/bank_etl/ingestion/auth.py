"""OAuth2 authentication helpers for API-based ingestion.

Supports:
- Client Credentials grant
- Password grant (used by Salesforce)
- API-key based auth (used by PMM)
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)


class OAuth2Client:
    """Generic OAuth2 client supporting client_credentials and password grants."""

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        *,
        username: Optional[str] = None,
        password: Optional[str] = None,
        scope: str = "",
    ) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.scope = scope
        self._token: Dict[str, Any] = {}
        self._instance_url: Optional[str] = None

    # ------------------------------------------------------------------
    # Token lifecycle
    # ------------------------------------------------------------------
    def _is_valid(self) -> bool:
        return bool(self._token) and time.time() < self._token.get("expires_at", 0)

    def fetch_token(self) -> Dict[str, Any]:
        """Obtain a new access token."""
        payload: Dict[str, str] = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        if self.username and self.password:
            payload["grant_type"] = "password"
            payload["username"] = self.username
            payload["password"] = self.password
        else:
            payload["grant_type"] = "client_credentials"

        if self.scope:
            payload["scope"] = self.scope

        logger.info("Fetching OAuth2 token from %s", self.token_url)
        resp = requests.post(self.token_url, data=payload, timeout=30)
        resp.raise_for_status()
        token_data = resp.json()

        expires_in = int(token_data.get("expires_in", 3600))
        token_data["expires_at"] = time.time() + expires_in - 30  # 30s buffer
        self._token = token_data

        # Salesforce returns instance_url in token response
        if "instance_url" in token_data:
            self._instance_url = token_data["instance_url"]

        logger.info("Token obtained, expires in %ds", expires_in)
        return token_data

    def get_token(self) -> str:
        """Return a valid access token, refreshing if needed."""
        if not self._is_valid():
            self.fetch_token()
        return str(self._token.get("access_token", ""))

    @property
    def instance_url(self) -> Optional[str]:
        return self._instance_url


class ApiKeyAuth:
    """Simple API-key based authentication (used by PMM)."""

    def __init__(self, api_key: str, header_name: str = "X-API-Key") -> None:
        self.api_key = api_key
        self.header_name = header_name

    def get_headers(self) -> Dict[str, str]:
        return {self.header_name: self.api_key, "Accept": "application/json"}
