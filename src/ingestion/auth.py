"""OAuth2 helper for obtaining access tokens for APIs."""
import time
from typing import Dict, Any
import requests


class OAuth2Client:
    def __init__(self, token_url: str, client_id: str, client_secret: str, scope: str = ""):
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self._token: Dict[str, Any] = {}

    def _is_valid(self) -> bool:
        return bool(self._token) and time.time() < self._token.get("expires_at", 0)

    def fetch_token(self) -> Dict[str, Any]:
        resp = requests.post(self.token_url, data={
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": self.scope
        })
        resp.raise_for_status()
        token_data = resp.json()
        # ensure expires_at present
        expires_in = token_data.get("expires_in", 3600)
        token_data["expires_at"] = time.time() + int(expires_in) - 10
        self._token = token_data
        return token_data

    def get_token(self) -> str:
        if not self._is_valid():
            self.fetch_token()
        return self._token.get("access_token")

