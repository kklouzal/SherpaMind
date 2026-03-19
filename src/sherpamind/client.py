from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import base64

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from .rate_limit import RequestPacer


@dataclass
class SherpaDeskClient:
    api_base_url: str
    api_key: str
    api_user: str | None = None
    org_key: str | None = None
    instance_key: str | None = None
    timeout_seconds: float = 30.0
    min_interval_seconds: float = 8.0
    pacer: RequestPacer = field(init=False)

    def __post_init__(self) -> None:
        self.pacer = RequestPacer(min_interval_seconds=self.min_interval_seconds)

    def _auth_identity(self) -> str:
        if self.org_key and self.instance_key:
            return f"{self.org_key}-{self.instance_key}"
        # SherpaDesk docs indicate `x:{api_token}` can be used to discover organizations.
        return "x"

    def _build_headers(self) -> dict[str, str]:
        credentials = f"{self._auth_identity()}:{self.api_key}".encode("utf-8")
        encoded = base64.b64encode(credentials).decode("ascii")
        headers = {
            "Accept": "application/json",
            "Authorization": f"Basic {encoded}",
        }
        if self.api_user:
            headers["X-API-User"] = self.api_user
        return headers

    def _build_url(self, path: str) -> str:
        return f"{self.api_base_url.rstrip('/')}/{path.lstrip('/')}"

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=16),
        retry=retry_if_exception_type(httpx.HTTPError),
        reraise=True,
    )
    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        self.pacer.wait()
        with httpx.Client(timeout=self.timeout_seconds, headers=self._build_headers()) as client:
            response = client.get(self._build_url(path), params=params)
            response.raise_for_status()
            content_type = response.headers.get("content-type", "")
            if "json" in content_type.lower():
                return response.json()
            return response.text

    def discover_organizations(self) -> Any:
        return self.get("organizations/")
