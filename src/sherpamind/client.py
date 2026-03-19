from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

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
    base_url: str
    api_key: str
    api_user: str | None = None
    timeout_seconds: float = 30.0
    min_interval_seconds: float = 2.0
    pacer: RequestPacer = field(init=False)

    def __post_init__(self) -> None:
        self.pacer = RequestPacer(min_interval_seconds=self.min_interval_seconds)

    def _build_headers(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            # SherpaDesk auth behavior must be verified against the live API/wiki.
            # Keep this implementation conservative and easy to adjust.
            "X-API-Key": self.api_key,
        }
        if self.api_user:
            headers["X-API-User"] = self.api_user
        return headers

    def _build_url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"

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
