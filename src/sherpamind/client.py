from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import base64

import httpx
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from .db import record_api_request_event
from .rate_limit import RequestPacer


_RETRYABLE_HTTP_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_HTTP_ERROR_BODY_PREVIEW_LIMIT = 400


def is_retryable_http_error(exc: BaseException) -> bool:
    if not isinstance(exc, httpx.HTTPError):
        return False
    if not isinstance(exc, httpx.HTTPStatusError):
        return True
    status_code = int(exc.response.status_code)
    return status_code in _RETRYABLE_HTTP_STATUS_CODES


def _sanitize_http_error_body(text: str | None) -> str | None:
    if text is None:
        return None
    collapsed = " ".join(str(text).split())
    if not collapsed:
        return None
    return collapsed[:_HTTP_ERROR_BODY_PREVIEW_LIMIT]


@dataclass
class SherpaDeskClient:
    api_base_url: str
    api_key: str
    api_user: str | None = None
    org_key: str | None = None
    instance_key: str | None = None
    timeout_seconds: float = 30.0
    min_interval_seconds: float = 8.0
    request_tracking_db_path: Path | None = None
    pacer: RequestPacer = field(init=False)

    def __post_init__(self) -> None:
        self.pacer = RequestPacer(min_interval_seconds=self.min_interval_seconds)

    def _auth_identity(self) -> str:
        if self.org_key and self.instance_key:
            return f"{self.org_key}-{self.instance_key}"
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
        retry=retry_if_exception(is_retryable_http_error),
        reraise=True,
    )
    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        self.pacer.wait()
        try:
            with httpx.Client(timeout=self.timeout_seconds, headers=self._build_headers()) as client:
                response = client.get(self._build_url(path), params=params)
                if self.request_tracking_db_path is not None:
                    record_api_request_event(
                        self.request_tracking_db_path,
                        method="GET",
                        path=path,
                        status_code=response.status_code,
                        outcome="http_response",
                        attempt_kind="get",
                        extra={"params": params or {}},
                    )
                response.raise_for_status()
                content_type = response.headers.get("content-type", "")
                if "json" in content_type.lower():
                    return response.json()
                return response.text
        except httpx.HTTPStatusError as exc:
            if self.request_tracking_db_path is not None:
                response = exc.response
                record_api_request_event(
                    self.request_tracking_db_path,
                    method="GET",
                    path=path,
                    status_code=response.status_code,
                    outcome="http_response",
                    attempt_kind="get",
                    extra={
                        "params": params or {},
                        "detail": str(exc),
                        "response_body_preview": _sanitize_http_error_body(response.text),
                    },
                )
            raise
        except httpx.HTTPError as exc:
            if self.request_tracking_db_path is not None:
                record_api_request_event(
                    self.request_tracking_db_path,
                    method="GET",
                    path=path,
                    status_code=None,
                    outcome="http_error",
                    attempt_kind=type(exc).__name__,
                    extra={"params": params or {}, "detail": str(exc)},
                )
            raise

    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=1, max=16),
        retry=retry_if_exception(is_retryable_http_error),
        reraise=True,
    )
    def put(self, path: str, data: dict[str, Any] | None = None) -> Any:
        self.pacer.wait()
        fields = sorted((data or {}).keys())
        try:
            with httpx.Client(timeout=self.timeout_seconds, headers=self._build_headers()) as client:
                response = client.put(self._build_url(path), data=data or {})
                if self.request_tracking_db_path is not None:
                    record_api_request_event(
                        self.request_tracking_db_path,
                        method="PUT",
                        path=path,
                        status_code=response.status_code,
                        outcome="http_response",
                        attempt_kind="put",
                        extra={"fields": fields},
                    )
                response.raise_for_status()
                content_type = response.headers.get("content-type", "")
                if "json" in content_type.lower() and response.text.strip():
                    return response.json()
                return response.text
        except httpx.HTTPStatusError as exc:
            if self.request_tracking_db_path is not None:
                response = exc.response
                record_api_request_event(
                    self.request_tracking_db_path,
                    method="PUT",
                    path=path,
                    status_code=response.status_code,
                    outcome="http_response",
                    attempt_kind="put",
                    extra={
                        "fields": fields,
                        "detail": str(exc),
                        "response_body_preview": _sanitize_http_error_body(response.text),
                    },
                )
            raise
        except httpx.HTTPError as exc:
            if self.request_tracking_db_path is not None:
                record_api_request_event(
                    self.request_tracking_db_path,
                    method="PUT",
                    path=path,
                    status_code=None,
                    outcome="http_error",
                    attempt_kind=type(exc).__name__,
                    extra={"fields": fields, "detail": str(exc)},
                )
            raise

    def discover_organizations(self) -> Any:
        return self.get("organizations/")

    def list_ticket_classes(self) -> Any:
        return self.get("classes/")

    def list_paginated(
        self,
        path: str,
        *,
        page_size: int = 100,
        max_pages: int | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        page = 0
        while True:
            if max_pages is not None and page >= max_pages:
                break
            params = {"limit": page_size, "page": page}
            if extra_params:
                params.update(extra_params)
            page_items = self.get(path, params=params)
            if not isinstance(page_items, list):
                raise TypeError(f"Expected list response from {path}, got {type(page_items).__name__}")
            items.extend(page_items)
            if len(page_items) < page_size:
                break
            page += 1
        return items
