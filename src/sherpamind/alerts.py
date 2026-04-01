from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib import request, error

from .client import SherpaDeskClient
from .db import now_iso, upsert_ticket_details
from .documents import materialize_ticket_documents
from .settings import Settings
from .summaries import get_ticket_summary

DEFAULT_AGENT_HOOK_URL = "http://127.0.0.1:18789/hooks/agent"
DEFAULT_ALERT_CHANNEL = "channel:1488924125736079492"
DEFAULT_AGENT_ID = "main"
DEFAULT_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class AlertDispatchResult:
    status: str
    ticket_id: str
    response_status: int | None = None
    message: str | None = None


def _normalize_ticket_summary(summary: dict[str, Any]) -> dict[str, Any]:
    ticket = summary.get("ticket") or {}
    metadata = summary.get("retrieval_metadata") or {}
    artifact_stats = summary.get("artifact_stats") or {}

    initial_post = metadata.get("cleaned_initial_post") or metadata.get("cleaned_subject") or ticket.get("subject")

    return {
        "ticket": ticket,
        "artifact_stats": artifact_stats,
        "initial_post_context": initial_post,
        "support_group_name": metadata.get("support_group_name"),
        "default_contract_name": metadata.get("default_contract_name"),
        "department_label": metadata.get("department_label"),
        "project_name": metadata.get("project_name"),
        "location_name": metadata.get("location_name") or metadata.get("account_location_name"),
    }


def _build_hook_payload(settings: Settings, ticket_id: str, summary: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_ticket_summary(summary)
    alert_channel = settings.new_ticket_alert_channel or DEFAULT_ALERT_CHANNEL
    compact = {
        "ticket_id": str(ticket_id),
        "ticket_number": normalized["ticket"].get("ticket_number"),
        "ticket_key": normalized["ticket"].get("ticket_key"),
        "subject": normalized["ticket"].get("subject"),
        "status": normalized["ticket"].get("status"),
        "priority": normalized["ticket"].get("priority"),
        "category": normalized["ticket"].get("category"),
        "account": normalized["ticket"].get("account"),
        "requester": normalized["ticket"].get("user_name") or normalized["ticket"].get("user_email"),
        "technician": normalized["ticket"].get("technician"),
        "initial_post_context": normalized["initial_post_context"],
        "support_group_name": normalized["support_group_name"],
        "default_contract_name": normalized["default_contract_name"],
        "department_label": normalized["department_label"],
        "project_name": normalized["project_name"],
        "location_name": normalized["location_name"],
        "artifact_stats": normalized["artifact_stats"],
    }

    prompt = (
        "A new SherpaDesk ticket was detected by SherpaMind. "
        "Write a concise triage alert for Discord for the technician who may pick it up. "
        "For this new-ticket synopsis, use the INITIAL POST / original user-submitted issue only as the issue narrative. "
        "Do not incorporate later technician updates, follow-up notes, resolution thinking, later log history, or inferred post-resolution context into the issue summary. "
        "You may still use the requester/client/priority/category metadata fields, but the problem description should reflect only the initial user-reported issue. "
        "Include: who sent it in (if known), which client/account it is for, a crisp breakdown of the initial issue, and likely first checks / next steps for whoever picks it up. "
        "Be practical, not fluffy. Use bullets, not a markdown table. Keep it readable in Discord. End with the ticket id and subject.\n\n"
        f"Alert destination: {alert_channel}\n"
        "When you are done, send the final alert message to that Discord target using delivery announce to that channel.\n\n"
        f"Ticket context JSON:\n{json.dumps(compact, ensure_ascii=False, indent=2)}"
    )

    return {
        "agentId": DEFAULT_AGENT_ID,
        "name": "SherpaMind New Ticket",
        "message": prompt,
        "wakeMode": "now",
        "deliver": True,
        "channel": "discord",
        "to": alert_channel,
        "timeoutSeconds": 180,
    }


def _build_client(settings: Settings) -> SherpaDeskClient:
    assert settings.api_key is not None
    return SherpaDeskClient(
        api_base_url=settings.api_base_url,
        api_key=settings.api_key,
        api_user=settings.api_user,
        org_key=settings.org_key,
        instance_key=settings.instance_key,
        timeout_seconds=settings.request_timeout_seconds,
        min_interval_seconds=settings.request_min_interval_seconds,
        request_tracking_db_path=settings.db_path,
    )


def _refresh_ticket_context(settings: Settings, ticket_id: str) -> None:
    client = _build_client(settings)
    detail = client.get(f"tickets/{ticket_id}")
    upsert_ticket_details(settings.db_path, [detail], synced_at=now_iso())
    materialize_ticket_documents(settings.db_path, limit=None)


def dispatch_new_ticket_alert(settings: Settings, ticket_id: str) -> AlertDispatchResult:
    _refresh_ticket_context(settings, str(ticket_id))
    summary = get_ticket_summary(settings.db_path, str(ticket_id), limit_logs=5, limit_attachments=5)
    if summary.get("status") != "ok":
        return AlertDispatchResult(status="skipped", ticket_id=str(ticket_id), message="ticket_summary_unavailable")

    payload = _build_hook_payload(settings, str(ticket_id), summary)
    body = json.dumps(payload).encode("utf-8")
    webhook_url = settings.openclaw_webhook_url or DEFAULT_AGENT_HOOK_URL
    req = request.Request(
        webhook_url,
        data=body,
        headers={
            "Content-Type": "application/json",
            **({"Authorization": f"Bearer {settings.openclaw_webhook_token}"} if settings.openclaw_webhook_token else {}),
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=DEFAULT_TIMEOUT_SECONDS) as resp:
            return AlertDispatchResult(status="ok", ticket_id=str(ticket_id), response_status=getattr(resp, "status", None), message="hook_dispatched")
    except error.HTTPError as exc:
        return AlertDispatchResult(status="error", ticket_id=str(ticket_id), response_status=exc.code, message=f"http_error:{exc.code}")
    except Exception as exc:  # noqa: BLE001
        return AlertDispatchResult(status="error", ticket_id=str(ticket_id), message=f"request_error:{type(exc).__name__}:{exc}")
