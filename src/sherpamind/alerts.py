from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib import request, error

from .analysis import search_ticket_document_chunks
from .client import SherpaDeskClient
from .db import enqueue_derived_refresh, mark_alert_failed, mark_alert_sent, mark_ticket_update_alert_sent, now_iso, upsert_ticket_details
from .settings import Settings
from .summaries import get_ticket_summary

DEFAULT_AGENT_HOOK_URL = "http://127.0.0.1:18789/hooks/agent"
DEFAULT_ALERT_TARGET: str | None = None
DEFAULT_AGENT_ID = "main"
DEFAULT_TIMEOUT_SECONDS = 30
MAX_ALERT_TEXT_CHARS = 1400
MAX_UPDATE_NOTE_CHARS = 450
MAX_SIMILAR_EVIDENCE = 3


def _compact_text(value: Any, limit: int) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    if not text:
        return None
    return text[:limit]


def _query_terms(*values: Any) -> str:
    text = " ".join(str(value or "") for value in values)
    words = []
    stop = {"the", "and", "for", "with", "that", "this", "from", "ticket", "issue", "user", "unable", "cannot"}
    for raw in text.replace("/", " ").replace("-", " ").split():
        word = "".join(ch for ch in raw.lower() if ch.isalnum())
        if len(word) < 4 or word in stop or word in words:
            continue
        words.append(word)
        if len(words) >= 6:
            break
    return " ".join(words) or " ".join(str(value or "") for value in values if value)[:80]


def _similar_ticket_evidence(settings: Settings, *, ticket_id: str, subject: Any, issue_text: Any, account: Any = None) -> list[dict[str, Any]]:
    """Return small, local-only prior-ticket hints for alert synthesis.

    This intentionally does not ask the LLM to run searches. We spend cheap local
    SQLite lookups to avoid hidden model/tool turns while still giving the alert
    prompt enough evidence to mention a prior pattern when one is obvious.
    """
    query = _query_terms(subject, issue_text)
    terms = [query, *query.split()] if query.strip() else []
    evidence: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_rows(rows: list[dict[str, Any]]) -> None:
        for row in rows:
            other_id = str(row.get("ticket_id"))
            if other_id == str(ticket_id) or other_id in seen:
                continue
            seen.add(other_id)
            evidence.append({
                "ticket_id": other_id,
                "ticket_number": row.get("ticket_number"),
                "ticket_key": row.get("ticket_key"),
                "subject": row.get("cleaned_subject"),
                "updated_at": row.get("updated_at"),
                "match_scope": "same_account" if scoped_account else "global",
                "status": row.get("status"),
                "account": row.get("account"),
                "class_name": row.get("class_name"),
                "resolution_category": row.get("resolution_category"),
                "resolution_summary": _compact_text(row.get("resolution_summary"), 220),
                "snippet": _compact_text(row.get("text"), 260),
            })
            if len(evidence) >= MAX_SIMILAR_EVIDENCE:
                return

    scoped_accounts = [str(account)] if account else []
    scoped_accounts.append(None)
    for scoped_account in scoped_accounts:
        for term in terms[:4]:
            if len(evidence) >= MAX_SIMILAR_EVIDENCE:
                return evidence
            rows = search_ticket_document_chunks(settings.db_path, term, limit=8, account=scoped_account, max_text_chars=320)
            add_rows(rows)
    return evidence


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

    initial_post = _compact_text(metadata.get("cleaned_initial_post") or metadata.get("cleaned_subject") or ticket.get("subject"), MAX_ALERT_TEXT_CHARS)

    return {
        "ticket": ticket,
        "artifact_stats": {k: artifact_stats.get(k) for k in ("detail_available", "document_available", "log_count", "attachment_count")},
        "initial_post_context": initial_post,
        "support_group_name": metadata.get("support_group_name"),
        "default_contract_name": metadata.get("default_contract_name"),
        "department_label": metadata.get("department_label"),
        "project_name": metadata.get("project_name"),
        "location_name": metadata.get("location_name") or metadata.get("account_location_name"),
    }


def _hook_request_payload(*, alert_channel: str | None, name: str, prompt: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "agentId": DEFAULT_AGENT_ID,
        "name": name,
        "message": prompt,
        "wakeMode": "now",
        "deliver": True,
        "channel": "discord",
        "timeoutSeconds": 180,
    }
    if alert_channel:
        payload["to"] = alert_channel
    return payload


def _build_hook_payload(settings: Settings, ticket_id: str, summary: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_ticket_summary(summary)
    alert_channel = settings.new_ticket_alert_channel or DEFAULT_ALERT_TARGET
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
        "similar_ticket_evidence": _similar_ticket_evidence(
            settings,
            ticket_id=str(ticket_id),
            subject=normalized["ticket"].get("subject"),
            issue_text=normalized["initial_post_context"],
            account=normalized["ticket"].get("account"),
        ),
    }

    prompt = (
        "A new SherpaDesk ticket was detected by SherpaMind. "
        "Write a concise triage alert optimized for quick scanning in chat channels like Discord or Slack. "
        "For this new-ticket synopsis, use the INITIAL POST / original user-submitted issue only as the issue narrative. "
        "Do not incorporate later technician updates, follow-up notes, resolution thinking, later log history, or inferred post-resolution context into the issue summary. "
        "You may still use requester/client/priority/category metadata, but the problem description should reflect only the initial user-reported issue. "
        "Expand the issue synopsis enough to preserve the user's actual perspective and practical context: write 3-5 short sentences or bullet-style lines, not a one-line over-compression. "
        "The issue synopsis should explain what is happening, what the user noticed, any stated impact or symptoms, and any concrete context the user supplied. "
        "Then produce 3-5 bullet points for 'First checks / next steps'. Those bullets should include the most practical immediate follow-up questions, obvious checks, likely diagnostic directions, or likely fixes worth considering next. "
        "Use the provided similar_ticket_evidence first for prior-issue comparison. If, and only if, a targeted SherpaMind skill/query lookup would materially improve alert accuracy, you may run one narrow retrieval before writing the alert; do not do broad exploratory searches. "
        "If the provided evidence contains a strong match, fold the learned pattern into the recommended next steps in a concrete way. "
        "Only claim 'seen before' when confidence is high based on provided evidence; if confidence is weak or absent, say no high-confidence match was found instead of bluffing. "
        "If you do find a high-confidence similar prior issue, include a line like '- Seen before: yes — similar to #<ticket-number> (<very short reason>)'. If not, include '- Seen before: no high-confidence match found'. "
        "Format it like a compact plain-text card, not a prose paragraph and not a markdown table. Prefer this structure exactly when possible: \n"
        "**NEW TICKET** <priority if useful>\n"
        "- Client: ...\n"
        "- From: ...\n"
        "- Issue synopsis:\n"
        "  - ...\n"
        "  - ...\n"
        "  - ...\n"
        "- Seen before: ...\n"
        "- First checks / next steps:\n"
        "  - ...\n"
        "  - ...\n"
        "  - ...\n"
        "- Ticket: #<id> — <subject>\n"
        "Use short bullets, strong labels, and at-a-glance clarity. Keep it practical, not fluffy.\n\n"
        f"Alert destination: {alert_channel}\n"
        "Return only the final alert card; OpenClaw hook delivery will send it to the requested destination.\n\n"
        f"Ticket context JSON:\n{json.dumps(compact, ensure_ascii=False, separators=(',', ':'))}"
    )

    return _hook_request_payload(
        alert_channel=alert_channel,
        name="SherpaMind New Ticket",
        prompt=prompt,
    )


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
    enqueue_derived_refresh(settings.db_path, ticket_id=str(ticket_id), source="alert_context", priority=20)


def _post_hook_payload(settings: Settings, ticket_id: str, payload: dict[str, Any]) -> AlertDispatchResult:
    body = json.dumps(payload).encode("utf-8")
    webhook_url = settings.openclaw_webhook_url or DEFAULT_AGENT_HOOK_URL
    headers = {"Content-Type": "application/json"}
    if settings.openclaw_webhook_token:
        headers["Authorization"] = f"Bearer {settings.openclaw_webhook_token}"
        headers["x-openclaw-token"] = settings.openclaw_webhook_token
    req = request.Request(
        webhook_url,
        data=body,
        headers=headers,
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=DEFAULT_TIMEOUT_SECONDS) as resp:
            return AlertDispatchResult(status="ok", ticket_id=str(ticket_id), response_status=getattr(resp, "status", None), message="hook_dispatched")
    except error.HTTPError as exc:
        return AlertDispatchResult(status="error", ticket_id=str(ticket_id), response_status=exc.code, message=f"http_error:{exc.code}")
    except Exception as exc:  # noqa: BLE001
        return AlertDispatchResult(status="error", ticket_id=str(ticket_id), message=f"request_error:{type(exc).__name__}:{exc}")


def _build_ticket_update_payload(settings: Settings, ticket_id: str, summary: dict[str, Any]) -> dict[str, Any]:
    ticket = summary.get("ticket") or {}
    metadata = summary.get("retrieval_metadata") or {}
    alert_channel = settings.ticket_update_alert_channel or settings.new_ticket_alert_channel or DEFAULT_ALERT_TARGET
    initial_or_subject = metadata.get("cleaned_initial_post") or metadata.get("cleaned_subject") or ticket.get("subject")
    recent_logs = [
        {
            "type": row.get("log_type"),
            "date": row.get("record_date"),
            "is_tech_only": row.get("is_tech_only"),
            "note": _compact_text(row.get("plain_note") or row.get("note"), MAX_UPDATE_NOTE_CHARS),
        }
        for row in (summary.get("recent_logs") or [])[:5]
    ]
    compact = {
        "ticket_id": str(ticket_id),
        "ticket_number": ticket.get("ticket_number"),
        "ticket_key": ticket.get("ticket_key"),
        "subject": ticket.get("subject"),
        "status": ticket.get("status"),
        "priority": ticket.get("priority"),
        "category": ticket.get("category"),
        "account": ticket.get("account"),
        "requester": ticket.get("user_name") or ticket.get("user_email"),
        "technician": ticket.get("technician"),
        "context": {
            "initial_issue_text": _compact_text(initial_or_subject, 800),
            "action_cue": _compact_text(metadata.get("cleaned_action_cue"), 500),
            "followup_note": _compact_text(metadata.get("cleaned_followup_note"), 500),
            "request_completion_note": _compact_text(metadata.get("cleaned_request_completion_note"), 500),
            "recent_logs": recent_logs,
        },
        "artifact_stats": {k: (summary.get("artifact_stats") or {}).get(k) for k in ("detail_available", "document_available", "log_count", "attachment_count")},
        "similar_ticket_evidence": _similar_ticket_evidence(settings, ticket_id=str(ticket_id), subject=ticket.get("subject"), issue_text=initial_or_subject, account=ticket.get("account")),
    }
    prompt = (
        "A SherpaDesk ticket received a new NON-TECH update from the requester/customer side. "
        "Write a concise update synopsis optimized for quick scanning in chat channels like Discord or Slack. "
        "For this update synopsis, you may use the broader ticket history and recent log context to explain what changed and what the new user-side update means. "
        "Expand the update synopsis enough to preserve the real meaning of the thread: write 3-5 short sentences or bullet-style lines, not a one-line over-compression. "
        "The synopsis should explain what the requester just added, the relevant context from prior ticket history, and what this update changes about the likely state of the issue. "
        "Then produce 3-5 bullet points for recommended follow-up / next steps. Those bullets should include the most practical immediate questions, obvious checks, likely diagnostic directions, likely resolutions, or specific follow-up actions worth taking next. "
        "Use the provided similar_ticket_evidence first for prior-issue comparison. If, and only if, a targeted SherpaMind skill/query lookup would materially improve alert accuracy, you may run one narrow retrieval before writing the alert; do not do broad exploratory searches. "
        "If the provided evidence contains a strong match, fold the learned pattern into the next-step bullets in a concrete way. "
        "Only claim 'seen before' when confidence is high based on provided evidence; if confidence is weak or absent, state that no high-confidence match was found. "
        "If you do find a high-confidence similar prior issue, include a line like '- Seen before: yes — similar to #<ticket-number> (<very short reason>)'. Otherwise include '- Seen before: no high-confidence match found'. "
        "Format it like a compact plain-text card, not a prose paragraph and not a markdown table. Prefer this structure exactly when possible: \n"
        "**USER UPDATE** <priority/status if useful>\n"
        "- Client: ...\n"
        "- From: ...\n"
        "- Update synopsis:\n"
        "  - ...\n"
        "  - ...\n"
        "  - ...\n"
        "- Seen before: ...\n"
        "- Current state: ...\n"
        "- Recommended follow-up / next steps:\n"
        "  - ...\n"
        "  - ...\n"
        "  - ...\n"
        "- Ticket: #<id> — <subject>\n"
        "Use short bullets, strong labels, and at-a-glance clarity. Keep it practical, not fluffy.\n\n"
        f"Alert destination: {alert_channel}\n"
        "Return only the final alert card; OpenClaw hook delivery will send it to the requested destination.\n\n"
        f"Ticket context JSON:\n{json.dumps(compact, ensure_ascii=False, separators=(',', ':'))}"
    )
    return _hook_request_payload(
        alert_channel=alert_channel,
        name="SherpaMind Ticket Update",
        prompt=prompt,
    )


def dispatch_new_ticket_alert(settings: Settings, ticket_id: str) -> AlertDispatchResult:
    _refresh_ticket_context(settings, str(ticket_id))
    summary = get_ticket_summary(settings.db_path, str(ticket_id), limit_logs=5, limit_attachments=5)
    if summary.get("status") != "ok":
        return AlertDispatchResult(status="skipped", ticket_id=str(ticket_id), message="ticket_summary_unavailable")

    payload = _build_hook_payload(settings, str(ticket_id), summary)
    return _post_hook_payload(settings, str(ticket_id), payload)


def dispatch_ticket_update_alert(settings: Settings, ticket_id: str) -> AlertDispatchResult:
    _refresh_ticket_context(settings, str(ticket_id))
    summary = get_ticket_summary(settings.db_path, str(ticket_id), limit_logs=10, limit_attachments=5)
    if summary.get("status") != "ok":
        return AlertDispatchResult(status="skipped", ticket_id=str(ticket_id), message="ticket_summary_unavailable")

    payload = _build_ticket_update_payload(settings, str(ticket_id), summary)
    return _post_hook_payload(settings, str(ticket_id), payload)


def dispatch_queued_alert(settings: Settings, alert_row: dict[str, Any]) -> AlertDispatchResult:
    alert_type = str(alert_row.get("alert_type") or "").strip()
    ticket_id = str(alert_row.get("ticket_id") or "")
    if not ticket_id:
        return AlertDispatchResult(status="error", ticket_id="", message="missing_ticket_id")
    if alert_type == "new_ticket":
        return dispatch_new_ticket_alert(settings, ticket_id)
    if alert_type == "ticket_update":
        return dispatch_ticket_update_alert(settings, ticket_id)
    return AlertDispatchResult(status="error", ticket_id=ticket_id, message=f"unknown_alert_type:{alert_type}")


def _alert_payload_json(alert_row: dict[str, Any]) -> dict[str, Any]:
    raw = alert_row.get("payload_json")
    if not raw:
        return {}
    try:
        payload = json.loads(str(raw))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def finalize_queued_alert(settings: Settings, alert_row: dict[str, Any], result: AlertDispatchResult, *, retry_after_seconds: int = 120) -> None:
    alert_id = int(alert_row["id"])
    if result.status == "ok":
        mark_alert_sent(settings.db_path, alert_id)
        if str(alert_row.get("alert_type") or "") == "ticket_update":
            payload = _alert_payload_json(alert_row)
            event_key = payload.get("event_key") or str(alert_row.get("ticket_id"))
            mark_ticket_update_alert_sent(settings.db_path, str(alert_row.get("ticket_id")), str(event_key))
    else:
        mark_alert_failed(settings.db_path, alert_id, result.message or result.status, retry_after_seconds=retry_after_seconds)
