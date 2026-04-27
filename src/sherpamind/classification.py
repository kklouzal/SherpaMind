from __future__ import annotations

import json
from typing import Any
from urllib import error, request

from .db import (
    enqueue_ticket_classification_event,
    lease_ticket_classification_events,
    list_ticket_taxonomy_classes,
    mark_ticket_classification_dispatched,
    mark_ticket_classification_failed,
    record_ticket_classification_result,
)
from .settings import Settings
from .summaries import get_ticket_summary

DEFAULT_TIMEOUT_SECONDS = 30
MAX_CONTEXT_CHARS_INITIAL = 1200
MAX_CONTEXT_CHARS_FINAL = 2200
MAX_RATIONALE_CHARS = 240
MAX_CLASS_CANDIDATES = 140


def _compact_text(value: Any, limit: int) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    if not text:
        return None
    return text[:limit]


def _class_candidate_lines(settings: Settings) -> list[str]:
    rows = list_ticket_taxonomy_classes(settings.db_path, active_only=True, leaves_only=True)
    if not rows:
        rows = list_ticket_taxonomy_classes(settings.db_path, active_only=True, leaves_only=False)
    return [f"{row['id']}:{row['path']}" for row in rows[:MAX_CLASS_CANDIDATES]]


def _ticket_brief(ticket: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": ticket.get("id"),
        "ticket_number": ticket.get("ticket_number"),
        "ticket_key": ticket.get("ticket_key"),
        "subject": ticket.get("subject"),
        "status": ticket.get("status"),
        "priority": ticket.get("priority"),
        "category": ticket.get("category"),
        "account": ticket.get("account"),
        "requester": ticket.get("user_name") or ticket.get("user_email"),
        "technician": ticket.get("technician"),
        "created_at": ticket.get("created_at"),
        "updated_at": ticket.get("updated_at"),
        "closed_at": ticket.get("closed_at"),
    }


def _event_payload(ticket: dict[str, Any], event_type: str, trigger_source: str) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "trigger_source": trigger_source,
        "ticket_id": str(ticket.get("id")),
        "status": ticket.get("status"),
        "subject": ticket.get("subject"),
        "account_name": ticket.get("account_name"),
        "priority": ticket.get("priority_name") or ticket.get("priority"),
        "created_time": ticket.get("created_time"),
        "updated_time": ticket.get("updated_time"),
        "closed_time": ticket.get("closed_time"),
        "current_class_id": str(ticket.get("class_id")) if ticket.get("class_id") is not None else None,
        "current_class_name": ticket.get("class_name"),
    }


def enqueue_initial_ticket_classification(settings: Settings, ticket: dict[str, Any], *, trigger_source: str) -> dict[str, Any]:
    if not settings.classification_enabled:
        return {"status": "disabled"}
    ticket_id = str(ticket.get("id"))
    if not ticket_id or ticket_id == "None":
        return {"status": "skipped", "reason": "missing_ticket_id"}
    dedupe_key = f"classification:initial:{ticket_id}"
    return enqueue_ticket_classification_event(
        settings.db_path,
        ticket_id=ticket_id,
        event_type="initial",
        dedupe_key=dedupe_key,
        trigger_source=trigger_source,
        payload=_event_payload(ticket, "initial", trigger_source),
        ticket_status=ticket.get("status"),
        ticket_updated_time=ticket.get("updated_time"),
        current_class_id=str(ticket.get("class_id")) if ticket.get("class_id") is not None else None,
        current_class_name=ticket.get("class_name"),
    )


def enqueue_final_ticket_classification(settings: Settings, ticket: dict[str, Any], *, trigger_source: str) -> dict[str, Any]:
    if not settings.classification_enabled:
        return {"status": "disabled"}
    ticket_id = str(ticket.get("id"))
    if not ticket_id or ticket_id == "None":
        return {"status": "skipped", "reason": "missing_ticket_id"}
    closed_key = ticket.get("closed_time") or ticket.get("updated_time") or "unknown"
    dedupe_key = f"classification:final:{ticket_id}:{closed_key}"
    return enqueue_ticket_classification_event(
        settings.db_path,
        ticket_id=ticket_id,
        event_type="final",
        dedupe_key=dedupe_key,
        trigger_source=trigger_source,
        payload=_event_payload(ticket, "final", trigger_source),
        ticket_status=ticket.get("status"),
        ticket_updated_time=ticket.get("updated_time"),
        current_class_id=str(ticket.get("class_id")) if ticket.get("class_id") is not None else None,
        current_class_name=ticket.get("class_name"),
    )


def _build_context(settings: Settings, event: dict[str, Any]) -> dict[str, Any]:
    event_type = str(event.get("event_type") or "")
    ticket_id = str(event.get("ticket_id"))
    summary = get_ticket_summary(settings.db_path, ticket_id, limit_logs=4 if event_type == "final" else 2, limit_attachments=3)
    payload = json.loads(event.get("payload_json") or "{}")
    if summary.get("status") != "ok":
        return {"event": payload, "summary_status": summary.get("status")}
    ticket = summary.get("ticket") or {}
    metadata = summary.get("retrieval_metadata") or {}
    logs = summary.get("recent_logs") or []
    if event_type == "initial":
        return {
            "event": payload,
            "ticket": _ticket_brief(ticket),
            "initial_issue_text": _compact_text(metadata.get("cleaned_initial_post") or metadata.get("cleaned_subject") or ticket.get("subject"), MAX_CONTEXT_CHARS_INITIAL),
            "metadata": {
                "department_label": metadata.get("department_label"),
                "support_group_name": metadata.get("support_group_name"),
                "location_name": metadata.get("location_name") or metadata.get("account_location_name"),
            },
        }
    return {
        "event": payload,
        "ticket": _ticket_brief(ticket),
        "final_context": {
            "initial_issue_text": _compact_text(metadata.get("cleaned_initial_post") or metadata.get("cleaned_subject") or ticket.get("subject"), 700),
            "resolution_summary": _compact_text(metadata.get("resolution_summary"), MAX_CONTEXT_CHARS_FINAL),
            "action_cue": _compact_text(metadata.get("cleaned_action_cue"), 500),
            "recent_logs": [
                {
                    "type": row.get("log_type"),
                    "date": row.get("record_date"),
                    "note": _compact_text(row.get("plain_note") or row.get("note"), 450),
                }
                for row in logs[:4]
            ],
        },
    }


def build_classification_prompt(settings: Settings, event: dict[str, Any]) -> dict[str, Any]:
    candidate_lines = _class_candidate_lines(settings)
    context = _build_context(settings, event)
    event_id = int(event["id"])
    payload = {
        "event_id": event_id,
        "event_type": event.get("event_type"),
        "ticket_id": event.get("ticket_id"),
        "task": "Choose exactly one class_id from candidate_classes. Prefer leaf/sub-class ids. Use only the provided ticket context and taxonomy.",
        "candidate_format": "Each candidate is class_id:path.",
        "accuracy_policy": "High accuracy first, but no chain-of-thought output. If uncertain, choose the closest allowed class and mark confidence low.",
        "token_policy": "Do not do retrieval or broad ticket searches. Do not inspect unrelated tickets. This classification is event-scoped and should use the compact context below.",
        "record_command": f"cd /home/kklouzal/SherpaMind && python3 scripts/run.py record-ticket-classification --event-id {event_id} --class-id <CLASS_ID> --confidence <high|medium|low> --rationale '<240-char reason>'",
        "candidate_classes": candidate_lines,
        "ticket_context": context,
    }
    message = (
        "SherpaMind needs one ticket class/sub-class classification.\n"
        "Use the JSON payload below only; do not perform extra searches unless the payload is malformed.\n"
        "Pick exactly one candidate class id. Then run the record_command with your chosen class id, confidence, and a concise reason.\n"
        "Do not send chat output to the user.\n\n"
        f"CLASSIFICATION_PAYLOAD_JSON:\n{json.dumps(payload, ensure_ascii=False, separators=(',', ':'))}"
    )
    hook_payload: dict[str, Any] = {
        "agentId": "main",
        "name": "SherpaMind Ticket Classification",
        "message": message,
        "wakeMode": "now",
        "deliver": False,
        "timeoutSeconds": 180,
    }
    if settings.classification_model:
        hook_payload["model"] = settings.classification_model
    return hook_payload


def _post_hook_payload(settings: Settings, payload: dict[str, Any]) -> tuple[bool, str]:
    if not settings.openclaw_webhook_url:
        return False, "missing_openclaw_webhook_url"
    body = json.dumps(payload).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if settings.openclaw_webhook_token:
        headers["Authorization"] = f"Bearer {settings.openclaw_webhook_token}"
        headers["x-openclaw-token"] = settings.openclaw_webhook_token
    req = request.Request(settings.openclaw_webhook_url, data=body, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=DEFAULT_TIMEOUT_SECONDS) as resp:
            return True, f"http:{getattr(resp, 'status', 'ok')}"
    except error.HTTPError as exc:
        return False, f"http_error:{exc.code}"
    except Exception as exc:  # noqa: BLE001
        return False, f"request_error:{type(exc).__name__}:{exc}"


def dispatch_ticket_classification_events(settings: Settings, *, limit: int = 2) -> dict[str, Any]:
    if not settings.classification_enabled:
        return {"status": "disabled", "dispatched": 0, "failed": 0}
    events = lease_ticket_classification_events(settings.db_path, limit=limit)
    results: list[dict[str, Any]] = []
    for event in events:
        prompt = build_classification_prompt(settings, event)
        ok, message = _post_hook_payload(settings, prompt)
        if ok:
            mark_ticket_classification_dispatched(settings.db_path, int(event["id"]), prompt=prompt)
            results.append({"event_id": event["id"], "status": "dispatched", "message": message})
        else:
            mark_ticket_classification_failed(settings.db_path, int(event["id"]), message)
            results.append({"event_id": event["id"], "status": "failed", "message": message})
    return {
        "status": "ok",
        "leased": len(events),
        "dispatched": sum(1 for row in results if row["status"] == "dispatched"),
        "failed": sum(1 for row in results if row["status"] == "failed"),
        "results": results,
    }


def record_classification(settings: Settings, *, event_id: int, class_id: str, confidence: str, rationale: str) -> dict[str, Any]:
    confidence = confidence.strip().lower()
    if confidence not in {"high", "medium", "low"}:
        raise ValueError("confidence must be high, medium, or low")
    return record_ticket_classification_result(
        settings.db_path,
        event_id=event_id,
        class_id=class_id,
        confidence=confidence,
        rationale=_compact_text(rationale, MAX_RATIONALE_CHARS) or "No rationale provided.",
    )
