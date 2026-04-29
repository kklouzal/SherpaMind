from __future__ import annotations

import json
from typing import Any
from urllib import error, request

import httpx

from .client import SherpaDeskClient
from .db import (
    connect,
    enqueue_ticket_classification_event,
    get_ticket_taxonomy_class,
    get_ticket_taxonomy_freshness,
    lease_ticket_classification_events,
    list_ticket_taxonomy_classes,
    mark_ticket_classification_dispatched,
    mark_ticket_classification_failed,
    now_iso,
    record_ticket_classification_result,
    upsert_ticket_details,
    upsert_tickets,
)
from .documents import materialize_ticket_documents
from .settings import Settings
from .summaries import get_ticket_summary
from .taxonomy import ensure_ticket_classes_fresh

DEFAULT_TIMEOUT_SECONDS = 30
MAX_CONTEXT_CHARS_INITIAL = 1200
MAX_CONTEXT_CHARS_FINAL = 2200
MAX_RATIONALE_CHARS = 240
MAX_CLASS_CANDIDATES = 140
PERMANENT_WRITEBACK_STATUS_CODES = {400, 404, 422}


def _compact_text(value: Any, limit: int) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    if not text:
        return None
    return text[:limit]


def _class_candidates(settings: Settings) -> dict[str, Any]:
    rows = list_ticket_taxonomy_classes(settings.db_path, active_only=True, leaves_only=True)
    if not rows:
        rows = list_ticket_taxonomy_classes(settings.db_path, active_only=True, leaves_only=False)
    included = rows[:MAX_CLASS_CANDIDATES]
    return {
        "total": len(rows),
        "included": len(included),
        "truncated": len(rows) > len(included),
        "lines": [f"{row['id']}:{row['path']}" for row in included],
    }


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



def _confidence_rank(value: str | None) -> int:
    return {"low": 1, "medium": 2, "high": 3}.get(str(value or "").strip().lower(), 0)


def _build_client(settings: Settings) -> SherpaDeskClient:
    if not settings.api_key:
        raise ValueError("SHERPADESK_API_KEY is required for classification write-back")
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


def _mark_writeback(db_path, event_id: int, status: str, *, error_message: str | None = None, response: Any = None) -> None:
    now = now_iso()
    with connect(db_path) as conn:
        conn.execute(
            """
            UPDATE ticket_classification_events
            SET writeback_status = ?,
                writeback_attempt_count = writeback_attempt_count + 1,
                writeback_at = CASE WHEN ? = 'succeeded' THEN ? ELSE writeback_at END,
                writeback_last_error = ?,
                writeback_response_json = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                status,
                status,
                now,
                error_message[:1000] if error_message else None,
                json.dumps(response, sort_keys=True)[:4000] if response is not None else None,
                now,
                event_id,
            ),
        )
        conn.commit()


def _eligible_writeback_events(settings: Settings, *, limit: int) -> list[dict[str, Any]]:
    min_confidence = settings.classification_writeback_min_confidence
    min_rank = _confidence_rank(min_confidence)
    with connect(settings.db_path) as conn:
        rows = conn.execute(
            """
            SELECT * FROM ticket_classification_events
            WHERE status = 'completed'
              AND result_class_id IS NOT NULL
              AND COALESCE(writeback_status, 'pending') IN ('pending', 'retry')
              AND writeback_attempt_count < 3
            ORDER BY completed_at ASC, id ASC
            LIMIT ?
            """,
            (limit * 4,),
        ).fetchall()
    eligible = []
    for row in rows:
        item = dict(row)
        if _confidence_rank(item.get("confidence")) >= min_rank:
            eligible.append(item)
        if len(eligible) >= limit:
            break
    return eligible


def _validate_writeback_class(settings: Settings, class_id: str) -> tuple[bool, str | None, dict[str, Any] | None]:
    taxonomy = get_ticket_taxonomy_class(settings.db_path, str(class_id))
    if taxonomy is None:
        return False, f"unknown_class_id:{class_id}", None
    if int(taxonomy.get("is_active") if taxonomy.get("is_active") is not None else 1) != 1:
        return False, f"inactive_class_id:{class_id}", taxonomy
    if int(taxonomy.get("is_lastchild") if taxonomy.get("is_lastchild") is not None else 0) != 1:
        return False, f"non_leaf_class_id:{class_id}", taxonomy
    return True, None, taxonomy


def _refresh_ticket_after_class_writeback(settings: Settings, client: SherpaDeskClient, ticket_id: str) -> dict[str, Any]:
    detail = client.get(f"tickets/{ticket_id}")
    if not isinstance(detail, dict):
        return {"status": "skipped", "reason": f"unexpected_detail_shape:{type(detail).__name__}"}
    synced_at = now_iso()
    upsert_tickets(settings.db_path, [detail], synced_at=synced_at)
    upsert_ticket_details(settings.db_path, [detail], synced_at=synced_at)
    materialize_ticket_documents(settings.db_path, ticket_ids=[str(ticket_id)])
    return {
        "status": "ok",
        "class_id": str(detail.get("class_id")) if detail.get("class_id") is not None else None,
        "class_name": detail.get("class_name"),
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


def _current_ticket_class_id(ticket: dict[str, Any]) -> str | None:
    value = ticket.get("class_id")
    if value is None:
        value = ticket.get("ticket_class_id")
    text = str(value).strip() if value is not None else ""
    return text or None


def _has_active_classification_attempt(settings: Settings, ticket_id: str) -> bool:
    with connect(settings.db_path) as conn:
        row = conn.execute(
            """
            SELECT id FROM ticket_classification_events
            WHERE ticket_id = ?
              AND (
                status IN ('pending', 'dispatching', 'awaiting_result')
                OR (status = 'failed' AND attempt_count < 3)
                OR (
                    status = 'completed'
                    AND result_class_id IS NOT NULL
                    AND COALESCE(writeback_status, 'pending') IN ('pending', 'retry')
                    AND writeback_attempt_count < 3
                )
              )
            LIMIT 1
            """,
            (ticket_id,),
        ).fetchone()
    return row is not None


def enqueue_update_ticket_classification_if_unclassified(settings: Settings, ticket: dict[str, Any], *, trigger_source: str) -> dict[str, Any]:
    if not settings.classification_enabled:
        return {"status": "disabled"}
    ticket_id = str(ticket.get("id"))
    if not ticket_id or ticket_id == "None":
        return {"status": "skipped", "reason": "missing_ticket_id"}
    current_class_id = _current_ticket_class_id(ticket)
    if current_class_id is not None:
        return {"status": "skipped", "reason": "already_classified", "ticket_id": ticket_id, "current_class_id": current_class_id}
    if _has_active_classification_attempt(settings, ticket_id):
        return {"status": "skipped", "reason": "active_classification_exists", "ticket_id": ticket_id}
    update_key = ticket.get("updated_time") or "unknown"
    dedupe_key = f"classification:update:{ticket_id}:{update_key}"
    return enqueue_ticket_classification_event(
        settings.db_path,
        ticket_id=ticket_id,
        event_type="update",
        dedupe_key=dedupe_key,
        trigger_source=trigger_source,
        payload=_event_payload(ticket, "update", trigger_source),
        ticket_status=ticket.get("status"),
        ticket_updated_time=ticket.get("updated_time"),
        current_class_id=current_class_id,
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
        current_class_id=_current_ticket_class_id(ticket),
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
    candidates = _class_candidates(settings)
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
        "record_command": (
            "cd /home/kklouzal/SherpaMind && python3 scripts/run.py record-ticket-classification-json "
            "\'{\"event_id\":" + str(event_id) + ",\"class_id\":\"<CLASS_ID>\",\"confidence\":\"<high|medium|low>\",\"rationale\":\"<240-char JSON-safe reason, no newlines>\"}\'"
        ),
        "candidate_count_total": candidates["total"],
        "candidate_count_included": candidates["included"],
        "candidate_truncated": candidates["truncated"],
        "candidate_classes": candidates["lines"],
        "ticket_context": context,
    }
    message = (
        "SherpaMind needs one ticket class/sub-class classification.\n"
        "Use the JSON payload below only. Do not perform extra searches or inspect unrelated tickets.\n"
        "If the payload is malformed or all candidates are unusable, still choose the closest included class and mark confidence low.\n"
        "Pick exactly one candidate class id. Then run the record_command with your chosen class id, confidence, and a concise JSON-safe reason.\n"
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
    try:
        return record_ticket_classification_result(
            settings.db_path,
            event_id=event_id,
            class_id=class_id,
            confidence=confidence,
            rationale=_compact_text(rationale, MAX_RATIONALE_CHARS) or "No rationale provided.",
        )
    except ValueError as exc:
        if settings.api_key and "class" in str(exc).lower():
            client = _build_client(settings)
            ensure_ticket_classes_fresh(client, settings.db_path, max_age_seconds=settings.ticket_class_taxonomy_max_age_seconds, force=True)
            return record_ticket_classification_result(
                settings.db_path,
                event_id=event_id,
                class_id=class_id,
                confidence=confidence,
                rationale=_compact_text(rationale, MAX_RATIONALE_CHARS) or "No rationale provided.",
            )
        raise


def refresh_ticket_class_taxonomy(settings: Settings, *, force: bool = False, client: SherpaDeskClient | None = None) -> dict[str, Any]:
    if not settings.api_key and client is None:
        return {"status": "needs_config", "refreshed": False, "reason": "SHERPADESK_API_KEY is required"}
    client = client or _build_client(settings)
    return ensure_ticket_classes_fresh(client, settings.db_path, max_age_seconds=settings.ticket_class_taxonomy_max_age_seconds, force=force)


def write_back_completed_ticket_classifications(
    settings: Settings,
    *,
    client: SherpaDeskClient | None = None,
    limit: int = 1,
    apply: bool = True,
) -> dict[str, Any]:
    if not settings.classification_writeback_enabled:
        return {"status": "disabled", "processed": 0, "updated": 0, "skipped": 0, "failed": 0, "results": []}
    if not settings.api_key and client is None:
        return {"status": "needs_config", "processed": 0, "updated": 0, "skipped": 0, "failed": 0, "results": [{"reason": "SHERPADESK_API_KEY is required"}]}
    client = client or _build_client(settings)
    freshness = ensure_ticket_classes_fresh(client, settings.db_path, max_age_seconds=settings.ticket_class_taxonomy_max_age_seconds)
    events = _eligible_writeback_events(settings, limit=limit)
    results: list[dict[str, Any]] = []
    for event in events:
        event_id = int(event["id"])
        ticket_id = str(event["ticket_id"])
        class_id = str(event["result_class_id"])
        valid, reason, taxonomy = _validate_writeback_class(settings, class_id)
        if not valid:
            ensure_ticket_classes_fresh(client, settings.db_path, max_age_seconds=settings.ticket_class_taxonomy_max_age_seconds, force=True)
            valid, reason, taxonomy = _validate_writeback_class(settings, class_id)
        if not valid:
            _mark_writeback(settings.db_path, event_id, "permanent_failed", error_message=reason or "invalid_class")
            results.append({"event_id": event_id, "ticket_id": ticket_id, "status": "permanent_failed", "reason": reason})
            continue
        try:
            detail = client.get(f"tickets/{ticket_id}")
            current_class_id = str(detail.get("class_id")) if isinstance(detail, dict) and detail.get("class_id") is not None else event.get("current_class_id")
            if current_class_id == class_id:
                _mark_writeback(settings.db_path, event_id, "skipped_same_class", response={"current_class_id": current_class_id})
                results.append({"event_id": event_id, "ticket_id": ticket_id, "status": "skipped", "reason": "already_set", "class_id": class_id})
                continue
            if not apply:
                results.append({"event_id": event_id, "ticket_id": ticket_id, "status": "dry_run", "from_class_id": current_class_id, "to_class_id": class_id, "class_path": taxonomy["path"] if taxonomy else None})
                continue
            response = client.put(f"tickets/{ticket_id}", data={"class_id": class_id})
            refresh = _refresh_ticket_after_class_writeback(settings, client, ticket_id)
            if str(refresh.get("class_id")) != class_id:
                message = f"writeback_not_confirmed expected={class_id} observed={refresh.get('class_id')}"
                _mark_writeback(settings.db_path, event_id, "retry", error_message=message, response={"response_type": type(response).__name__, "refresh": refresh})
                results.append({"event_id": event_id, "ticket_id": ticket_id, "status": "retry", "error": message})
                continue
            _mark_writeback(settings.db_path, event_id, "succeeded", response={"response_type": type(response).__name__, "refresh": refresh})
            results.append({"event_id": event_id, "ticket_id": ticket_id, "status": "updated", "from_class_id": current_class_id, "to_class_id": class_id, "class_path": taxonomy["path"] if taxonomy else None})
        except httpx.HTTPStatusError as exc:
            status_code = int(exc.response.status_code)
            body = " ".join((exc.response.text or "").split())[:400]
            status = "permanent_failed" if status_code in PERMANENT_WRITEBACK_STATUS_CODES else "retry"
            _mark_writeback(settings.db_path, event_id, status, error_message=f"http_{status_code}:{body}")
            results.append({"event_id": event_id, "ticket_id": ticket_id, "status": status, "http_status": status_code, "error": body})
        except Exception as exc:  # noqa: BLE001
            _mark_writeback(settings.db_path, event_id, "retry", error_message=f"{type(exc).__name__}:{exc}")
            results.append({"event_id": event_id, "ticket_id": ticket_id, "status": "retry", "error": f"{type(exc).__name__}:{exc}"})
    return {
        "status": "ok",
        "apply": apply,
        "taxonomy": freshness,
        "processed": len(results),
        "updated": sum(1 for row in results if row.get("status") == "updated"),
        "skipped": sum(1 for row in results if row.get("status") in {"skipped", "dry_run"}),
        "failed": sum(1 for row in results if row.get("status") in {"retry", "permanent_failed"}),
        "results": results,
    }
