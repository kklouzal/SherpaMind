from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .db import connect, now_iso, replace_ticket_document_chunks, replace_ticket_documents
from .text_cleanup import normalize_ticket_text, summarize_resolution_from_logs


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def _split_csv_values(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(',') if part and part.strip()]


def _first_present(*values: str | None) -> str | None:
    for value in values:
        if value is None:
            continue
        cleaned = str(value).strip()
        if cleaned:
            return cleaned
    return None


def _looks_like_identifier(value: str | None) -> bool:
    if value is None:
        return False
    candidate = value.strip()
    return bool(candidate) and candidate.isdigit()


def _join_name_parts(*parts: str | None) -> str | None:
    joined = " ".join(part.strip() for part in parts if part and part.strip())
    return joined or None


def _resolve_account_label(record: dict) -> tuple[str | None, str]:
    joined_name = _first_present(record.get("account"))
    raw_name = _first_present(record.get("raw_account_name"), record.get("raw_account_location_name"))
    account_id = _first_present(record.get("account_id"))
    if joined_name and not _looks_like_identifier(joined_name):
        return joined_name, "joined"
    if raw_name:
        return raw_name, "raw"
    if joined_name:
        return joined_name, "joined"
    if account_id:
        return account_id, "id"
    return None, "missing"


def _resolve_user_label(record: dict) -> tuple[str | None, str]:
    joined_name = _first_present(record.get("user_name"))
    raw_name = _first_present(
        record.get("raw_user_name"),
        _join_name_parts(record.get("raw_user_firstname"), record.get("raw_user_lastname")),
        record.get("raw_user_fullname"),
    )
    user_email = _first_present(record.get("user_email"), record.get("raw_user_email"))
    user_id = _first_present(record.get("user_id"))
    if joined_name and not _looks_like_identifier(joined_name):
        return joined_name, "joined"
    if raw_name:
        return raw_name, "raw"
    if user_email:
        return user_email, "email"
    if joined_name:
        return joined_name, "joined"
    if user_id:
        return user_id, "id"
    return None, "missing"


def _resolve_technician_label(record: dict) -> tuple[str | None, str]:
    joined_name = _first_present(record.get("technician"))
    raw_name = _first_present(
        record.get("raw_assigned_technician_name"),
        record.get("raw_technician_name"),
        record.get("raw_tech_name"),
        _join_name_parts(record.get("raw_technician_firstname"), record.get("raw_technician_lastname")),
    )
    technician_id = _first_present(record.get("assigned_technician_id"))
    if joined_name and not _looks_like_identifier(joined_name):
        return joined_name, "joined"
    if raw_name:
        return raw_name, "raw"
    if joined_name:
        return joined_name, "joined"
    if technician_id:
        return technician_id, "id"
    return None, "missing"


def _chunk_text(text: str, target_chars: int = 1800) -> list[str]:
    if len(text) <= target_chars:
        return [text]
    paragraphs = text.split("\n")
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for para in paragraphs:
        para_len = len(para) + 1
        if current and current_len + para_len > target_chars:
            chunks.append("\n".join(current).strip())
            current = [para]
            current_len = para_len
        else:
            current.append(para)
            current_len += para_len
    if current:
        chunks.append("\n".join(current).strip())
    return [chunk for chunk in chunks if chunk]


def build_ticket_documents(db_path: Path, limit: int | None = None) -> list[dict]:
    query = """
        SELECT t.id,
               t.subject,
               t.status,
               t.priority,
               t.category,
               t.created_at,
               t.updated_at,
               t.closed_at,
               COALESCE(a.name, t.account_id) AS account,
               COALESCE(u.display_name, t.user_id) AS user_name,
               COALESCE(u.email, json_extract(t.raw_json, '$.user_email')) AS user_email,
               COALESCE(te.display_name, t.assigned_technician_id) AS technician,
               t.account_id,
               t.user_id,
               t.assigned_technician_id,
               json_extract(t.raw_json, '$.account_name') AS raw_account_name,
               json_extract(t.raw_json, '$.account_location_name') AS raw_account_location_name,
               json_extract(t.raw_json, '$.user_name') AS raw_user_name,
               json_extract(t.raw_json, '$.user_fullname') AS raw_user_fullname,
               json_extract(t.raw_json, '$.user_firstname') AS raw_user_firstname,
               json_extract(t.raw_json, '$.user_lastname') AS raw_user_lastname,
               json_extract(t.raw_json, '$.user_email') AS raw_user_email,
               json_extract(t.raw_json, '$.tech_name') AS raw_tech_name,
               json_extract(t.raw_json, '$.technician_name') AS raw_technician_name,
               json_extract(t.raw_json, '$.assigned_technician_name') AS raw_assigned_technician_name,
               json_extract(t.raw_json, '$.technician_firstname') AS raw_technician_firstname,
               json_extract(t.raw_json, '$.technician_lastname') AS raw_technician_lastname,
               json_extract(t.raw_json, '$.initial_post') AS initial_post,
               json_extract(t.raw_json, '$.plain_initial_post') AS plain_initial_post,
               json_extract(td.raw_json, '$.initial_post') AS detail_initial_post,
               json_extract(td.raw_json, '$.plain_initial_post') AS detail_plain_initial_post,
               json_extract(t.raw_json, '$.creation_category_name') AS creation_category_name,
               json_extract(t.raw_json, '$.class_name') AS class_name,
               json_extract(t.raw_json, '$.submission_category') AS submission_category,
               json_extract(t.raw_json, '$.resolution_category_name') AS resolution_category_name,
               td.workpad,
               td.note AS detail_note,
               td.initial_response,
               json_extract(t.raw_json, '$.next_step') AS next_step,
               json_extract(td.raw_json, '$.next_step') AS detail_next_step,
               json_extract(t.raw_json, '$.next_step_date') AS next_step_date,
               json_extract(td.raw_json, '$.followup_date') AS followup_date,
               json_extract(td.raw_json, '$.followup_note') AS followup_note,
               json_extract(td.raw_json, '$.request_completion_date') AS request_completion_date,
               json_extract(td.raw_json, '$.request_completion_note') AS request_completion_note,
               json_extract(td.raw_json, '$.support_group_name') AS support_group_name,
               json_extract(td.raw_json, '$.default_contract_name') AS default_contract_name,
               json_extract(td.raw_json, '$.location_name') AS location_name,
               json_extract(td.raw_json, '$.confirmed_by_name') AS confirmed_by_name,
               json_extract(td.raw_json, '$.is_waiting_on_response') AS is_waiting_on_response,
               json_extract(td.raw_json, '$.is_resolved') AS is_resolved,
               json_extract(td.raw_json, '$.is_confirmed') AS is_confirmed,
               td.sla_response_date,
               td.sla_complete_date,
               td.ticketlogs_count,
               td.timelogs_count,
               td.attachments_count,
               (
                   SELECT group_concat(COALESCE(tl.plain_note, tl.note), '\n---\n')
                   FROM (
                       SELECT plain_note, note
                       FROM ticket_logs
                       WHERE ticket_id = t.id
                       ORDER BY record_date DESC, id DESC
                       LIMIT 5
                   ) tl
               ) AS recent_log_text,
               (
                   SELECT group_concat(COALESCE(tl.log_type, 'log'), ', ')
                   FROM (
                       SELECT log_type
                       FROM ticket_logs
                       WHERE ticket_id = t.id
                       ORDER BY record_date DESC, id DESC
                       LIMIT 5
                   ) tl
               ) AS recent_log_types,
               (
                   SELECT json_group_array(json_object(
                       'id', ta.id,
                       'name', ta.name,
                       'size', ta.size,
                       'recorded_at', ta.recorded_at,
                       'url', ta.url
                   ))
                   FROM ticket_attachments ta
                   WHERE ta.ticket_id = t.id
               ) AS attachment_metadata_json
        FROM tickets t
        LEFT JOIN accounts a ON a.id = t.account_id
        LEFT JOIN users u ON u.id = t.user_id
        LEFT JOIN technicians te ON te.id = t.assigned_technician_id
        LEFT JOIN ticket_details td ON td.ticket_id = t.id
        ORDER BY COALESCE(t.updated_at, t.created_at) DESC, t.id DESC
    """
    params: tuple = ()
    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)

    with connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    docs: list[dict] = []
    for row in rows:
        record = dict(row)
        cleaned_initial_post = normalize_ticket_text(
            _first_present(
                record.get("detail_plain_initial_post"),
                record.get("detail_initial_post"),
                record.get("plain_initial_post"),
                record.get("initial_post"),
            )
        )
        cleaned_detail_note = normalize_ticket_text(record.get("detail_note"))
        cleaned_workpad = normalize_ticket_text(record.get("workpad"))
        cleaned_followup_note = normalize_ticket_text(record.get("followup_note"))
        cleaned_request_completion_note = normalize_ticket_text(record.get("request_completion_note"))
        cleaned_recent_logs = normalize_ticket_text(record.get("recent_log_text"))
        resolution_summary = summarize_resolution_from_logs(record.get("recent_log_text"))
        normalized_category = (
            record.get("category")
            or record.get("creation_category_name")
            or record.get("class_name")
            or record.get("submission_category")
        )
        cleaned_subject = normalize_ticket_text(record.get("subject"))
        cleaned_next_step = normalize_ticket_text(_first_present(record.get("detail_next_step"), record.get("next_step")))
        recent_log_types = _split_csv_values(record.get("recent_log_types"))
        account_label, account_label_source = _resolve_account_label(record)
        user_label, user_label_source = _resolve_user_label(record)
        technician_label, technician_label_source = _resolve_technician_label(record)

        text_parts = [
            f"Ticket #{record['id']}: {record.get('subject') or '(no subject)'}",
            f"Status: {record.get('status') or 'unknown'}",
            f"Priority: {record.get('priority') or 'unknown'}",
            f"Category: {normalized_category or 'unknown'}",
            f"Account: {account_label or 'unknown'}",
            f"User: {user_label or 'unknown'}",
            f"Technician: {technician_label or 'unassigned'}",
            f"Created: {record.get('created_at') or 'unknown'}",
            f"Updated: {record.get('updated_at') or 'unknown'}",
            f"Closed: {record.get('closed_at') or 'not closed'}",
        ]
        if record.get("initial_response"):
            text_parts.append(f"Initial response flag/value: {record['initial_response']}")
        if record.get("sla_response_date"):
            text_parts.append(f"SLA response date: {record['sla_response_date']}")
        if record.get("sla_complete_date"):
            text_parts.append(f"SLA completion date: {record['sla_complete_date']}")
        if record.get("ticketlogs_count") is not None:
            text_parts.append(f"Ticket log count: {record['ticketlogs_count']}")
        if record.get("timelogs_count") is not None:
            text_parts.append(f"Time log count: {record['timelogs_count']}")
        if record.get("attachments_count") is not None:
            text_parts.append(f"Attachment count: {record['attachments_count']}")
        if record.get("support_group_name"):
            text_parts.append(f"Support group: {record['support_group_name']}")
        if record.get("default_contract_name"):
            text_parts.append(f"Contract: {record['default_contract_name']}")
        if record.get("location_name"):
            text_parts.append(f"Location: {record['location_name']}")
        if record.get("confirmed_by_name"):
            text_parts.append(f"Confirmed by: {record['confirmed_by_name']}")
        if record.get("is_waiting_on_response") is not None:
            text_parts.append(f"Waiting on response: {bool(record['is_waiting_on_response'])}")
        if record.get("is_confirmed") is not None:
            text_parts.append(f"Confirmed: {bool(record['is_confirmed'])}")
        if record.get("is_resolved") is not None:
            text_parts.append(f"Resolved flag: {bool(record['is_resolved'])}")
        if cleaned_next_step:
            text_parts.append(f"Next step: {cleaned_next_step}")
        if record.get("next_step_date"):
            text_parts.append(f"Next step date: {record['next_step_date']}")
        if record.get("followup_date"):
            text_parts.append(f"Follow-up date: {record['followup_date']}")
        if cleaned_followup_note:
            text_parts.append(f"Follow-up note: {cleaned_followup_note[:1200]}")
        if record.get("request_completion_date"):
            text_parts.append(f"Requested completion date: {record['request_completion_date']}")
        if cleaned_request_completion_note:
            text_parts.append(f"Requested completion note: {cleaned_request_completion_note[:1200]}")
        if cleaned_initial_post:
            text_parts.append(f"Issue summary: {cleaned_initial_post[:2400]}")
        if cleaned_detail_note:
            text_parts.append(f"Internal note: {cleaned_detail_note[:1600]}")
        if cleaned_workpad:
            text_parts.append(f"Workpad summary: {cleaned_workpad[:1600]}")
        if record.get("recent_log_types"):
            text_parts.append(f"Recent log types: {record['recent_log_types']}")
        if cleaned_recent_logs:
            text_parts.append(f"Recent log summary: {cleaned_recent_logs[:2400]}")
        if resolution_summary:
            text_parts.append(f"Resolution/activity highlight: {resolution_summary}")
        if record.get("resolution_category_name"):
            text_parts.append(f"Resolution category: {record['resolution_category_name']}")

        attachment_metadata = []
        if record.get("attachment_metadata_json"):
            try:
                attachment_metadata = json.loads(record["attachment_metadata_json"]) or []
            except json.JSONDecodeError:
                attachment_metadata = []
        if attachment_metadata:
            text_parts.append(
                "Attachments (metadata only): " + ", ".join(
                    f"{item.get('name')} [{item.get('size')} bytes]" for item in attachment_metadata[:5]
                )
            )

        text = "\n".join(text_parts)
        docs.append(
            {
                "doc_id": f"ticket:{record['id']}",
                "ticket_id": record["id"],
                "status": record.get("status"),
                "account": account_label,
                "account_id": record.get("account_id"),
                "user_name": user_label,
                "user_id": record.get("user_id"),
                "technician": technician_label,
                "technician_id": record.get("assigned_technician_id"),
                "updated_at": record.get("updated_at"),
                "created_at": record.get("created_at"),
                "text": text,
                "content_hash": _content_hash(text),
                "metadata": {
                    "priority": record.get("priority"),
                    "category": normalized_category,
                    "class_name": record.get("class_name"),
                    "submission_category": record.get("submission_category"),
                    "resolution_category": record.get("resolution_category_name"),
                    "closed_at": record.get("closed_at"),
                    "ticketlogs_count": record.get("ticketlogs_count"),
                    "timelogs_count": record.get("timelogs_count"),
                    "attachments_count": record.get("attachments_count"),
                    "attachments": attachment_metadata,
                    "attachment_names": [item.get("name") for item in attachment_metadata if item.get("name")],
                    "has_attachments": bool(attachment_metadata),
                    "detail_available": bool(
                        record.get("detail_note")
                        or record.get("workpad")
                        or record.get("initial_response")
                        or record.get("ticketlogs_count")
                        or record.get("attachments_count")
                        or record.get("detail_initial_post")
                        or record.get("detail_plain_initial_post")
                        or record.get("followup_note")
                        or record.get("request_completion_note")
                        or record.get("support_group_name")
                        or record.get("default_contract_name")
                    ),
                    "cleaned_subject": cleaned_subject[:300] if cleaned_subject else None,
                    "cleaned_initial_post": cleaned_initial_post[:400] if cleaned_initial_post else None,
                    "cleaned_detail_note": cleaned_detail_note[:400] if cleaned_detail_note else None,
                    "cleaned_workpad": cleaned_workpad[:400] if cleaned_workpad else None,
                    "cleaned_followup_note": cleaned_followup_note[:400] if cleaned_followup_note else None,
                    "cleaned_request_completion_note": cleaned_request_completion_note[:400] if cleaned_request_completion_note else None,
                    "cleaned_next_step": cleaned_next_step[:300] if cleaned_next_step else None,
                    "next_step_date": record.get("next_step_date"),
                    "followup_date": record.get("followup_date"),
                    "request_completion_date": record.get("request_completion_date"),
                    "has_next_step": bool(cleaned_next_step or record.get("next_step_date") or record.get("followup_date")),
                    "recent_log_types": recent_log_types,
                    "recent_log_types_csv": ", ".join(recent_log_types) if recent_log_types else None,
                    "initial_response_present": record.get("initial_response") is not None,
                    "user_email": record.get("user_email"),
                    "support_group_name": record.get("support_group_name"),
                    "default_contract_name": record.get("default_contract_name"),
                    "location_name": record.get("location_name"),
                    "confirmed_by_name": record.get("confirmed_by_name"),
                    "is_waiting_on_response": bool(record.get("is_waiting_on_response")) if record.get("is_waiting_on_response") is not None else None,
                    "is_resolved": bool(record.get("is_resolved")) if record.get("is_resolved") is not None else None,
                    "is_confirmed": bool(record.get("is_confirmed")) if record.get("is_confirmed") is not None else None,
                    "account_label_source": account_label_source,
                    "user_label_source": user_label_source,
                    "technician_label_source": technician_label_source,
                    "resolution_summary": resolution_summary,
                    "has_resolution_summary": bool(resolution_summary),
                },
            }
        )
    return docs


def build_ticket_document_chunks(docs: list[dict]) -> list[dict]:
    chunks: list[dict] = []
    for doc in docs:
        parts = _chunk_text(doc["text"])
        for idx, chunk_text in enumerate(parts):
            chunks.append(
                {
                    "chunk_id": f"{doc['doc_id']}:chunk:{idx}",
                    "doc_id": doc["doc_id"],
                    "ticket_id": doc["ticket_id"],
                    "account": doc.get("account"),
                    "account_id": doc.get("account_id"),
                    "status": doc.get("status"),
                    "technician": doc.get("technician"),
                    "technician_id": doc.get("technician_id"),
                    "chunk_index": idx,
                    "text": chunk_text,
                    "content_hash": _content_hash(chunk_text),
                    "updated_at": doc.get("updated_at"),
                    "created_at": doc.get("created_at"),
                }
            )
    return chunks


def materialize_ticket_documents(db_path: Path, limit: int | None = None) -> dict:
    docs = build_ticket_documents(db_path, limit=limit)
    chunks = build_ticket_document_chunks(docs)
    synced_at = now_iso()
    replace_ticket_documents(db_path, docs, synced_at=synced_at)
    replace_ticket_document_chunks(db_path, chunks, synced_at=synced_at)
    return {
        "status": "ok",
        "document_count": len(docs),
        "chunk_count": len(chunks),
        "synced_at": synced_at,
    }


def export_ticket_documents(db_path: Path, output_path: Path, limit: int | None = None) -> dict:
    docs = build_ticket_documents(db_path, limit=limit)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")
    return {
        "status": "ok",
        "output_path": str(output_path),
        "document_count": len(docs),
    }


def export_ticket_chunks(db_path: Path, output_path: Path, limit: int | None = None) -> dict:
    docs = build_ticket_documents(db_path, limit=limit)
    chunks = build_ticket_document_chunks(docs)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for chunk in chunks:
            f.write(json.dumps(chunk, ensure_ascii=False) + "\n")
    return {
        "status": "ok",
        "output_path": str(output_path),
        "chunk_count": len(chunks),
    }
