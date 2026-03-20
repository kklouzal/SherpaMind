from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .db import connect, now_iso, replace_ticket_document_chunks, replace_ticket_documents
from .text_cleanup import normalize_ticket_text, summarize_resolution_from_logs


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


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
               json_extract(t.raw_json, '$.initial_post') AS initial_post,
               json_extract(t.raw_json, '$.plain_initial_post') AS plain_initial_post,
               json_extract(t.raw_json, '$.creation_category_name') AS creation_category_name,
               json_extract(t.raw_json, '$.resolution_category_name') AS resolution_category_name,
               td.workpad,
               td.note AS detail_note,
               td.initial_response,
               json_extract(t.raw_json, '$.next_step') AS next_step,
               json_extract(t.raw_json, '$.next_step_date') AS next_step_date,
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
        cleaned_initial_post = normalize_ticket_text(record.get("initial_post") or record.get("plain_initial_post"))
        cleaned_detail_note = normalize_ticket_text(record.get("detail_note"))
        cleaned_workpad = normalize_ticket_text(record.get("workpad"))
        cleaned_recent_logs = normalize_ticket_text(record.get("recent_log_text"))
        resolution_summary = summarize_resolution_from_logs(record.get("recent_log_text"))

        text_parts = [
            f"Ticket #{record['id']}: {record.get('subject') or '(no subject)'}",
            f"Status: {record.get('status') or 'unknown'}",
            f"Priority: {record.get('priority') or 'unknown'}",
            f"Category: {record.get('category') or record.get('creation_category_name') or 'unknown'}",
            f"Account: {record.get('account') or 'unknown'}",
            f"User: {record.get('user_name') or record.get('user_email') or 'unknown'}",
            f"Technician: {record.get('technician') or 'unassigned'}",
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
        if record.get("next_step"):
            text_parts.append(f"Next step: {normalize_ticket_text(record['next_step'])}")
        if record.get("next_step_date"):
            text_parts.append(f"Next step date: {record['next_step_date']}")
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
                "account": record.get("account"),
                "user_name": record.get("user_name"),
                "technician": record.get("technician"),
                "updated_at": record.get("updated_at"),
                "text": text,
                "content_hash": _content_hash(text),
                "metadata": {
                    "priority": record.get("priority"),
                    "category": record.get("category") or record.get("creation_category_name"),
                    "closed_at": record.get("closed_at"),
                    "ticketlogs_count": record.get("ticketlogs_count"),
                    "timelogs_count": record.get("timelogs_count"),
                    "attachments_count": record.get("attachments_count"),
                    "attachments": attachment_metadata,
                    "cleaned_initial_post": cleaned_initial_post[:400] if cleaned_initial_post else None,
                    "resolution_summary": resolution_summary,
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
                    "chunk_index": idx,
                    "text": chunk_text,
                    "content_hash": _content_hash(chunk_text),
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
