from __future__ import annotations

import json
from pathlib import Path

from .db import connect


def _load_rows(db_path: Path, limit: int | None = None) -> list[dict]:
    query = """
        SELECT c.chunk_id,
               c.doc_id,
               c.ticket_id,
               c.chunk_index,
               c.text,
               c.content_hash,
               d.updated_at,
               d.status,
               d.account,
               d.user_name,
               d.technician,
               json_extract(d.raw_json, '$.account_id') AS account_id,
               json_extract(d.raw_json, '$.user_id') AS user_id,
               json_extract(d.raw_json, '$.technician_id') AS technician_id,
               json_extract(d.raw_json, '$.created_at') AS created_at,
               json_extract(d.raw_json, '$.metadata.priority') AS priority,
               json_extract(d.raw_json, '$.metadata.category') AS category,
               json_extract(d.raw_json, '$.metadata.closed_at') AS closed_at,
               json_extract(d.raw_json, '$.metadata.attachments_count') AS attachments_count,
               json_extract(d.raw_json, '$.metadata.ticketlogs_count') AS ticketlogs_count,
               json_extract(d.raw_json, '$.metadata.timelogs_count') AS timelogs_count,
               json_extract(d.raw_json, '$.metadata.cleaned_subject') AS cleaned_subject,
               json_extract(d.raw_json, '$.metadata.cleaned_initial_post') AS cleaned_initial_post,
               json_extract(d.raw_json, '$.metadata.cleaned_detail_note') AS cleaned_detail_note,
               json_extract(d.raw_json, '$.metadata.cleaned_workpad') AS cleaned_workpad,
               json_extract(d.raw_json, '$.metadata.cleaned_next_step') AS cleaned_next_step,
               json_extract(d.raw_json, '$.metadata.next_step_date') AS next_step_date,
               json_extract(d.raw_json, '$.metadata.recent_log_types_csv') AS recent_log_types,
               json_extract(d.raw_json, '$.metadata.initial_response_present') AS initial_response_present,
               json_extract(d.raw_json, '$.metadata.user_email') AS user_email,
               json_extract(d.raw_json, '$.metadata.has_attachments') AS has_attachments,
               json_extract(d.raw_json, '$.metadata.has_next_step') AS has_next_step,
               json_extract(d.raw_json, '$.metadata.resolution_summary') AS resolution_summary,
               json_extract(d.raw_json, '$.metadata.has_resolution_summary') AS has_resolution_summary
        FROM ticket_document_chunks c
        JOIN ticket_documents d ON d.doc_id = c.doc_id
        ORDER BY c.ticket_id DESC, c.chunk_index ASC
    """
    params: tuple = ()
    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)

    with connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def export_embedding_ready_chunks(db_path: Path, output_path: Path, limit: int | None = None) -> dict:
    rows = _load_rows(db_path, limit=limit)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8") as f:
        for record in rows:
            payload = {
                "id": record["chunk_id"],
                "text": record["text"],
                "metadata": {
                    "doc_id": record["doc_id"],
                    "ticket_id": record["ticket_id"],
                    "chunk_index": record["chunk_index"],
                    "status": record["status"],
                    "account": record["account"],
                    "account_id": record["account_id"],
                    "user_name": record["user_name"],
                    "user_id": record["user_id"],
                    "user_email": record["user_email"],
                    "technician": record["technician"],
                    "technician_id": record["technician_id"],
                    "priority": record["priority"],
                    "category": record["category"],
                    "closed_at": record["closed_at"],
                    "attachments_count": record["attachments_count"],
                    "has_attachments": bool(record["has_attachments"]),
                    "ticketlogs_count": record["ticketlogs_count"],
                    "timelogs_count": record["timelogs_count"],
                    "cleaned_subject": record["cleaned_subject"],
                    "cleaned_initial_post": record["cleaned_initial_post"],
                    "cleaned_detail_note": record["cleaned_detail_note"],
                    "cleaned_workpad": record["cleaned_workpad"],
                    "cleaned_next_step": record["cleaned_next_step"],
                    "next_step_date": record["next_step_date"],
                    "has_next_step": bool(record["has_next_step"]),
                    "recent_log_types": record["recent_log_types"],
                    "initial_response_present": bool(record["initial_response_present"]),
                    "resolution_summary": record["resolution_summary"],
                    "has_resolution_summary": bool(record["has_resolution_summary"]),
                    "created_at": record["created_at"],
                    "updated_at": record["updated_at"],
                    "content_hash": record["content_hash"],
                },
            }
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
            count += 1
    return {
        "status": "ok",
        "output_path": str(output_path),
        "chunk_count": count,
    }


def export_embedding_manifest(db_path: Path, output_path: Path, limit: int | None = None) -> dict:
    rows = _load_rows(db_path, limit=limit)
    manifest = {
        "chunk_count": len(rows),
        "latest_updated_at": max((row.get("updated_at") for row in rows if row.get("updated_at")), default=None),
        "accounts": sorted({row.get("account") for row in rows if row.get("account")}),
        "technicians": sorted({row.get("technician") for row in rows if row.get("technician")}),
        "statuses": sorted({row.get("status") for row in rows if row.get("status")}),
        "content_hashes": [row.get("content_hash") for row in rows],
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return {
        "status": "ok",
        "output_path": str(output_path),
        "chunk_count": len(rows),
    }
