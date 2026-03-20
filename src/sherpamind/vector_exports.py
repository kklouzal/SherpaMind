from __future__ import annotations

import json
from pathlib import Path

from .db import connect


def export_embedding_ready_chunks(db_path: Path, output_path: Path, limit: int | None = None) -> dict:
    query = """
        SELECT c.chunk_id,
               c.doc_id,
               c.ticket_id,
               c.chunk_index,
               c.text,
               c.content_hash,
               c.updated_at,
               d.status,
               d.account,
               d.user_name,
               d.technician,
               json_extract(d.raw_json, '$.metadata.priority') AS priority,
               json_extract(d.raw_json, '$.metadata.category') AS category,
               json_extract(d.raw_json, '$.metadata.closed_at') AS closed_at,
               json_extract(d.raw_json, '$.metadata.attachments_count') AS attachments_count
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

    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8") as f:
        for row in rows:
            record = dict(row)
            payload = {
                "id": record["chunk_id"],
                "text": record["text"],
                "metadata": {
                    "doc_id": record["doc_id"],
                    "ticket_id": record["ticket_id"],
                    "chunk_index": record["chunk_index"],
                    "status": record["status"],
                    "account": record["account"],
                    "user_name": record["user_name"],
                    "technician": record["technician"],
                    "priority": record["priority"],
                    "category": record["category"],
                    "closed_at": record["closed_at"],
                    "attachments_count": record["attachments_count"],
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
