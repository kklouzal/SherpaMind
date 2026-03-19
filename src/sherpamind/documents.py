from __future__ import annotations

import json
from pathlib import Path

from .db import connect


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
               json_extract(t.raw_json, '$.next_step') AS next_step,
               json_extract(t.raw_json, '$.next_step_date') AS next_step_date,
               json_extract(t.raw_json, '$.account_location_name') AS account_location_name
        FROM tickets t
        LEFT JOIN accounts a ON a.id = t.account_id
        LEFT JOIN users u ON u.id = t.user_id
        LEFT JOIN technicians te ON te.id = t.assigned_technician_id
        ORDER BY COALESCE(t.updated_at, t.created_at) DESC, t.id DESC
    """
    params: tuple = ()
    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)

    with connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()

    docs = []
    for row in rows:
        record = dict(row)
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
        if record.get('account_location_name'):
            text_parts.append(f"Location: {record['account_location_name']}")
        if record.get('next_step'):
            text_parts.append(f"Next step: {record['next_step']}")
        if record.get('next_step_date'):
            text_parts.append(f"Next step date: {record['next_step_date']}")
        if record.get('initial_post'):
            text_parts.append(f"Initial post: {record['initial_post']}")
        elif record.get('plain_initial_post'):
            text_parts.append(f"Initial post: {record['plain_initial_post']}")
        if record.get('resolution_category_name'):
            text_parts.append(f"Resolution category: {record['resolution_category_name']}")

        docs.append(
            {
                "doc_id": f"ticket:{record['id']}",
                "ticket_id": record["id"],
                "status": record.get("status"),
                "account": record.get("account"),
                "user_name": record.get("user_name"),
                "technician": record.get("technician"),
                "updated_at": record.get("updated_at"),
                "text": "\n".join(text_parts),
                "metadata": {
                    "priority": record.get("priority"),
                    "category": record.get("category") or record.get("creation_category_name"),
                    "closed_at": record.get("closed_at"),
                },
            }
        )
    return docs


def export_ticket_documents(db_path: Path, output_path: Path, limit: int | None = None) -> dict:
    docs = build_ticket_documents(db_path, limit=limit)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open('w', encoding='utf-8') as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + '\n')
    return {
        "status": "ok",
        "output_path": str(output_path),
        "document_count": len(docs),
    }
