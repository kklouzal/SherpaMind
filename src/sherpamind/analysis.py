from __future__ import annotations

from .db import connect
from pathlib import Path


def list_ticket_counts_by_account(db_path: Path) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(a.name, t.account_id, 'unknown') AS account,
                   COUNT(*) AS ticket_count
            FROM tickets t
            LEFT JOIN accounts a ON a.id = t.account_id
            GROUP BY COALESCE(a.name, t.account_id, 'unknown')
            ORDER BY ticket_count DESC, account ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]
