from __future__ import annotations

from pathlib import Path

from .db import connect


def get_account_summary(db_path: Path, account_query: str, limit_open: int = 10, limit_recent: int = 10) -> dict:
    with connect(db_path) as conn:
        account = conn.execute(
            """
            SELECT id, name, raw_json
            FROM accounts
            WHERE name LIKE ? COLLATE NOCASE
            ORDER BY name ASC
            LIMIT 1
            """,
            (f"%{account_query}%",),
        ).fetchone()
        if not account:
            return {"status": "not_found", "account_query": account_query}
        open_tickets = conn.execute(
            """
            SELECT id, subject, priority, updated_at, closed_at
            FROM tickets
            WHERE account_id = ? AND status = 'Open'
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (account["id"], limit_open),
        ).fetchall()
        recent_tickets = conn.execute(
            """
            SELECT id, subject, status, priority, updated_at
            FROM tickets
            WHERE account_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (account["id"], limit_recent),
        ).fetchall()
        stats = conn.execute(
            """
            SELECT COUNT(*) AS total_tickets,
                   SUM(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) AS open_tickets,
                   SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) AS closed_tickets,
                   MAX(updated_at) AS latest_activity_at
            FROM tickets
            WHERE account_id = ?
            """,
            (account["id"],),
        ).fetchone()
        recent_log_types = conn.execute(
            """
            SELECT COALESCE(tl.log_type, 'unknown') AS log_type, COUNT(*) AS log_count
            FROM ticket_logs tl
            JOIN tickets t ON t.id = tl.ticket_id
            WHERE t.account_id = ?
            GROUP BY COALESCE(tl.log_type, 'unknown')
            ORDER BY log_count DESC, log_type ASC
            LIMIT 10
            """,
            (account["id"],),
        ).fetchall()
    return {
        "status": "ok",
        "account": {"id": account["id"], "name": account["name"]},
        "stats": dict(stats),
        "open_tickets": [dict(row) for row in open_tickets],
        "recent_tickets": [dict(row) for row in recent_tickets],
        "recent_log_types": [dict(row) for row in recent_log_types],
    }


def get_technician_summary(db_path: Path, technician_query: str, limit_open: int = 10, limit_recent: int = 10) -> dict:
    with connect(db_path) as conn:
        technician = conn.execute(
            """
            SELECT id, display_name, email
            FROM technicians
            WHERE display_name LIKE ? COLLATE NOCASE
            ORDER BY display_name ASC
            LIMIT 1
            """,
            (f"%{technician_query}%",),
        ).fetchone()
        if not technician:
            return {"status": "not_found", "technician_query": technician_query}
        open_tickets = conn.execute(
            """
            SELECT id, subject, priority, updated_at
            FROM tickets
            WHERE assigned_technician_id = ? AND status = 'Open'
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (technician["id"], limit_open),
        ).fetchall()
        recent_tickets = conn.execute(
            """
            SELECT id, subject, status, priority, updated_at
            FROM tickets
            WHERE assigned_technician_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (technician["id"], limit_recent),
        ).fetchall()
        stats = conn.execute(
            """
            SELECT COUNT(*) AS total_tickets,
                   SUM(CASE WHEN status = 'Open' THEN 1 ELSE 0 END) AS open_tickets,
                   SUM(CASE WHEN status = 'Closed' THEN 1 ELSE 0 END) AS closed_tickets,
                   MAX(updated_at) AS latest_activity_at
            FROM tickets
            WHERE assigned_technician_id = ?
            """,
            (technician["id"],),
        ).fetchone()
        recent_log_types = conn.execute(
            """
            SELECT COALESCE(tl.log_type, 'unknown') AS log_type, COUNT(*) AS log_count
            FROM ticket_logs tl
            JOIN tickets t ON t.id = tl.ticket_id
            WHERE t.assigned_technician_id = ?
            GROUP BY COALESCE(tl.log_type, 'unknown')
            ORDER BY log_count DESC, log_type ASC
            LIMIT 10
            """,
            (technician["id"],),
        ).fetchall()
    return {
        "status": "ok",
        "technician": {"id": technician["id"], "display_name": technician["display_name"], "email": technician["email"]},
        "stats": dict(stats),
        "open_tickets": [dict(row) for row in open_tickets],
        "recent_tickets": [dict(row) for row in recent_tickets],
        "recent_log_types": [dict(row) for row in recent_log_types],
    }
