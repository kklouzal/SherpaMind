from __future__ import annotations

from pathlib import Path

from .db import connect


def list_ticket_counts_by_account(db_path: Path, limit: int = 20) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(a.name, t.account_id, 'unknown') AS account,
                   COUNT(*) AS ticket_count
            FROM tickets t
            LEFT JOIN accounts a ON a.id = t.account_id
            GROUP BY COALESCE(a.name, t.account_id, 'unknown')
            ORDER BY ticket_count DESC, account ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def list_ticket_counts_by_status(db_path: Path) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(status, 'unknown') AS status,
                   COUNT(*) AS ticket_count
            FROM tickets
            GROUP BY COALESCE(status, 'unknown')
            ORDER BY ticket_count DESC, status ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def list_ticket_counts_by_priority(db_path: Path) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(priority, 'unknown') AS priority,
                   COUNT(*) AS ticket_count
            FROM tickets
            GROUP BY COALESCE(priority, 'unknown')
            ORDER BY ticket_count DESC, priority ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def list_ticket_counts_by_technician(db_path: Path, limit: int = 20) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(te.display_name, t.assigned_technician_id, 'unassigned') AS technician,
                   COUNT(*) AS ticket_count
            FROM tickets t
            LEFT JOIN technicians te ON te.id = t.assigned_technician_id
            GROUP BY COALESCE(te.display_name, t.assigned_technician_id, 'unassigned')
            ORDER BY ticket_count DESC, technician ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def list_recent_tickets(db_path: Path, limit: int = 20) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT t.id,
                   t.subject,
                   t.status,
                   t.priority,
                   COALESCE(a.name, t.account_id) AS account,
                   COALESCE(u.display_name, t.user_id) AS user,
                   COALESCE(te.display_name, t.assigned_technician_id) AS technician,
                   t.created_at,
                   t.updated_at,
                   t.closed_at
            FROM tickets t
            LEFT JOIN accounts a ON a.id = t.account_id
            LEFT JOIN users u ON u.id = t.user_id
            LEFT JOIN technicians te ON te.id = t.assigned_technician_id
            ORDER BY COALESCE(t.updated_at, t.created_at) DESC, t.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_dataset_summary(db_path: Path) -> dict:
    with connect(db_path) as conn:
        counts = {}
        for table in ["accounts", "users", "technicians", "tickets", "ticket_comments", "ingest_runs"]:
            counts[table] = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        latest_run = conn.execute(
            "SELECT id, mode, started_at, finished_at, status, notes FROM ingest_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return {
        "counts": counts,
        "latest_ingest_run": dict(latest_run) if latest_run else None,
    }
