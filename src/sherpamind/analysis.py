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


def list_open_ticket_ages(db_path: Path, limit: int = 20) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT t.id,
                   t.subject,
                   COALESCE(a.name, t.account_id) AS account,
                   COALESCE(u.display_name, t.user_id) AS user,
                   COALESCE(te.display_name, t.assigned_technician_id) AS technician,
                   t.priority,
                   t.status,
                   t.created_at,
                   t.updated_at,
                   ROUND((julianday('now') - julianday(REPLACE(substr(t.created_at, 1, 19), 'T', ' '))), 2) AS age_days,
                   ROUND((julianday('now') - julianday(REPLACE(substr(COALESCE(t.updated_at, t.created_at), 1, 19), 'T', ' '))), 2) AS days_since_update
            FROM tickets t
            LEFT JOIN accounts a ON a.id = t.account_id
            LEFT JOIN users u ON u.id = t.user_id
            LEFT JOIN technicians te ON te.id = t.assigned_technician_id
            WHERE t.status = 'Open'
            ORDER BY age_days DESC, days_since_update DESC, t.id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def list_recent_account_activity(db_path: Path, days: int = 7, limit: int = 20) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(a.name, t.account_id, 'unknown') AS account,
                   COUNT(*) AS ticket_count,
                   SUM(CASE WHEN t.status = 'Open' THEN 1 ELSE 0 END) AS open_count,
                   SUM(CASE WHEN t.status = 'Closed' THEN 1 ELSE 0 END) AS closed_count,
                   MAX(COALESCE(t.updated_at, t.created_at)) AS latest_activity_at
            FROM tickets t
            LEFT JOIN accounts a ON a.id = t.account_id
            WHERE julianday(REPLACE(substr(COALESCE(t.updated_at, t.created_at), 1, 19), 'T', ' ')) >= julianday('now', ?)
            GROUP BY COALESCE(a.name, t.account_id, 'unknown')
            ORDER BY ticket_count DESC, latest_activity_at DESC, account ASC
            LIMIT ?
            """,
            (f'-{days} days', limit),
        ).fetchall()
    return [dict(row) for row in rows]


def list_technician_recent_load(db_path: Path, days: int = 7, limit: int = 20) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(te.display_name, t.assigned_technician_id, 'unassigned') AS technician,
                   COUNT(*) AS ticket_count,
                   SUM(CASE WHEN t.status = 'Open' THEN 1 ELSE 0 END) AS open_count,
                   SUM(CASE WHEN t.status = 'Closed' THEN 1 ELSE 0 END) AS closed_count,
                   MAX(COALESCE(t.updated_at, t.created_at)) AS latest_activity_at
            FROM tickets t
            LEFT JOIN technicians te ON te.id = t.assigned_technician_id
            WHERE julianday(REPLACE(substr(COALESCE(t.updated_at, t.created_at), 1, 19), 'T', ' ')) >= julianday('now', ?)
            GROUP BY COALESCE(te.display_name, t.assigned_technician_id, 'unassigned')
            ORDER BY ticket_count DESC, latest_activity_at DESC, technician ASC
            LIMIT ?
            """,
            (f'-{days} days', limit),
        ).fetchall()
    return [dict(row) for row in rows]


def list_ticket_log_types(db_path: Path, limit: int = 20) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(log_type, 'unknown') AS log_type,
                   COUNT(*) AS log_count
            FROM ticket_logs
            GROUP BY COALESCE(log_type, 'unknown')
            ORDER BY log_count DESC, log_type ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def search_ticket_documents(db_path: Path, query: str, limit: int = 20) -> list[dict]:
    needle = f"%{query}%"
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT doc_id, ticket_id, status, account, user_name, technician, updated_at, text
            FROM ticket_documents
            WHERE text LIKE ? COLLATE NOCASE
               OR account LIKE ? COLLATE NOCASE
               OR user_name LIKE ? COLLATE NOCASE
               OR technician LIKE ? COLLATE NOCASE
            ORDER BY updated_at DESC, ticket_id DESC
            LIMIT ?
            """,
            (needle, needle, needle, needle, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def get_dataset_summary(db_path: Path) -> dict:
    with connect(db_path) as conn:
        counts = {}
        for table in [
            "accounts",
            "users",
            "technicians",
            "tickets",
            "ticket_details",
            "ticket_logs",
            "ticket_time_logs",
            "ticket_documents",
            "ticket_comments",
            "ingest_runs",
        ]:
            counts[table] = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        latest_run = conn.execute(
            "SELECT id, mode, started_at, finished_at, status, notes FROM ingest_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return {
        "counts": counts,
        "latest_ingest_run": dict(latest_run) if latest_run else None,
    }


def get_insight_snapshot(db_path: Path) -> dict:
    return {
        "dataset_summary": get_dataset_summary(db_path),
        "status_counts": list_ticket_counts_by_status(db_path),
        "priority_counts": list_ticket_counts_by_priority(db_path),
        "top_accounts": list_ticket_counts_by_account(db_path, limit=10),
        "top_technicians": list_ticket_counts_by_technician(db_path, limit=10),
        "oldest_open_tickets": list_open_ticket_ages(db_path, limit=10),
        "recent_account_activity_7d": list_recent_account_activity(db_path, days=7, limit=10),
        "recent_technician_load_7d": list_technician_recent_load(db_path, days=7, limit=10),
        "ticket_log_types": list_ticket_log_types(db_path, limit=10),
        "recent_tickets": list_recent_tickets(db_path, limit=10),
    }
