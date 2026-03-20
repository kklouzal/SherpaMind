from __future__ import annotations

from pathlib import Path

from .db import connect

# Summary helpers in this module are intentionally factual/structural.
# They should organize and expose data for OpenClaw, not pre-interpret it too aggressively.


def _ratio(numerator: int, denominator: int) -> float:
    return round((numerator / denominator), 4) if denominator else 0.0


def list_account_artifact_summaries(db_path: Path) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            WITH ticket_core AS (
                SELECT t.id,
                       COALESCE(a.id, t.account_id) AS account_ref,
                       COALESCE(a.name, t.account_id, 'unknown') AS account,
                       t.status,
                       COALESCE(t.updated_at, t.created_at) AS activity_at
                FROM tickets t
                LEFT JOIN accounts a ON a.id = t.account_id
            ),
            detail_tickets AS (
                SELECT DISTINCT ticket_id FROM ticket_details
            ),
            log_tickets AS (
                SELECT DISTINCT ticket_id FROM ticket_logs
            ),
            attachment_tickets AS (
                SELECT DISTINCT ticket_id FROM ticket_attachments
            ),
            document_tickets AS (
                SELECT DISTINCT ticket_id FROM ticket_documents
            ),
            chunk_counts AS (
                SELECT ticket_id, COUNT(*) AS chunk_count
                FROM ticket_document_chunks
                GROUP BY ticket_id
            )
            SELECT MIN(tc.account_ref) AS account_ref,
                   tc.account,
                   COUNT(*) AS total_tickets,
                   SUM(CASE WHEN tc.status = 'Open' THEN 1 ELSE 0 END) AS open_tickets,
                   SUM(CASE WHEN tc.status = 'Closed' THEN 1 ELSE 0 END) AS closed_tickets,
                   MAX(tc.activity_at) AS latest_activity_at,
                   SUM(CASE WHEN dt.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS detail_tickets,
                   SUM(CASE WHEN lt.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS log_tickets,
                   SUM(CASE WHEN at.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS attachment_tickets,
                   SUM(CASE WHEN doc.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS document_tickets,
                   COALESCE(SUM(cc.chunk_count), 0) AS chunk_count
            FROM ticket_core tc
            LEFT JOIN detail_tickets dt ON dt.ticket_id = tc.id
            LEFT JOIN log_tickets lt ON lt.ticket_id = tc.id
            LEFT JOIN attachment_tickets at ON at.ticket_id = tc.id
            LEFT JOIN document_tickets doc ON doc.ticket_id = tc.id
            LEFT JOIN chunk_counts cc ON cc.ticket_id = tc.id
            WHERE tc.account IS NOT NULL AND tc.account != '' AND tc.account != 'unknown'
            GROUP BY tc.account
            ORDER BY total_tickets DESC, latest_activity_at DESC, tc.account ASC
            """
        ).fetchall()
    summaries = []
    for row in rows:
        total_tickets = int(row["total_tickets"] or 0)
        detail_tickets = int(row["detail_tickets"] or 0)
        log_tickets = int(row["log_tickets"] or 0)
        attachment_tickets = int(row["attachment_tickets"] or 0)
        document_tickets = int(row["document_tickets"] or 0)
        summaries.append({
            "account_ref": row["account_ref"],
            "account": row["account"],
            "total_tickets": total_tickets,
            "open_tickets": int(row["open_tickets"] or 0),
            "closed_tickets": int(row["closed_tickets"] or 0),
            "latest_activity_at": row["latest_activity_at"],
            "detail_tickets": detail_tickets,
            "detail_coverage_ratio": _ratio(detail_tickets, total_tickets),
            "log_tickets": log_tickets,
            "log_coverage_ratio": _ratio(log_tickets, total_tickets),
            "attachment_tickets": attachment_tickets,
            "attachment_coverage_ratio": _ratio(attachment_tickets, total_tickets),
            "document_tickets": document_tickets,
            "document_coverage_ratio": _ratio(document_tickets, total_tickets),
            "chunk_count": int(row["chunk_count"] or 0),
        })
    return summaries


def list_technician_artifact_summaries(db_path: Path) -> list[dict]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            WITH ticket_core AS (
                SELECT t.id,
                       COALESCE(te.id, t.assigned_technician_id) AS technician_ref,
                       COALESCE(te.display_name, t.assigned_technician_id, 'unassigned') AS technician,
                       t.status,
                       COALESCE(t.updated_at, t.created_at) AS activity_at
                FROM tickets t
                LEFT JOIN technicians te ON te.id = t.assigned_technician_id
            ),
            detail_tickets AS (
                SELECT DISTINCT ticket_id FROM ticket_details
            ),
            log_tickets AS (
                SELECT DISTINCT ticket_id FROM ticket_logs
            ),
            attachment_tickets AS (
                SELECT DISTINCT ticket_id FROM ticket_attachments
            ),
            document_tickets AS (
                SELECT DISTINCT ticket_id FROM ticket_documents
            ),
            chunk_counts AS (
                SELECT ticket_id, COUNT(*) AS chunk_count
                FROM ticket_document_chunks
                GROUP BY ticket_id
            )
            SELECT MIN(tc.technician_ref) AS technician_ref,
                   tc.technician,
                   COUNT(*) AS total_tickets,
                   SUM(CASE WHEN tc.status = 'Open' THEN 1 ELSE 0 END) AS open_tickets,
                   SUM(CASE WHEN tc.status = 'Closed' THEN 1 ELSE 0 END) AS closed_tickets,
                   MAX(tc.activity_at) AS latest_activity_at,
                   SUM(CASE WHEN dt.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS detail_tickets,
                   SUM(CASE WHEN lt.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS log_tickets,
                   SUM(CASE WHEN at.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS attachment_tickets,
                   SUM(CASE WHEN doc.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS document_tickets,
                   COALESCE(SUM(cc.chunk_count), 0) AS chunk_count
            FROM ticket_core tc
            LEFT JOIN detail_tickets dt ON dt.ticket_id = tc.id
            LEFT JOIN log_tickets lt ON lt.ticket_id = tc.id
            LEFT JOIN attachment_tickets at ON at.ticket_id = tc.id
            LEFT JOIN document_tickets doc ON doc.ticket_id = tc.id
            LEFT JOIN chunk_counts cc ON cc.ticket_id = tc.id
            WHERE tc.technician IS NOT NULL AND tc.technician != '' AND tc.technician != 'unassigned'
            GROUP BY tc.technician
            ORDER BY total_tickets DESC, latest_activity_at DESC, tc.technician ASC
            """
        ).fetchall()
    summaries = []
    for row in rows:
        total_tickets = int(row["total_tickets"] or 0)
        detail_tickets = int(row["detail_tickets"] or 0)
        log_tickets = int(row["log_tickets"] or 0)
        attachment_tickets = int(row["attachment_tickets"] or 0)
        document_tickets = int(row["document_tickets"] or 0)
        summaries.append({
            "technician_ref": row["technician_ref"],
            "technician": row["technician"],
            "total_tickets": total_tickets,
            "open_tickets": int(row["open_tickets"] or 0),
            "closed_tickets": int(row["closed_tickets"] or 0),
            "latest_activity_at": row["latest_activity_at"],
            "detail_tickets": detail_tickets,
            "detail_coverage_ratio": _ratio(detail_tickets, total_tickets),
            "log_tickets": log_tickets,
            "log_coverage_ratio": _ratio(log_tickets, total_tickets),
            "attachment_tickets": attachment_tickets,
            "attachment_coverage_ratio": _ratio(attachment_tickets, total_tickets),
            "document_tickets": document_tickets,
            "document_coverage_ratio": _ratio(document_tickets, total_tickets),
            "chunk_count": int(row["chunk_count"] or 0),
        })
    return summaries


def get_account_summary(db_path: Path, account_query: str, limit_open: int = 10, limit_recent: int = 10) -> dict:
    with connect(db_path) as conn:
        account = conn.execute(
            """
            SELECT id, name, raw_json
            FROM accounts
            WHERE id = ? OR name = ? COLLATE NOCASE OR name LIKE ? COLLATE NOCASE
            ORDER BY CASE WHEN id = ? THEN 0 WHEN name = ? COLLATE NOCASE THEN 1 ELSE 2 END, name ASC
            LIMIT 1
            """,
            (account_query, account_query, f"%{account_query}%", account_query, account_query),
        ).fetchone()
        if not account:
            fallback_account = conn.execute(
                """
                SELECT account_id AS id, account_id AS name
                FROM tickets
                WHERE account_id = ?
                LIMIT 1
                """,
                (account_query,),
            ).fetchone()
            if not fallback_account:
                return {"status": "not_found", "account_query": account_query}
            account = fallback_account
        open_tickets = conn.execute(
            """
            SELECT id, subject, status, priority, category, updated_at, closed_at
            FROM tickets
            WHERE account_id = ? AND status = 'Open'
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (account["id"], limit_open),
        ).fetchall()
        recent_tickets = conn.execute(
            """
            SELECT id, subject, status, priority, category, updated_at
            FROM tickets
            WHERE account_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (account["id"], limit_recent),
        ).fetchall()
        stats = conn.execute(
            """
            WITH log_tickets AS (
                SELECT DISTINCT ticket_id FROM ticket_logs
            ),
            attachment_tickets AS (
                SELECT DISTINCT ticket_id FROM ticket_attachments
            ),
            chunk_counts AS (
                SELECT ticket_id, COUNT(*) AS chunk_count
                FROM ticket_document_chunks
                GROUP BY ticket_id
            )
            SELECT COUNT(*) AS total_tickets,
                   SUM(CASE WHEN t.status = 'Open' THEN 1 ELSE 0 END) AS open_tickets,
                   SUM(CASE WHEN t.status = 'Closed' THEN 1 ELSE 0 END) AS closed_tickets,
                   MAX(t.updated_at) AS latest_activity_at,
                   SUM(CASE WHEN td.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS detail_tickets,
                   SUM(CASE WHEN lt.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS log_tickets,
                   SUM(CASE WHEN at.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS attachment_tickets,
                   SUM(CASE WHEN doc.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS document_tickets,
                   COALESCE(SUM(cc.chunk_count), 0) AS chunk_count
            FROM tickets t
            LEFT JOIN ticket_details td ON td.ticket_id = t.id
            LEFT JOIN log_tickets lt ON lt.ticket_id = t.id
            LEFT JOIN attachment_tickets at ON at.ticket_id = t.id
            LEFT JOIN ticket_documents doc ON doc.ticket_id = t.id
            LEFT JOIN chunk_counts cc ON cc.ticket_id = t.id
            WHERE t.account_id = ?
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
        status_breakdown = conn.execute(
            """
            SELECT COALESCE(status, 'unknown') AS status, COUNT(*) AS ticket_count
            FROM tickets
            WHERE account_id = ?
            GROUP BY COALESCE(status, 'unknown')
            ORDER BY ticket_count DESC, status ASC
            """,
            (account["id"],),
        ).fetchall()
        priority_breakdown = conn.execute(
            """
            SELECT COALESCE(priority, 'unknown') AS priority, COUNT(*) AS ticket_count
            FROM tickets
            WHERE account_id = ?
            GROUP BY COALESCE(priority, 'unknown')
            ORDER BY ticket_count DESC, priority ASC
            LIMIT 10
            """,
            (account["id"],),
        ).fetchall()
        category_breakdown = conn.execute(
            """
            SELECT COALESCE(category, 'unknown') AS category, COUNT(*) AS ticket_count
            FROM tickets
            WHERE account_id = ?
            GROUP BY COALESCE(category, 'unknown')
            ORDER BY ticket_count DESC, category ASC
            LIMIT 10
            """,
            (account["id"],),
        ).fetchall()
    stats_dict = dict(stats)
    total_tickets = int(stats_dict.get("total_tickets") or 0)
    detail_tickets = int(stats_dict.get("detail_tickets") or 0)
    log_tickets = int(stats_dict.get("log_tickets") or 0)
    attachment_tickets = int(stats_dict.get("attachment_tickets") or 0)
    document_tickets = int(stats_dict.get("document_tickets") or 0)
    stats_dict.update({
        "detail_coverage_ratio": _ratio(detail_tickets, total_tickets),
        "log_coverage_ratio": _ratio(log_tickets, total_tickets),
        "attachment_coverage_ratio": _ratio(attachment_tickets, total_tickets),
        "document_coverage_ratio": _ratio(document_tickets, total_tickets),
    })
    return {
        "status": "ok",
        "account": {"id": account["id"], "name": account["name"]},
        "stats": stats_dict,
        "open_tickets": [dict(row) for row in open_tickets],
        "recent_tickets": [dict(row) for row in recent_tickets],
        "recent_log_types": [dict(row) for row in recent_log_types],
        "status_breakdown": [dict(row) for row in status_breakdown],
        "priority_breakdown": [dict(row) for row in priority_breakdown],
        "category_breakdown": [dict(row) for row in category_breakdown],
    }


def get_technician_summary(db_path: Path, technician_query: str, limit_open: int = 10, limit_recent: int = 10) -> dict:
    with connect(db_path) as conn:
        technician = conn.execute(
            """
            SELECT id, display_name, email
            FROM technicians
            WHERE id = ? OR display_name = ? COLLATE NOCASE OR display_name LIKE ? COLLATE NOCASE
            ORDER BY CASE WHEN id = ? THEN 0 WHEN display_name = ? COLLATE NOCASE THEN 1 ELSE 2 END, display_name ASC
            LIMIT 1
            """,
            (technician_query, technician_query, f"%{technician_query}%", technician_query, technician_query),
        ).fetchone()
        if not technician:
            fallback_technician = conn.execute(
                """
                SELECT assigned_technician_id AS id, assigned_technician_id AS display_name, NULL AS email
                FROM tickets
                WHERE assigned_technician_id = ?
                LIMIT 1
                """,
                (technician_query,),
            ).fetchone()
            if not fallback_technician:
                return {"status": "not_found", "technician_query": technician_query}
            technician = fallback_technician
        open_tickets = conn.execute(
            """
            SELECT id, subject, status, priority, category, updated_at
            FROM tickets
            WHERE assigned_technician_id = ? AND status = 'Open'
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (technician["id"], limit_open),
        ).fetchall()
        recent_tickets = conn.execute(
            """
            SELECT id, subject, status, priority, category, updated_at
            FROM tickets
            WHERE assigned_technician_id = ?
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (technician["id"], limit_recent),
        ).fetchall()
        stats = conn.execute(
            """
            WITH log_tickets AS (
                SELECT DISTINCT ticket_id FROM ticket_logs
            ),
            attachment_tickets AS (
                SELECT DISTINCT ticket_id FROM ticket_attachments
            ),
            chunk_counts AS (
                SELECT ticket_id, COUNT(*) AS chunk_count
                FROM ticket_document_chunks
                GROUP BY ticket_id
            )
            SELECT COUNT(*) AS total_tickets,
                   SUM(CASE WHEN t.status = 'Open' THEN 1 ELSE 0 END) AS open_tickets,
                   SUM(CASE WHEN t.status = 'Closed' THEN 1 ELSE 0 END) AS closed_tickets,
                   MAX(t.updated_at) AS latest_activity_at,
                   SUM(CASE WHEN td.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS detail_tickets,
                   SUM(CASE WHEN lt.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS log_tickets,
                   SUM(CASE WHEN at.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS attachment_tickets,
                   SUM(CASE WHEN doc.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS document_tickets,
                   COALESCE(SUM(cc.chunk_count), 0) AS chunk_count
            FROM tickets t
            LEFT JOIN ticket_details td ON td.ticket_id = t.id
            LEFT JOIN log_tickets lt ON lt.ticket_id = t.id
            LEFT JOIN attachment_tickets at ON at.ticket_id = t.id
            LEFT JOIN ticket_documents doc ON doc.ticket_id = t.id
            LEFT JOIN chunk_counts cc ON cc.ticket_id = t.id
            WHERE t.assigned_technician_id = ?
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
        status_breakdown = conn.execute(
            """
            SELECT COALESCE(status, 'unknown') AS status, COUNT(*) AS ticket_count
            FROM tickets
            WHERE assigned_technician_id = ?
            GROUP BY COALESCE(status, 'unknown')
            ORDER BY ticket_count DESC, status ASC
            """,
            (technician["id"],),
        ).fetchall()
        priority_breakdown = conn.execute(
            """
            SELECT COALESCE(priority, 'unknown') AS priority, COUNT(*) AS ticket_count
            FROM tickets
            WHERE assigned_technician_id = ?
            GROUP BY COALESCE(priority, 'unknown')
            ORDER BY ticket_count DESC, priority ASC
            LIMIT 10
            """,
            (technician["id"],),
        ).fetchall()
        category_breakdown = conn.execute(
            """
            SELECT COALESCE(category, 'unknown') AS category, COUNT(*) AS ticket_count
            FROM tickets
            WHERE assigned_technician_id = ?
            GROUP BY COALESCE(category, 'unknown')
            ORDER BY ticket_count DESC, category ASC
            LIMIT 10
            """,
            (technician["id"],),
        ).fetchall()
    stats_dict = dict(stats)
    total_tickets = int(stats_dict.get("total_tickets") or 0)
    detail_tickets = int(stats_dict.get("detail_tickets") or 0)
    log_tickets = int(stats_dict.get("log_tickets") or 0)
    attachment_tickets = int(stats_dict.get("attachment_tickets") or 0)
    document_tickets = int(stats_dict.get("document_tickets") or 0)
    stats_dict.update({
        "detail_coverage_ratio": _ratio(detail_tickets, total_tickets),
        "log_coverage_ratio": _ratio(log_tickets, total_tickets),
        "attachment_coverage_ratio": _ratio(attachment_tickets, total_tickets),
        "document_coverage_ratio": _ratio(document_tickets, total_tickets),
    })
    return {
        "status": "ok",
        "technician": {"id": technician["id"], "display_name": technician["display_name"], "email": technician["email"]},
        "stats": stats_dict,
        "open_tickets": [dict(row) for row in open_tickets],
        "recent_tickets": [dict(row) for row in recent_tickets],
        "recent_log_types": [dict(row) for row in recent_log_types],
        "status_breakdown": [dict(row) for row in status_breakdown],
        "priority_breakdown": [dict(row) for row in priority_breakdown],
        "category_breakdown": [dict(row) for row in category_breakdown],
    }
