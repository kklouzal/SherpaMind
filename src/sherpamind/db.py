from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar

from .text_cleanup import normalize_metadata_label
from .time_utils import parse_sherpadesk_timestamp

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS accounts (
    id TEXT PRIMARY KEY,
    name TEXT,
    raw_json TEXT NOT NULL,
    updated_at TEXT,
    synced_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    account_id TEXT,
    display_name TEXT,
    email TEXT,
    raw_json TEXT NOT NULL,
    updated_at TEXT,
    synced_at TEXT NOT NULL,
    FOREIGN KEY(account_id) REFERENCES accounts(id)
);

CREATE TABLE IF NOT EXISTS technicians (
    id TEXT PRIMARY KEY,
    display_name TEXT,
    email TEXT,
    raw_json TEXT NOT NULL,
    updated_at TEXT,
    synced_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tickets (
    id TEXT PRIMARY KEY,
    account_id TEXT,
    user_id TEXT,
    assigned_technician_id TEXT,
    subject TEXT,
    status TEXT,
    priority TEXT,
    category TEXT,
    created_at TEXT,
    updated_at TEXT,
    closed_at TEXT,
    raw_json TEXT NOT NULL,
    synced_at TEXT NOT NULL,
    FOREIGN KEY(account_id) REFERENCES accounts(id),
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(assigned_technician_id) REFERENCES technicians(id)
);

CREATE TABLE IF NOT EXISTS ticket_details (
    ticket_id TEXT PRIMARY KEY,
    workpad TEXT,
    note TEXT,
    initial_response TEXT,
    sla_response_date TEXT,
    sla_complete_date TEXT,
    waiting_date TEXT,
    next_step_date TEXT,
    ticketlogs_count INTEGER NOT NULL DEFAULT 0,
    timelogs_count INTEGER NOT NULL DEFAULT 0,
    attachments_count INTEGER NOT NULL DEFAULT 0,
    raw_json TEXT NOT NULL,
    synced_at TEXT NOT NULL,
    FOREIGN KEY(ticket_id) REFERENCES tickets(id)
);

CREATE TABLE IF NOT EXISTS ticket_attachments (
    id TEXT PRIMARY KEY,
    ticket_id TEXT NOT NULL,
    name TEXT,
    url TEXT,
    size INTEGER,
    recorded_at TEXT,
    raw_json TEXT NOT NULL,
    synced_at TEXT NOT NULL,
    FOREIGN KEY(ticket_id) REFERENCES tickets(id)
);

CREATE TABLE IF NOT EXISTS ticket_logs (
    id TEXT PRIMARY KEY,
    ticket_id TEXT NOT NULL,
    log_type TEXT,
    record_date TEXT,
    note TEXT,
    plain_note TEXT,
    user_id TEXT,
    user_email TEXT,
    user_name TEXT,
    is_tech_only INTEGER,
    is_waiting INTEGER,
    raw_json TEXT NOT NULL,
    synced_at TEXT NOT NULL,
    FOREIGN KEY(ticket_id) REFERENCES tickets(id)
);

CREATE TABLE IF NOT EXISTS ticket_time_logs (
    id TEXT PRIMARY KEY,
    ticket_id TEXT NOT NULL,
    record_date TEXT,
    note TEXT,
    raw_json TEXT NOT NULL,
    synced_at TEXT NOT NULL,
    FOREIGN KEY(ticket_id) REFERENCES tickets(id)
);

CREATE TABLE IF NOT EXISTS ticket_comments (
    id TEXT PRIMARY KEY,
    ticket_id TEXT NOT NULL,
    author_type TEXT,
    author_id TEXT,
    created_at TEXT,
    body TEXT,
    raw_json TEXT NOT NULL,
    synced_at TEXT NOT NULL,
    FOREIGN KEY(ticket_id) REFERENCES tickets(id)
);

CREATE TABLE IF NOT EXISTS ticket_documents (
    doc_id TEXT PRIMARY KEY,
    ticket_id TEXT NOT NULL,
    status TEXT,
    account TEXT,
    user_name TEXT,
    technician TEXT,
    updated_at TEXT,
    text TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    content_hash TEXT,
    synced_at TEXT NOT NULL,
    FOREIGN KEY(ticket_id) REFERENCES tickets(id)
);

CREATE TABLE IF NOT EXISTS ticket_document_chunks (
    chunk_id TEXT PRIMARY KEY,
    doc_id TEXT NOT NULL,
    ticket_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    text TEXT NOT NULL,
    content_hash TEXT,
    synced_at TEXT NOT NULL,
    FOREIGN KEY(doc_id) REFERENCES ticket_documents(doc_id),
    FOREIGN KEY(ticket_id) REFERENCES tickets(id)
);

CREATE TABLE IF NOT EXISTS sync_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mode TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS ingest_mode_leases (
    mode TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    leased_at TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS api_request_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    recorded_at TEXT NOT NULL,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    status_code INTEGER,
    outcome TEXT NOT NULL,
    attempt_kind TEXT,
    raw_json TEXT
);

CREATE TABLE IF NOT EXISTS ticket_detail_failures (
    ticket_id TEXT PRIMARY KEY,
    status_code INTEGER,
    error_kind TEXT,
    error_message TEXT,
    last_path TEXT,
    last_failure_at TEXT NOT NULL,
    next_retry_at TEXT,
    failure_count INTEGER NOT NULL DEFAULT 1,
    permanent_failure INTEGER NOT NULL DEFAULT 0,
    raw_json TEXT NOT NULL,
    FOREIGN KEY(ticket_id) REFERENCES tickets(id)
);

CREATE TABLE IF NOT EXISTS vector_chunk_index (
    chunk_id TEXT PRIMARY KEY,
    doc_id TEXT NOT NULL,
    ticket_id TEXT NOT NULL,
    vector_json TEXT NOT NULL,
    dims INTEGER NOT NULL,
    content_hash TEXT,
    synced_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS alert_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_type TEXT NOT NULL,
    ticket_id TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    payload_json TEXT,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    available_at TEXT NOT NULL,
    leased_at TEXT,
    lease_expires_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    writeback_status TEXT,
    writeback_attempt_count INTEGER NOT NULL DEFAULT 0,
    writeback_at TEXT,
    writeback_last_error TEXT,
    writeback_response_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    sent_at TEXT
);

CREATE TABLE IF NOT EXISTS worker_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    worker_name TEXT NOT NULL,
    mode TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS worker_leases (
    worker_name TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    leased_at TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS derived_refresh_queue (
    ticket_id TEXT PRIMARY KEY,
    source TEXT,
    priority INTEGER NOT NULL DEFAULT 100,
    requested_at TEXT NOT NULL,
    leased_at TEXT,
    lease_expires_at TEXT,
    completed_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ticket_alert_state (
    ticket_id TEXT PRIMARY KEY,
    is_currently_monitored_open INTEGER NOT NULL DEFAULT 0,
    open_cycle_id INTEGER NOT NULL DEFAULT 0,
    open_alert_sent_at TEXT,
    last_seen_open_at TEXT,
    missing_open_polls INTEGER NOT NULL DEFAULT 0,
    close_confirmed_at TEXT,
    last_non_tech_event_key TEXT,
    last_non_tech_alerted_key TEXT,
    last_status TEXT,
    last_updated_time TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ticket_taxonomy_classes (
    id TEXT PRIMARY KEY,
    parent_id TEXT,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    hierarchy_level INTEGER,
    is_lastchild INTEGER,
    is_restrict_to_techs INTEGER,
    is_active INTEGER,
    priority_id TEXT,
    level_override INTEGER,
    todo_templates TEXT,
    raw_json TEXT NOT NULL,
    synced_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ticket_classification_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    dedupe_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    trigger_source TEXT,
    ticket_status TEXT,
    ticket_updated_time TEXT,
    current_class_id TEXT,
    current_class_name TEXT,
    payload_json TEXT NOT NULL,
    prompt_json TEXT,
    result_class_id TEXT,
    result_class_path TEXT,
    confidence TEXT,
    rationale TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    dispatched_at TEXT,
    completed_at TEXT,
    last_error TEXT,
    writeback_status TEXT,
    writeback_attempt_count INTEGER NOT NULL DEFAULT 0,
    writeback_at TEXT,
    writeback_last_error TEXT,
    writeback_response_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(ticket_id) REFERENCES tickets(id)
);
"""

DB_LOCK_RETRY_DELAY_SECONDS = 90
DB_LOCK_MAX_RETRIES = 5
DB_STALE_RUNNING_AFTER_SECONDS = 6 * 3600
DB_SQLITE_TIMEOUT_SECONDS = 30.0
DB_BUSY_TIMEOUT_MS = 30000

T = TypeVar("T")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=DB_SQLITE_TIMEOUT_SECONDS)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {DB_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def is_db_locked_error(exc: BaseException) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and "database is locked" in str(exc).lower()


def run_with_db_lock_retries(
    operation: Callable[[], T],
    *,
    max_retries: int = DB_LOCK_MAX_RETRIES,
    delay_seconds: int = DB_LOCK_RETRY_DELAY_SECONDS,
    on_retry: Callable[[int, int, BaseException], None] | None = None,
) -> T:
    attempts = 0
    while True:
        try:
            return operation()
        except BaseException as exc:
            if not is_db_locked_error(exc):
                raise
            attempts += 1
            if attempts > max_retries:
                raise
            if on_retry is not None:
                on_retry(attempts, max_retries, exc)
            time.sleep(delay_seconds)


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row['name'] for row in rows}


def initialize_db(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        # Lightweight schema evolution for older local DBs created before new columns/tables existed.
        table_names = {row['name'] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if 'ticket_documents' in table_names:
            ticket_doc_cols = _table_columns(conn, 'ticket_documents')
            if 'content_hash' not in ticket_doc_cols:
                conn.execute("ALTER TABLE ticket_documents ADD COLUMN content_hash TEXT")
        if 'ticket_detail_failures' in table_names:
            failure_cols = _table_columns(conn, 'ticket_detail_failures')
            if 'last_path' not in failure_cols:
                conn.execute("ALTER TABLE ticket_detail_failures ADD COLUMN last_path TEXT")
            if 'raw_json' not in failure_cols:
                conn.execute("ALTER TABLE ticket_detail_failures ADD COLUMN raw_json TEXT NOT NULL DEFAULT '{}' ")
        if 'ticket_classification_events' in table_names:
            class_event_cols = _table_columns(conn, 'ticket_classification_events')
            if 'writeback_status' not in class_event_cols:
                conn.execute("ALTER TABLE ticket_classification_events ADD COLUMN writeback_status TEXT")
            if 'writeback_attempt_count' not in class_event_cols:
                conn.execute("ALTER TABLE ticket_classification_events ADD COLUMN writeback_attempt_count INTEGER NOT NULL DEFAULT 0")
            if 'writeback_at' not in class_event_cols:
                conn.execute("ALTER TABLE ticket_classification_events ADD COLUMN writeback_at TEXT")
            if 'writeback_last_error' not in class_event_cols:
                conn.execute("ALTER TABLE ticket_classification_events ADD COLUMN writeback_last_error TEXT")
            if 'writeback_response_json' not in class_event_cols:
                conn.execute("ALTER TABLE ticket_classification_events ADD COLUMN writeback_response_json TEXT")
        conn.commit()


def start_ingest_run(db_path: Path, mode: str, notes: str | None = None) -> int:
    started_at = now_iso()

    def _operation() -> int:
        with connect(db_path) as conn:
            stale_same_mode_rows = conn.execute(
                "SELECT id, notes FROM ingest_runs WHERE mode = ? AND status = 'running'",
                (mode,),
            ).fetchall()
            for row in stale_same_mode_rows:
                note_suffix = f" [auto-cleanup {started_at}: superseded by newer {mode} run]"
                existing_notes = row["notes"] or ""
                next_notes = f"{existing_notes}{note_suffix}" if note_suffix not in existing_notes else existing_notes
                conn.execute(
                    "UPDATE ingest_runs SET finished_at = ?, status = ?, notes = ? WHERE id = ?",
                    (started_at, "abandoned", next_notes, row["id"]),
                )
            cursor = conn.execute(
                "INSERT INTO ingest_runs(mode, started_at, status, notes) VALUES(?, ?, ?, ?)",
                (mode, started_at, "running", notes),
            )
            conn.commit()
            return int(cursor.lastrowid)

    return run_with_db_lock_retries(_operation)


def finish_ingest_run(db_path: Path, run_id: int, status: str, notes: str | None = None) -> None:
    def _operation() -> None:
        with connect(db_path) as conn:
            conn.execute(
                "UPDATE ingest_runs SET finished_at = ?, status = ?, notes = ? WHERE id = ?",
                (now_iso(), status, notes, run_id),
            )
            conn.commit()

    run_with_db_lock_retries(_operation)


def cleanup_stale_ingest_runs(
    db_path: Path,
    *,
    stale_after_seconds: int = DB_STALE_RUNNING_AFTER_SECONDS,
    final_status: str = "abandoned",
) -> int:
    stale_before = (datetime.now(timezone.utc) - timedelta(seconds=stale_after_seconds)).isoformat()
    finished_at = now_iso()

    def _operation() -> int:
        with connect(db_path) as conn:
            stale_rows = conn.execute(
                "SELECT id, notes FROM ingest_runs WHERE status = 'running' AND started_at < ?",
                (stale_before,),
            ).fetchall()
            cleaned = 0
            for row in stale_rows:
                note_suffix = f" [auto-cleanup {finished_at}: stale running row]"
                existing_notes = row["notes"] or ""
                next_notes = f"{existing_notes}{note_suffix}" if note_suffix not in existing_notes else existing_notes
                conn.execute(
                    "UPDATE ingest_runs SET finished_at = ?, status = ?, notes = ? WHERE id = ?",
                    (finished_at, final_status, next_notes, row["id"]),
                )
                cleaned += 1
            conn.commit()
            return cleaned

    return run_with_db_lock_retries(_operation)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def upsert_accounts(db_path: Path, accounts: list[dict[str, Any]], synced_at: str | None = None) -> int:
    synced_at = synced_at or now_iso()
    with connect(db_path) as conn:
        for account in accounts:
            conn.execute(
                """
                INSERT INTO accounts(id, name, raw_json, updated_at, synced_at)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = CASE
                        WHEN excluded.name IS NOT NULL AND trim(excluded.name) <> ''
                        THEN excluded.name
                        ELSE accounts.name
                    END,
                    raw_json = CASE
                        WHEN json_extract(excluded.raw_json, '$.source') = 'ticket_stub'
                             AND COALESCE(json_extract(accounts.raw_json, '$.source'), '') <> 'ticket_stub'
                        THEN accounts.raw_json
                        ELSE excluded.raw_json
                    END,
                    updated_at = COALESCE(excluded.updated_at, accounts.updated_at),
                    synced_at = excluded.synced_at
                """,
                (
                    str(account["id"]),
                    account.get("name"),
                    _json(account),
                    account.get("updated") or account.get("updated_time"),
                    synced_at,
                ),
            )
        conn.commit()
    return len(accounts)


def _display_name(record: dict[str, Any]) -> str | None:
    return record.get("FullName") or record.get("full_name2") or " ".join(
        part for part in [record.get("firstname"), record.get("lastname")] if part
    ) or None


def _ticket_account_stub(ticket: dict[str, Any]) -> dict[str, Any] | None:
    account_id = ticket.get("account_id")
    if account_id is None:
        return None
    name = ticket.get("account_name") or ticket.get("company") or ticket.get("account") or None
    return {
        "id": account_id,
        "name": name,
        "updated": ticket.get("updated_time"),
        "source": "ticket_stub",
    }


def _ticket_user_stub(ticket: dict[str, Any]) -> dict[str, Any] | None:
    user_id = ticket.get("user_id")
    if user_id is None:
        return None
    display_name = (
        ticket.get("user_name")
        or ticket.get("contact_name")
        or ticket.get("requester_name")
        or " ".join(
            part
            for part in [ticket.get("user_firstname"), ticket.get("user_lastname")]
            if part
        )
        or None
    )
    return {
        "id": user_id,
        "account_id": ticket.get("account_id"),
        "FullName": display_name,
        "email": ticket.get("user_email"),
        "updated_time": ticket.get("updated_time"),
        "source": "ticket_stub",
    }


def _ticket_technician_stub(ticket: dict[str, Any]) -> dict[str, Any] | None:
    technician_id = ticket.get("tech_id")
    if technician_id is None:
        return None
    display_name = (
        ticket.get("assigned_technician_name")
        or ticket.get("technician_name")
        or ticket.get("tech_name")
        or " ".join(
            part
            for part in [ticket.get("technician_firstname"), ticket.get("technician_lastname")]
            if part
        )
        or None
    )
    return {
        "id": technician_id,
        "FullName": display_name,
        "email": ticket.get("technician_email"),
        "updated_time": ticket.get("updated_time"),
        "source": "ticket_stub",
    }


def upsert_users(db_path: Path, users: list[dict[str, Any]], synced_at: str | None = None) -> int:
    synced_at = synced_at or now_iso()
    with connect(db_path) as conn:
        for user in users:
            conn.execute(
                """
                INSERT INTO users(id, account_id, display_name, email, raw_json, updated_at, synced_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    account_id = COALESCE(excluded.account_id, users.account_id),
                    display_name = CASE
                        WHEN excluded.display_name IS NOT NULL AND trim(excluded.display_name) <> ''
                        THEN excluded.display_name
                        ELSE users.display_name
                    END,
                    email = COALESCE(excluded.email, users.email),
                    raw_json = CASE
                        WHEN json_extract(excluded.raw_json, '$.source') = 'ticket_stub'
                             AND COALESCE(json_extract(users.raw_json, '$.source'), '') <> 'ticket_stub'
                        THEN users.raw_json
                        ELSE excluded.raw_json
                    END,
                    updated_at = COALESCE(excluded.updated_at, users.updated_at),
                    synced_at = excluded.synced_at
                """,
                (
                    str(user["id"]),
                    str(user["account_id"]) if user.get("account_id") is not None else None,
                    _display_name(user),
                    user.get("email"),
                    _json(user),
                    user.get("updated") or user.get("modified") or user.get("updated_time"),
                    synced_at,
                ),
            )
        conn.commit()
    return len(users)


def upsert_technicians(db_path: Path, technicians: list[dict[str, Any]], synced_at: str | None = None) -> int:
    synced_at = synced_at or now_iso()
    with connect(db_path) as conn:
        for technician in technicians:
            display_name = _display_name(technician)
            email = technician.get("email")
            updated_at = technician.get("updated") or technician.get("modified") or technician.get("updated_time")
            conn.execute(
                """
                INSERT INTO technicians(id, display_name, email, raw_json, updated_at, synced_at)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    display_name = CASE
                        WHEN excluded.display_name IS NOT NULL AND trim(excluded.display_name) <> ''
                        THEN excluded.display_name
                        ELSE technicians.display_name
                    END,
                    email = COALESCE(excluded.email, technicians.email),
                    raw_json = CASE
                        WHEN json_extract(excluded.raw_json, '$.source') = 'ticket_stub'
                             AND COALESCE(json_extract(technicians.raw_json, '$.source'), '') <> 'ticket_stub'
                        THEN technicians.raw_json
                        ELSE excluded.raw_json
                    END,
                    updated_at = COALESCE(excluded.updated_at, technicians.updated_at),
                    synced_at = excluded.synced_at
                """,
                (
                    str(technician["id"]),
                    display_name,
                    email,
                    _json(technician),
                    updated_at,
                    synced_at,
                ),
            )
        conn.commit()
    return len(technicians)



def _bool_to_int(value: Any) -> int | None:
    if value is None:
        return None
    return 1 if bool(value) else 0


def replace_ticket_taxonomy_classes(db_path: Path, rows: list[dict[str, Any]], synced_at: str | None = None) -> int:
    synced_at = synced_at or now_iso()
    with connect(db_path) as conn:
        conn.execute("DELETE FROM ticket_taxonomy_classes")
        for row in rows:
            conn.execute(
                """
                INSERT INTO ticket_taxonomy_classes(
                    id, parent_id, name, path, hierarchy_level, is_lastchild, is_restrict_to_techs,
                    is_active, priority_id, level_override, todo_templates, raw_json, synced_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(row["id"]),
                    str(row["parent_id"]) if row.get("parent_id") not in (None, "") else None,
                    row["name"],
                    row["path"],
                    row.get("hierarchy_level"),
                    _bool_to_int(row.get("is_lastchild")),
                    _bool_to_int(row.get("is_restrict_to_techs")),
                    _bool_to_int(row.get("is_active")),
                    str(row["priority_id"]) if row.get("priority_id") not in (None, "") else None,
                    row.get("level_override"),
                    row.get("todo_templates"),
                    _json(row.get("raw_json") or row),
                    synced_at,
                ),
            )
        conn.commit()
    return len(rows)


def list_ticket_taxonomy_classes(db_path: Path, *, active_only: bool = False, leaves_only: bool = False) -> list[dict[str, Any]]:
    clauses = []
    params: list[Any] = []
    if active_only:
        clauses.append("COALESCE(is_active, 1) = 1")
    if leaves_only:
        clauses.append("COALESCE(is_lastchild, 0) = 1")
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT id, parent_id, name, path, hierarchy_level, is_lastchild, is_restrict_to_techs,
                   is_active, priority_id, level_override, todo_templates, synced_at
            FROM ticket_taxonomy_classes
            {where}
            ORDER BY path COLLATE NOCASE, id
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]



def get_ticket_taxonomy_freshness(db_path: Path) -> dict[str, Any]:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS class_count,
                   MIN(synced_at) AS oldest_synced_at,
                   MAX(synced_at) AS newest_synced_at,
                   SUM(CASE WHEN COALESCE(is_active, 1) = 1 THEN 1 ELSE 0 END) AS active_count,
                   SUM(CASE WHEN COALESCE(is_active, 1) = 1 AND COALESCE(is_lastchild, 0) = 1 THEN 1 ELSE 0 END) AS active_leaf_count
            FROM ticket_taxonomy_classes
            """
        ).fetchone()
    return dict(row)


def get_ticket_taxonomy_class(db_path: Path, class_id: str) -> dict[str, Any] | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT id, parent_id, name, path, hierarchy_level, is_lastchild, is_active, synced_at
            FROM ticket_taxonomy_classes
            WHERE id = ?
            """,
            (str(class_id),),
        ).fetchone()
    return dict(row) if row else None

def upsert_tickets(db_path: Path, tickets: list[dict[str, Any]], synced_at: str | None = None) -> int:
    synced_at = synced_at or now_iso()
    account_stubs = [stub for ticket in tickets if (stub := _ticket_account_stub(ticket)) is not None]
    if account_stubs:
        upsert_accounts(db_path, account_stubs, synced_at=synced_at)
    user_stubs = [stub for ticket in tickets if (stub := _ticket_user_stub(ticket)) is not None]
    if user_stubs:
        upsert_users(db_path, user_stubs, synced_at=synced_at)
    technician_stubs = [stub for ticket in tickets if (stub := _ticket_technician_stub(ticket)) is not None]
    if technician_stubs:
        upsert_technicians(db_path, technician_stubs, synced_at=synced_at)
    with connect(db_path) as conn:
        for ticket in tickets:
            core = _ticket_core_fields(ticket)
            conn.execute(
                """
                INSERT INTO tickets(
                    id, account_id, user_id, assigned_technician_id, subject, status, priority, category,
                    created_at, updated_at, closed_at, raw_json, synced_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    account_id = excluded.account_id,
                    user_id = excluded.user_id,
                    assigned_technician_id = excluded.assigned_technician_id,
                    subject = excluded.subject,
                    status = excluded.status,
                    priority = excluded.priority,
                    category = excluded.category,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at,
                    closed_at = excluded.closed_at,
                    raw_json = excluded.raw_json,
                    synced_at = excluded.synced_at
                """,
                (
                    str(ticket["id"]),
                    core["account_id"],
                    core["user_id"],
                    core["assigned_technician_id"],
                    core["subject"],
                    core["status"],
                    core["priority"],
                    core["category"],
                    core["created_at"],
                    core["updated_at"],
                    core["closed_at"],
                    _json(ticket),
                    synced_at,
                ),
            )
        conn.commit()
    return len(tickets)


def backfill_ticket_entity_stubs(db_path: Path, synced_at: str | None = None) -> dict[str, Any]:
    synced_at = synced_at or now_iso()
    with connect(db_path) as conn:
        ticket_rows = conn.execute("SELECT raw_json FROM tickets").fetchall()
        before_counts = {
            "accounts": int(conn.execute("SELECT COUNT(*) AS c FROM accounts").fetchone()["c"]),
            "users": int(conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]),
            "technicians": int(conn.execute("SELECT COUNT(*) AS c FROM technicians").fetchone()["c"]),
        }

    def _collect_stubs(factory):
        stubs = []
        seen_ids: set[str] = set()
        for row in ticket_rows:
            ticket = json.loads(row["raw_json"])
            stub = factory(ticket)
            if not stub:
                continue
            stub_id = str(stub["id"])
            if stub_id in seen_ids:
                continue
            seen_ids.add(stub_id)
            stubs.append(stub)
        return stubs

    account_stubs = _collect_stubs(_ticket_account_stub)
    user_stubs = _collect_stubs(_ticket_user_stub)
    technician_stubs = _collect_stubs(_ticket_technician_stub)

    if account_stubs:
        upsert_accounts(db_path, account_stubs, synced_at=synced_at)
    if user_stubs:
        upsert_users(db_path, user_stubs, synced_at=synced_at)
    if technician_stubs:
        upsert_technicians(db_path, technician_stubs, synced_at=synced_at)

    with connect(db_path) as conn:
        after_counts = {
            "accounts": int(conn.execute("SELECT COUNT(*) AS c FROM accounts").fetchone()["c"]),
            "users": int(conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]),
            "technicians": int(conn.execute("SELECT COUNT(*) AS c FROM technicians").fetchone()["c"]),
        }

    return {
        "status": "ok",
        "observed_ticket_account_ids": len(account_stubs),
        "observed_ticket_user_ids": len(user_stubs),
        "observed_ticket_technician_ids": len(technician_stubs),
        "account_count_before": before_counts["accounts"],
        "account_count_after": after_counts["accounts"],
        "account_rows_added": max(after_counts["accounts"] - before_counts["accounts"], 0),
        "user_count_before": before_counts["users"],
        "user_count_after": after_counts["users"],
        "user_rows_added": max(after_counts["users"] - before_counts["users"], 0),
        "technician_count_before": before_counts["technicians"],
        "technician_count_after": after_counts["technicians"],
        "technician_rows_added": max(after_counts["technicians"] - before_counts["technicians"], 0),
    }


def backfill_ticket_technician_stubs(db_path: Path, synced_at: str | None = None) -> dict[str, int | str]:
    result = backfill_ticket_entity_stubs(db_path, synced_at=synced_at)
    return {
        "status": result["status"],
        "observed_ticket_technician_ids": result["observed_ticket_technician_ids"],
        "technician_count_before": result["technician_count_before"],
        "technician_count_after": result["technician_count_after"],
        "technician_rows_added": result["technician_rows_added"],
    }


def _ticket_core_fields(ticket: dict[str, Any]) -> dict[str, Any]:
    return {
        "account_id": str(ticket["account_id"]) if ticket.get("account_id") is not None else None,
        "user_id": str(ticket["user_id"]) if ticket.get("user_id") is not None else None,
        "assigned_technician_id": str(ticket["tech_id"]) if ticket.get("tech_id") is not None else None,
        "subject": ticket.get("subject"),
        "status": ticket.get("status"),
        "priority": normalize_metadata_label(ticket.get("priority_name") or ticket.get("priority")),
        "category": normalize_metadata_label(
            ticket.get("creation_category_name") or ticket.get("category") or ticket.get("class_name") or ticket.get("submission_category")
        ),
        "created_at": ticket.get("created_time"),
        "updated_at": ticket.get("updated_time"),
        "closed_at": ticket.get("closed_time"),
    }


def backfill_ticket_core_fields(db_path: Path, synced_at: str | None = None) -> dict[str, Any]:
    synced_at = synced_at or now_iso()
    with connect(db_path) as conn:
        rows = conn.execute("SELECT id, raw_json, account_id, user_id, assigned_technician_id, subject, status, priority, category, created_at, updated_at, closed_at, synced_at FROM tickets").fetchall()
        repaired_rows = 0
        field_repairs = {
            "account_id": 0,
            "user_id": 0,
            "assigned_technician_id": 0,
            "subject": 0,
            "status": 0,
            "priority": 0,
            "category": 0,
            "created_at": 0,
            "updated_at": 0,
            "closed_at": 0,
        }

        for row in rows:
            ticket = json.loads(row["raw_json"])
            repaired = _ticket_core_fields(ticket)
            updates: dict[str, Any] = {}
            for field_name, repaired_value in repaired.items():
                current_value = row[field_name]
                if current_value in (None, "") and repaired_value not in (None, ""):
                    updates[field_name] = repaired_value
                    field_repairs[field_name] += 1
            if not updates:
                continue
            updates["synced_at"] = synced_at
            assignments = ", ".join(f"{field} = ?" for field in updates)
            conn.execute(
                f"UPDATE tickets SET {assignments} WHERE id = ?",
                (*updates.values(), row["id"]),
            )
            repaired_rows += 1
        conn.commit()

    return {
        "status": "ok",
        "ticket_rows_scanned": len(rows),
        "ticket_rows_repaired": repaired_rows,
        "field_repairs": field_repairs,
        "synced_at": synced_at,
    }


def upsert_ticket_details(db_path: Path, ticket_details: list[dict[str, Any]], synced_at: str | None = None) -> int:
    synced_at = synced_at or now_iso()
    with connect(db_path) as conn:
        for detail in ticket_details:
            ticket_id = str(detail["id"])
            attachments = detail.get("attachments") if isinstance(detail.get("attachments"), list) else []
            ticketlogs = detail.get("ticketlogs") if isinstance(detail.get("ticketlogs"), list) else []
            timelogs = detail.get("timelogs") if isinstance(detail.get("timelogs"), list) else []
            conn.execute(
                """
                INSERT INTO ticket_details(
                    ticket_id, workpad, note, initial_response, sla_response_date, sla_complete_date,
                    waiting_date, next_step_date, ticketlogs_count, timelogs_count, attachments_count,
                    raw_json, synced_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticket_id) DO UPDATE SET
                    workpad = excluded.workpad,
                    note = excluded.note,
                    initial_response = excluded.initial_response,
                    sla_response_date = excluded.sla_response_date,
                    sla_complete_date = excluded.sla_complete_date,
                    waiting_date = excluded.waiting_date,
                    next_step_date = excluded.next_step_date,
                    ticketlogs_count = excluded.ticketlogs_count,
                    timelogs_count = excluded.timelogs_count,
                    attachments_count = excluded.attachments_count,
                    raw_json = excluded.raw_json,
                    synced_at = excluded.synced_at
                """,
                (
                    ticket_id,
                    detail.get("workpad"),
                    detail.get("note"),
                    str(detail.get("initial_response")) if detail.get("initial_response") is not None else None,
                    detail.get("sla_response_date"),
                    detail.get("sla_complete_date"),
                    detail.get("waiting_date"),
                    detail.get("next_step_date"),
                    len(ticketlogs),
                    len(timelogs),
                    len(attachments),
                    _json(detail),
                    synced_at,
                ),
            )

            conn.execute("DELETE FROM ticket_attachments WHERE ticket_id = ?", (ticket_id,))
            for attachment in attachments:
                conn.execute(
                    """
                    INSERT INTO ticket_attachments(id, ticket_id, name, url, size, recorded_at, raw_json, synced_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        ticket_id = excluded.ticket_id,
                        name = excluded.name,
                        url = excluded.url,
                        size = excluded.size,
                        recorded_at = excluded.recorded_at,
                        raw_json = excluded.raw_json,
                        synced_at = excluded.synced_at
                    """,
                    (
                        str(attachment["id"]),
                        ticket_id,
                        attachment.get("name"),
                        attachment.get("url"),
                        attachment.get("size"),
                        attachment.get("date"),
                        _json(attachment),
                        synced_at,
                    ),
                )

            for log in ticketlogs:
                log_id = str(log["id"])
                user_name = " ".join(part for part in [log.get("user_firstname"), log.get("user_lastname")] if part) or None
                conn.execute(
                    """
                    INSERT INTO ticket_logs(
                        id, ticket_id, log_type, record_date, note, plain_note, user_id, user_email, user_name,
                        is_tech_only, is_waiting, raw_json, synced_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        ticket_id = excluded.ticket_id,
                        log_type = excluded.log_type,
                        record_date = excluded.record_date,
                        note = excluded.note,
                        plain_note = excluded.plain_note,
                        user_id = excluded.user_id,
                        user_email = excluded.user_email,
                        user_name = excluded.user_name,
                        is_tech_only = excluded.is_tech_only,
                        is_waiting = excluded.is_waiting,
                        raw_json = excluded.raw_json,
                        synced_at = excluded.synced_at
                    """,
                    (
                        log_id,
                        ticket_id,
                        log.get("log_type"),
                        log.get("record_date"),
                        log.get("note"),
                        log.get("plain_note"),
                        str(log.get("user_id")) if log.get("user_id") is not None else None,
                        log.get("user_email"),
                        user_name,
                        1 if log.get("is_tech_only") else 0,
                        1 if log.get("is_waiting") else 0,
                        _json(log),
                        synced_at,
                    ),
                )

            for timelog in timelogs:
                if timelog.get("id") is None:
                    continue
                conn.execute(
                    """
                    INSERT INTO ticket_time_logs(id, ticket_id, record_date, note, raw_json, synced_at)
                    VALUES(?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        ticket_id = excluded.ticket_id,
                        record_date = excluded.record_date,
                        note = excluded.note,
                        raw_json = excluded.raw_json,
                        synced_at = excluded.synced_at
                    """,
                    (
                        str(timelog["id"]),
                        ticket_id,
                        timelog.get("record_date"),
                        timelog.get("note"),
                        _json(timelog),
                        synced_at,
                    ),
                )
        conn.commit()
    return len(ticket_details)


def replace_ticket_documents(db_path: Path, docs: list[dict[str, Any]], synced_at: str | None = None) -> int:
    synced_at = synced_at or now_iso()
    with connect(db_path) as conn:
        if docs:
            ticket_ids = sorted({str(doc["ticket_id"]) for doc in docs})
            placeholders = ",".join("?" for _ in ticket_ids)
            conn.execute(f"DELETE FROM ticket_documents WHERE ticket_id IN ({placeholders})", ticket_ids)
        for doc in docs:
            conn.execute(
                """
                INSERT INTO ticket_documents(doc_id, ticket_id, status, account, user_name, technician, updated_at, text, raw_json, content_hash, synced_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    doc["doc_id"],
                    str(doc["ticket_id"]),
                    doc.get("status"),
                    doc.get("account"),
                    doc.get("user_name"),
                    doc.get("technician"),
                    doc.get("updated_at"),
                    doc["text"],
                    _json(doc),
                    doc.get("content_hash"),
                    synced_at,
                ),
            )
        conn.commit()
    return len(docs)


def replace_ticket_document_chunks(db_path: Path, chunks: list[dict[str, Any]], synced_at: str | None = None) -> int:
    synced_at = synced_at or now_iso()
    with connect(db_path) as conn:
        if chunks:
            ticket_ids = sorted({str(chunk["ticket_id"]) for chunk in chunks})
            placeholders = ",".join("?" for _ in ticket_ids)
            conn.execute(f"DELETE FROM ticket_document_chunks WHERE ticket_id IN ({placeholders})", ticket_ids)
        for chunk in chunks:
            conn.execute(
                """
                INSERT INTO ticket_document_chunks(chunk_id, doc_id, ticket_id, chunk_index, text, content_hash, synced_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    chunk["chunk_id"],
                    chunk["doc_id"],
                    str(chunk["ticket_id"]),
                    int(chunk["chunk_index"]),
                    chunk["text"],
                    chunk.get("content_hash"),
                    synced_at,
                ),
            )
        conn.commit()
    return len(chunks)


def record_api_request_event(
    db_path: Path,
    *,
    method: str,
    path: str,
    status_code: int | None,
    outcome: str,
    attempt_kind: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    initialize_db(db_path)

    def _operation() -> None:
        with connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO api_request_events(recorded_at, method, path, status_code, outcome, attempt_kind, raw_json)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_iso(),
                    method,
                    path,
                    status_code,
                    outcome,
                    attempt_kind,
                    _json(extra or {}),
                ),
            )
            conn.commit()

    run_with_db_lock_retries(_operation)


def prune_api_request_events(db_path: Path, retention_days: int) -> int:
    initialize_db(db_path)

    def _operation() -> int:
        with connect(db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM api_request_events WHERE julianday(recorded_at) < julianday('now', ?)",
                (f'-{retention_days} days',),
            )
            conn.commit()
            return int(cursor.rowcount)

    return run_with_db_lock_retries(_operation)


def record_ticket_detail_failure(
    db_path: Path,
    *,
    ticket_id: str,
    status_code: int | None,
    error_kind: str,
    error_message: str,
    last_path: str,
    last_failure_at: str | None = None,
    next_retry_at: str | None = None,
    permanent_failure: bool = False,
    extra: dict[str, Any] | None = None,
) -> None:
    initialize_db(db_path)
    failure_at = last_failure_at or now_iso()

    def _operation() -> None:
        with connect(db_path) as conn:
            existing = conn.execute(
                "SELECT failure_count FROM ticket_detail_failures WHERE ticket_id = ?",
                (str(ticket_id),),
            ).fetchone()
            next_count = int(existing["failure_count"] or 0) + 1 if existing else 1
            payload = {
                "status_code": status_code,
                "error_kind": error_kind,
                "error_message": error_message,
                "last_path": last_path,
                "last_failure_at": failure_at,
                "next_retry_at": next_retry_at,
                "failure_count": next_count,
                "permanent_failure": bool(permanent_failure),
            }
            if extra:
                payload["extra"] = extra
            conn.execute(
                """
                INSERT INTO ticket_detail_failures(
                    ticket_id, status_code, error_kind, error_message, last_path,
                    last_failure_at, next_retry_at, failure_count, permanent_failure, raw_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticket_id) DO UPDATE SET
                    status_code = excluded.status_code,
                    error_kind = excluded.error_kind,
                    error_message = excluded.error_message,
                    last_path = excluded.last_path,
                    last_failure_at = excluded.last_failure_at,
                    next_retry_at = excluded.next_retry_at,
                    failure_count = excluded.failure_count,
                    permanent_failure = excluded.permanent_failure,
                    raw_json = excluded.raw_json
                """,
                (
                    str(ticket_id),
                    status_code,
                    error_kind,
                    error_message,
                    last_path,
                    failure_at,
                    next_retry_at,
                    next_count,
                    1 if permanent_failure else 0,
                    _json(payload),
                ),
            )
            conn.commit()

    run_with_db_lock_retries(_operation)


def clear_ticket_detail_failure(db_path: Path, ticket_id: str) -> None:
    initialize_db(db_path)

    def _operation() -> None:
        with connect(db_path) as conn:
            conn.execute("DELETE FROM ticket_detail_failures WHERE ticket_id = ?", (str(ticket_id),))
            conn.commit()

    run_with_db_lock_retries(_operation)


def start_worker_run(db_path: Path, worker_name: str, mode: str, notes: str | None = None) -> int:
    initialize_db(db_path)
    started_at = now_iso()

    def _operation() -> int:
        with connect(db_path) as conn:
            cursor = conn.execute(
                "INSERT INTO worker_runs(worker_name, mode, started_at, status, notes) VALUES(?, ?, ?, ?, ?)",
                (worker_name, mode, started_at, "running", notes),
            )
            conn.commit()
            return int(cursor.lastrowid)

    return run_with_db_lock_retries(_operation)


def finish_worker_run(db_path: Path, run_id: int, status: str, notes: str | None = None) -> None:
    initialize_db(db_path)

    def _operation() -> None:
        with connect(db_path) as conn:
            conn.execute(
                "UPDATE worker_runs SET finished_at = ?, status = ?, notes = ? WHERE id = ?",
                (now_iso(), status, notes, run_id),
            )
            conn.commit()

    run_with_db_lock_retries(_operation)


def cleanup_stale_worker_runs(db_path: Path, *, stale_after_seconds: int = DB_STALE_RUNNING_AFTER_SECONDS) -> int:
    initialize_db(db_path)
    stale_before = (datetime.now(timezone.utc) - timedelta(seconds=stale_after_seconds)).isoformat()
    finished_at = now_iso()

    def _operation() -> int:
        with connect(db_path) as conn:
            rows = conn.execute(
                "SELECT id, notes FROM worker_runs WHERE status = 'running' AND started_at < ?",
                (stale_before,),
            ).fetchall()
            cleaned = 0
            for row in rows:
                existing_notes = row['notes'] or ''
                suffix = f" [auto-cleanup {finished_at}: stale running worker row]"
                next_notes = f"{existing_notes}{suffix}" if suffix not in existing_notes else existing_notes
                conn.execute(
                    "UPDATE worker_runs SET finished_at = ?, status = ?, notes = ? WHERE id = ?",
                    (finished_at, 'abandoned', next_notes, row['id']),
                )
                cleaned += 1
            conn.commit()
            return cleaned

    return run_with_db_lock_retries(_operation)


def get_worker_lease(db_path: Path, worker_name: str) -> dict[str, Any] | None:
    initialize_db(db_path)
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT worker_name, owner_id, leased_at, lease_expires_at, notes FROM worker_leases WHERE worker_name = ?",
            (worker_name,),
        ).fetchone()
    return dict(row) if row is not None else None


def _lease_owner_pid(owner_id: str | None) -> int | None:
    if not owner_id:
        return None
    parts = str(owner_id).split(":")
    if len(parts) < 2:
        return None
    try:
        return int(parts[1])
    except (TypeError, ValueError):
        return None


def _pid_is_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def try_acquire_worker_lease(db_path: Path, worker_name: str, owner_id: str, *, lease_seconds: int = 900, notes: str | None = None) -> bool:
    initialize_db(db_path)
    leased_at = now_iso()
    lease_expires_at = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat()

    def _operation() -> bool:
        with connect(db_path) as conn:
            row = conn.execute(
                "SELECT owner_id, lease_expires_at FROM worker_leases WHERE worker_name = ?",
                (worker_name,),
            ).fetchone()
            if row is not None:
                expiry = parse_sherpadesk_timestamp(row['lease_expires_at']) if row['lease_expires_at'] else None
                owner_pid = _lease_owner_pid(row['owner_id'])
                owner_alive = _pid_is_alive(owner_pid)
                if expiry and expiry > datetime.now(timezone.utc) and row['owner_id'] != owner_id and owner_alive:
                    return False
            conn.execute(
                "INSERT INTO worker_leases(worker_name, owner_id, leased_at, lease_expires_at, notes) VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(worker_name) DO UPDATE SET owner_id=excluded.owner_id, leased_at=excluded.leased_at, lease_expires_at=excluded.lease_expires_at, notes=excluded.notes",
                (worker_name, owner_id, leased_at, lease_expires_at, notes),
            )
            conn.commit()
            return True

    return run_with_db_lock_retries(_operation)


def renew_worker_lease(db_path: Path, worker_name: str, owner_id: str, *, lease_seconds: int = 900, notes: str | None = None) -> bool:
    initialize_db(db_path)
    leased_at = now_iso()
    lease_expires_at = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat()

    def _operation() -> bool:
        with connect(db_path) as conn:
            cursor = conn.execute(
                "UPDATE worker_leases SET leased_at = ?, lease_expires_at = ?, notes = ? WHERE worker_name = ? AND owner_id = ?",
                (leased_at, lease_expires_at, notes, worker_name, owner_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    return run_with_db_lock_retries(_operation)


def release_worker_lease(db_path: Path, worker_name: str, owner_id: str) -> None:
    initialize_db(db_path)

    def _operation() -> None:
        with connect(db_path) as conn:
            conn.execute(
                "DELETE FROM worker_leases WHERE worker_name = ? AND owner_id = ?",
                (worker_name, owner_id),
            )
            conn.commit()

    run_with_db_lock_retries(_operation)


def get_ingest_mode_lease(db_path: Path, mode: str) -> dict[str, Any] | None:
    initialize_db(db_path)
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT mode, owner_id, leased_at, lease_expires_at, notes FROM ingest_mode_leases WHERE mode = ?",
            (mode,),
        ).fetchone()
    return dict(row) if row is not None else None


def try_acquire_ingest_mode_lease(db_path: Path, mode: str, owner_id: str, *, lease_seconds: int = 1800, notes: str | None = None) -> bool:
    initialize_db(db_path)
    leased_at = now_iso()
    lease_expires_at = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat()

    def _operation() -> bool:
        with connect(db_path) as conn:
            row = conn.execute(
                "SELECT owner_id, lease_expires_at FROM ingest_mode_leases WHERE mode = ?",
                (mode,),
            ).fetchone()
            if row is not None:
                expiry = parse_sherpadesk_timestamp(row['lease_expires_at']) if row['lease_expires_at'] else None
                owner_pid = _lease_owner_pid(row['owner_id'])
                owner_alive = _pid_is_alive(owner_pid)
                if expiry and expiry > datetime.now(timezone.utc) and row['owner_id'] != owner_id and owner_alive:
                    return False
            conn.execute(
                "INSERT INTO ingest_mode_leases(mode, owner_id, leased_at, lease_expires_at, notes) VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(mode) DO UPDATE SET owner_id=excluded.owner_id, leased_at=excluded.leased_at, lease_expires_at=excluded.lease_expires_at, notes=excluded.notes",
                (mode, owner_id, leased_at, lease_expires_at, notes),
            )
            conn.commit()
            return True

    return run_with_db_lock_retries(_operation)


def renew_ingest_mode_lease(db_path: Path, mode: str, owner_id: str, *, lease_seconds: int = 1800, notes: str | None = None) -> bool:
    initialize_db(db_path)
    leased_at = now_iso()
    lease_expires_at = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat()

    def _operation() -> bool:
        with connect(db_path) as conn:
            cursor = conn.execute(
                "UPDATE ingest_mode_leases SET leased_at = ?, lease_expires_at = ?, notes = ? WHERE mode = ? AND owner_id = ?",
                (leased_at, lease_expires_at, notes, mode, owner_id),
            )
            conn.commit()
            return cursor.rowcount > 0

    return run_with_db_lock_retries(_operation)


def release_ingest_mode_lease(db_path: Path, mode: str, owner_id: str) -> None:
    initialize_db(db_path)

    def _operation() -> None:
        with connect(db_path) as conn:
            conn.execute(
                "DELETE FROM ingest_mode_leases WHERE mode = ? AND owner_id = ?",
                (mode, owner_id),
            )
            conn.commit()

    run_with_db_lock_retries(_operation)



def enqueue_ticket_classification_event(
    db_path: Path,
    *,
    ticket_id: str,
    event_type: str,
    dedupe_key: str,
    trigger_source: str,
    payload: dict[str, Any],
    ticket_status: str | None = None,
    ticket_updated_time: str | None = None,
    current_class_id: str | None = None,
    current_class_name: str | None = None,
) -> dict[str, Any]:
    now = now_iso()
    with connect(db_path) as conn:
        existing = conn.execute("SELECT id, status FROM ticket_classification_events WHERE dedupe_key = ?", (dedupe_key,)).fetchone()
        if existing:
            return {"status": "exists", "id": existing["id"], "event_status": existing["status"], "dedupe_key": dedupe_key}
        cursor = conn.execute(
            """
            INSERT INTO ticket_classification_events(
                ticket_id, event_type, dedupe_key, status, trigger_source, ticket_status, ticket_updated_time,
                current_class_id, current_class_name, payload_json, created_at, updated_at
            ) VALUES(?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(ticket_id),
                event_type,
                dedupe_key,
                trigger_source,
                ticket_status,
                ticket_updated_time,
                current_class_id,
                current_class_name,
                _json(payload),
                now,
                now,
            ),
        )
        conn.commit()
        return {"status": "enqueued", "id": int(cursor.lastrowid), "dedupe_key": dedupe_key}


def lease_ticket_classification_events(db_path: Path, *, limit: int = 3) -> list[dict[str, Any]]:
    now = now_iso()
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT * FROM ticket_classification_events
            WHERE status IN ('pending', 'failed') AND attempt_count < 3
            ORDER BY id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        ids = [row["id"] for row in rows]
        if ids:
            conn.executemany(
                "UPDATE ticket_classification_events SET status = 'dispatching', attempt_count = attempt_count + 1, updated_at = ? WHERE id = ?",
                [(now, event_id) for event_id in ids],
            )
            conn.commit()
        if not ids:
            return []
        refreshed = conn.execute(
            f"SELECT * FROM ticket_classification_events WHERE id IN ({','.join('?' for _ in ids)}) ORDER BY id ASC",
            ids,
        ).fetchall()
    return [dict(row) for row in refreshed]


def mark_ticket_classification_dispatched(db_path: Path, event_id: int, *, prompt: dict[str, Any]) -> None:
    now = now_iso()
    with connect(db_path) as conn:
        conn.execute(
            "UPDATE ticket_classification_events SET status = 'awaiting_result', prompt_json = ?, dispatched_at = ?, updated_at = ?, last_error = NULL WHERE id = ?",
            (_json(prompt), now, now, event_id),
        )
        conn.commit()


def mark_ticket_classification_failed(db_path: Path, event_id: int, error_message: str) -> None:
    now = now_iso()
    with connect(db_path) as conn:
        conn.execute(
            "UPDATE ticket_classification_events SET status = 'failed', last_error = ?, updated_at = ? WHERE id = ?",
            (error_message[:1000], now, event_id),
        )
        conn.commit()


def record_ticket_classification_result(
    db_path: Path,
    *,
    event_id: int,
    class_id: str,
    confidence: str,
    rationale: str,
) -> dict[str, Any]:
    now = now_iso()
    with connect(db_path) as conn:
        taxonomy = conn.execute("SELECT id, path, is_active, is_lastchild FROM ticket_taxonomy_classes WHERE id = ?", (str(class_id),)).fetchone()
        if taxonomy is None:
            raise ValueError(f"Unknown ticket class id: {class_id}")
        if int(taxonomy["is_active"] if taxonomy["is_active"] is not None else 1) != 1:
            raise ValueError(f"Inactive ticket class id: {class_id}")
        if int(taxonomy["is_lastchild"] if taxonomy["is_lastchild"] is not None else 0) != 1:
            raise ValueError(f"Ticket class id is not a leaf/sub-class: {class_id}")
        event = conn.execute("SELECT id FROM ticket_classification_events WHERE id = ?", (event_id,)).fetchone()
        if event is None:
            raise ValueError(f"Unknown classification event id: {event_id}")
        conn.execute(
            """
            UPDATE ticket_classification_events
            SET status = 'completed', result_class_id = ?, result_class_path = ?, confidence = ?, rationale = ?, completed_at = ?, updated_at = ?, last_error = NULL
            WHERE id = ?
            """,
            (str(class_id), taxonomy["path"], confidence, rationale[:1000], now, now, event_id),
        )
        conn.commit()
        return {"status": "ok", "event_id": event_id, "class_id": str(class_id), "class_path": taxonomy["path"], "confidence": confidence}


def get_ticket_classification_summary(db_path: Path) -> dict[str, Any]:
    with connect(db_path) as conn:
        by_status = [dict(row) for row in conn.execute("SELECT status, COUNT(*) AS count FROM ticket_classification_events GROUP BY status ORDER BY status").fetchall()]
        by_type = [dict(row) for row in conn.execute("SELECT event_type, status, COUNT(*) AS count FROM ticket_classification_events GROUP BY event_type, status ORDER BY event_type, status").fetchall()]
        recent = [dict(row) for row in conn.execute(
            """
            SELECT id, ticket_id, event_type, status, result_class_id, result_class_path, confidence, updated_at, last_error, writeback_status, writeback_last_error
            FROM ticket_classification_events
            ORDER BY id DESC
            LIMIT 10
            """
        ).fetchall()]
    return {"status": "ok", "by_status": by_status, "by_type": by_type, "recent": recent}


def enqueue_alert(
    db_path: Path,
    *,
    alert_type: str,
    ticket_id: str,
    dedupe_key: str,
    payload: dict[str, Any] | None = None,
    priority: int = 100,
    available_at: str | None = None,
) -> dict[str, Any]:
    initialize_db(db_path)
    created_at = now_iso()
    available_at = available_at or created_at

    def _operation() -> dict[str, Any]:
        with connect(db_path) as conn:
            existing = conn.execute(
                "SELECT id, status, attempt_count, sent_at FROM alert_queue WHERE dedupe_key = ?",
                (dedupe_key,),
            ).fetchone()
            if existing is not None:
                return {"status": "duplicate", "alert_id": int(existing['id']), "existing_status": existing['status'], "attempt_count": int(existing['attempt_count'] or 0), "sent_at": existing['sent_at']}
            cursor = conn.execute(
                "INSERT INTO alert_queue(alert_type, ticket_id, dedupe_key, payload_json, status, priority, available_at, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (alert_type, str(ticket_id), dedupe_key, _json(payload or {}), 'pending', priority, available_at, created_at, created_at),
            )
            conn.commit()
            return {"status": "enqueued", "alert_id": int(cursor.lastrowid)}

    return run_with_db_lock_retries(_operation)


def lease_alert_batch(db_path: Path, *, batch_size: int = 10, lease_seconds: int = 300) -> list[dict[str, Any]]:
    initialize_db(db_path)
    leased_at = now_iso()
    lease_expires_at = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat()

    def _operation() -> list[dict[str, Any]]:
        with connect(db_path) as conn:
            conn.execute(
                "UPDATE alert_queue SET status = 'pending', leased_at = NULL, lease_expires_at = NULL, updated_at = ? WHERE status = 'leased' AND lease_expires_at < ?",
                (leased_at, leased_at),
            )
            rows = conn.execute(
                "SELECT id FROM alert_queue WHERE status IN ('pending','failed') AND available_at <= ? ORDER BY priority ASC, id ASC LIMIT ?",
                (leased_at, batch_size),
            ).fetchall()
            ids = [int(row['id']) for row in rows]
            if not ids:
                conn.commit()
                return []
            conn.executemany(
                "UPDATE alert_queue SET status = 'leased', leased_at = ?, lease_expires_at = ?, updated_at = ?, attempt_count = attempt_count + 1 WHERE id = ?",
                [(leased_at, lease_expires_at, leased_at, alert_id) for alert_id in ids],
            )
            leased = conn.execute(
                f"SELECT * FROM alert_queue WHERE id IN ({','.join('?' for _ in ids)}) ORDER BY priority ASC, id ASC",
                ids,
            ).fetchall()
            conn.commit()
            return [dict(row) for row in leased]

    return run_with_db_lock_retries(_operation)


def mark_alert_sent(db_path: Path, alert_id: int) -> None:
    initialize_db(db_path)
    sent_at = now_iso()

    def _operation() -> None:
        with connect(db_path) as conn:
            conn.execute(
                "UPDATE alert_queue SET status = 'sent', sent_at = ?, updated_at = ?, lease_expires_at = NULL, leased_at = NULL WHERE id = ?",
                (sent_at, sent_at, alert_id),
            )
            conn.commit()

    run_with_db_lock_retries(_operation)


def mark_alert_failed(db_path: Path, alert_id: int, error_message: str, *, retry_after_seconds: int = 120, dead_after_attempts: int = 8) -> None:
    initialize_db(db_path)
    updated_at = now_iso()
    available_at = (datetime.now(timezone.utc) + timedelta(seconds=retry_after_seconds)).isoformat()

    def _operation() -> None:
        with connect(db_path) as conn:
            row = conn.execute("SELECT attempt_count FROM alert_queue WHERE id = ?", (alert_id,)).fetchone()
            attempts = int((row['attempt_count'] if row else 0) or 0)
            next_status = 'dead' if attempts >= dead_after_attempts else 'failed'
            conn.execute(
                "UPDATE alert_queue SET status = ?, available_at = ?, last_error = ?, updated_at = ?, lease_expires_at = NULL, leased_at = NULL WHERE id = ?",
                (next_status, available_at, error_message, updated_at, alert_id),
            )
            conn.commit()

    run_with_db_lock_retries(_operation)


def get_alert_queue_summary(db_path: Path) -> dict[str, Any]:
    initialize_db(db_path)
    with connect(db_path) as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS count FROM alert_queue GROUP BY status"
        ).fetchall()
        oldest_pending = conn.execute(
            "SELECT created_at FROM alert_queue WHERE status IN ('pending','failed') ORDER BY created_at ASC LIMIT 1"
        ).fetchone()
    summary = {row['status']: int(row['count'] or 0) for row in rows}
    summary['oldest_pending_created_at'] = oldest_pending['created_at'] if oldest_pending else None
    return summary


def enqueue_derived_refresh(
    db_path: Path,
    *,
    ticket_id: str,
    source: str,
    priority: int = 100,
) -> dict[str, Any]:
    initialize_db(db_path)
    requested_at = now_iso()

    def _operation() -> dict[str, Any]:
        with connect(db_path) as conn:
            existing = conn.execute(
                "SELECT ticket_id, priority, attempt_count, lease_expires_at FROM derived_refresh_queue WHERE ticket_id = ?",
                (str(ticket_id),),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO derived_refresh_queue(ticket_id, source, priority, requested_at, leased_at, lease_expires_at, completed_at, attempt_count, last_error, updated_at)
                VALUES(?, ?, ?, ?, NULL, NULL, NULL, 0, NULL, ?)
                ON CONFLICT(ticket_id) DO UPDATE SET
                    source = excluded.source,
                    priority = MIN(derived_refresh_queue.priority, excluded.priority),
                    requested_at = excluded.requested_at,
                    leased_at = NULL,
                    lease_expires_at = NULL,
                    completed_at = NULL,
                    last_error = NULL,
                    updated_at = excluded.updated_at
                """,
                (str(ticket_id), source, int(priority), requested_at, requested_at),
            )
            conn.commit()
            return {
                "status": "updated" if existing is not None else "enqueued",
                "ticket_id": str(ticket_id),
                "priority": min(int(existing['priority']), int(priority)) if existing is not None and existing['priority'] is not None else int(priority),
            }

    return run_with_db_lock_retries(_operation)


def lease_derived_refresh_batch(db_path: Path, *, batch_size: int = 25, lease_seconds: int = 300) -> list[dict[str, Any]]:
    initialize_db(db_path)
    leased_at = now_iso()
    lease_expires_at = (datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)).isoformat()

    def _operation() -> list[dict[str, Any]]:
        with connect(db_path) as conn:
            conn.execute(
                "UPDATE derived_refresh_queue SET leased_at = NULL, lease_expires_at = NULL, updated_at = ? WHERE lease_expires_at IS NOT NULL AND lease_expires_at < ?",
                (leased_at, leased_at),
            )
            rows = conn.execute(
                """
                SELECT ticket_id
                FROM derived_refresh_queue
                WHERE completed_at IS NULL AND (lease_expires_at IS NULL OR lease_expires_at < ?)
                ORDER BY priority ASC, requested_at ASC, ticket_id ASC
                LIMIT ?
                """,
                (leased_at, batch_size),
            ).fetchall()
            ticket_ids = [str(row['ticket_id']) for row in rows]
            if not ticket_ids:
                conn.commit()
                return []
            conn.executemany(
                "UPDATE derived_refresh_queue SET leased_at = ?, lease_expires_at = ?, attempt_count = attempt_count + 1, updated_at = ? WHERE ticket_id = ?",
                [(leased_at, lease_expires_at, leased_at, ticket_id) for ticket_id in ticket_ids],
            )
            leased = conn.execute(
                f"SELECT * FROM derived_refresh_queue WHERE ticket_id IN ({','.join('?' for _ in ticket_ids)}) ORDER BY priority ASC, requested_at ASC, ticket_id ASC",
                ticket_ids,
            ).fetchall()
            conn.commit()
            return [dict(row) for row in leased]

    return run_with_db_lock_retries(_operation)


def complete_derived_refresh_batch(db_path: Path, ticket_ids: list[str], *, error_message: str | None = None) -> None:
    initialize_db(db_path)
    if not ticket_ids:
        return
    completed_at = now_iso()

    def _operation() -> None:
        with connect(db_path) as conn:
            if error_message:
                conn.executemany(
                    "UPDATE derived_refresh_queue SET leased_at = NULL, lease_expires_at = NULL, last_error = ?, updated_at = ? WHERE ticket_id = ?",
                    [(error_message, completed_at, str(ticket_id)) for ticket_id in ticket_ids],
                )
            else:
                conn.executemany(
                    "DELETE FROM derived_refresh_queue WHERE ticket_id = ?",
                    [(str(ticket_id),) for ticket_id in ticket_ids],
                )
            conn.commit()

    run_with_db_lock_retries(_operation)


def get_derived_refresh_summary(db_path: Path) -> dict[str, Any]:
    initialize_db(db_path)
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS pending_count, MIN(requested_at) AS oldest_requested_at, MIN(priority) AS highest_priority FROM derived_refresh_queue WHERE completed_at IS NULL"
        ).fetchone()
    return {
        "pending_count": int(row['pending_count'] or 0),
        "oldest_requested_at": row['oldest_requested_at'],
        "highest_priority": row['highest_priority'],
    }


def get_ticket_alert_state(db_path: Path, ticket_id: str) -> dict[str, Any] | None:
    initialize_db(db_path)
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM ticket_alert_state WHERE ticket_id = ?", (str(ticket_id),)).fetchone()
    return dict(row) if row is not None else None


def upsert_ticket_alert_state(db_path: Path, ticket_id: str, **fields: Any) -> dict[str, Any]:
    initialize_db(db_path)
    ticket_id = str(ticket_id)
    updated_at = now_iso()

    def _operation() -> dict[str, Any]:
        with connect(db_path) as conn:
            current = conn.execute("SELECT * FROM ticket_alert_state WHERE ticket_id = ?", (ticket_id,)).fetchone()
            values = dict(current) if current is not None else {
                'ticket_id': ticket_id,
                'is_currently_monitored_open': 0,
                'open_cycle_id': 0,
                'open_alert_sent_at': None,
                'last_seen_open_at': None,
                'missing_open_polls': 0,
                'close_confirmed_at': None,
                'last_non_tech_event_key': None,
                'last_non_tech_alerted_key': None,
                'last_status': None,
                'last_updated_time': None,
                'updated_at': updated_at,
            }
            values.update(fields)
            values['ticket_id'] = ticket_id
            values['updated_at'] = updated_at
            conn.execute(
                """
                INSERT INTO ticket_alert_state(
                    ticket_id, is_currently_monitored_open, open_cycle_id, open_alert_sent_at,
                    last_seen_open_at, missing_open_polls, close_confirmed_at,
                    last_non_tech_event_key, last_non_tech_alerted_key, last_status,
                    last_updated_time, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(ticket_id) DO UPDATE SET
                    is_currently_monitored_open=excluded.is_currently_monitored_open,
                    open_cycle_id=excluded.open_cycle_id,
                    open_alert_sent_at=excluded.open_alert_sent_at,
                    last_seen_open_at=excluded.last_seen_open_at,
                    missing_open_polls=excluded.missing_open_polls,
                    close_confirmed_at=excluded.close_confirmed_at,
                    last_non_tech_event_key=excluded.last_non_tech_event_key,
                    last_non_tech_alerted_key=excluded.last_non_tech_alerted_key,
                    last_status=excluded.last_status,
                    last_updated_time=excluded.last_updated_time,
                    updated_at=excluded.updated_at
                """,
                (
                    values['ticket_id'],
                    int(bool(values['is_currently_monitored_open'])),
                    int(values['open_cycle_id'] or 0),
                    values['open_alert_sent_at'],
                    values['last_seen_open_at'],
                    int(values['missing_open_polls'] or 0),
                    values['close_confirmed_at'],
                    values['last_non_tech_event_key'],
                    values['last_non_tech_alerted_key'],
                    values['last_status'],
                    values['last_updated_time'],
                    values['updated_at'],
                ),
            )
            conn.commit()
            row = conn.execute("SELECT * FROM ticket_alert_state WHERE ticket_id = ?", (ticket_id,)).fetchone()
            return dict(row)

    return run_with_db_lock_retries(_operation)


def mark_ticket_open_seen(db_path: Path, ticket: dict[str, Any]) -> dict[str, Any]:
    ticket_id = str(ticket.get('id'))
    current = get_ticket_alert_state(db_path, ticket_id)
    is_monitored = bool((current or {}).get('is_currently_monitored_open'))
    open_cycle_id = int((current or {}).get('open_cycle_id') or 0)
    if not is_monitored:
        open_cycle_id += 1
    return upsert_ticket_alert_state(
        db_path,
        ticket_id,
        is_currently_monitored_open=1,
        open_cycle_id=open_cycle_id,
        last_seen_open_at=now_iso(),
        missing_open_polls=0,
        close_confirmed_at=None,
        last_status=ticket.get('status'),
        last_updated_time=ticket.get('updated_time'),
    )


def mark_ticket_open_missing(db_path: Path, ticket_id: str) -> dict[str, Any] | None:
    current = get_ticket_alert_state(db_path, str(ticket_id))
    if current is None:
        return None
    return upsert_ticket_alert_state(
        db_path,
        str(ticket_id),
        missing_open_polls=int(current.get('missing_open_polls') or 0) + 1,
    )


def mark_ticket_closed_confirmed(db_path: Path, ticket_id: str, *, status: str | None = None, updated_time: str | None = None) -> dict[str, Any]:
    current = get_ticket_alert_state(db_path, str(ticket_id)) or {}
    return upsert_ticket_alert_state(
        db_path,
        str(ticket_id),
        is_currently_monitored_open=0,
        missing_open_polls=0,
        close_confirmed_at=now_iso(),
        last_status=status or current.get('last_status'),
        last_updated_time=updated_time or current.get('last_updated_time'),
    )


def mark_new_ticket_alert_sent(db_path: Path, ticket_id: str) -> dict[str, Any]:
    current = get_ticket_alert_state(db_path, str(ticket_id)) or {}
    return upsert_ticket_alert_state(
        db_path,
        str(ticket_id),
        open_alert_sent_at=now_iso(),
        is_currently_monitored_open=1,
        open_cycle_id=int(current.get('open_cycle_id') or 1),
    )


def mark_ticket_update_alert_sent(db_path: Path, ticket_id: str, event_key: str) -> dict[str, Any]:
    return upsert_ticket_alert_state(
        db_path,
        str(ticket_id),
        last_non_tech_alerted_key=event_key,
        last_non_tech_event_key=event_key,
    )
