from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
"""


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        conn.commit()


def start_ingest_run(db_path: Path, mode: str, notes: str | None = None) -> int:
    with connect(db_path) as conn:
        cursor = conn.execute(
            "INSERT INTO ingest_runs(mode, started_at, status, notes) VALUES(?, ?, ?, ?)",
            (mode, now_iso(), "running", notes),
        )
        conn.commit()
        return int(cursor.lastrowid)


def finish_ingest_run(db_path: Path, run_id: int, status: str, notes: str | None = None) -> None:
    with connect(db_path) as conn:
        conn.execute(
            "UPDATE ingest_runs SET finished_at = ?, status = ?, notes = ? WHERE id = ?",
            (now_iso(), status, notes, run_id),
        )
        conn.commit()


def _json(value: dict[str, Any]) -> str:
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
                    name = excluded.name,
                    raw_json = excluded.raw_json,
                    updated_at = excluded.updated_at,
                    synced_at = excluded.synced_at
                """,
                (
                    str(account["id"]),
                    account.get("name"),
                    _json(account),
                    account.get("updated"),
                    synced_at,
                ),
            )
        conn.commit()
    return len(accounts)


def _display_name(record: dict[str, Any]) -> str | None:
    return record.get("FullName") or record.get("full_name2") or " ".join(
        part for part in [record.get("firstname"), record.get("lastname")] if part
    ) or None


def upsert_users(db_path: Path, users: list[dict[str, Any]], synced_at: str | None = None) -> int:
    synced_at = synced_at or now_iso()
    with connect(db_path) as conn:
        for user in users:
            conn.execute(
                """
                INSERT INTO users(id, account_id, display_name, email, raw_json, updated_at, synced_at)
                VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    account_id = excluded.account_id,
                    display_name = excluded.display_name,
                    email = excluded.email,
                    raw_json = excluded.raw_json,
                    updated_at = excluded.updated_at,
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
            conn.execute(
                """
                INSERT INTO technicians(id, display_name, email, raw_json, updated_at, synced_at)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    display_name = excluded.display_name,
                    email = excluded.email,
                    raw_json = excluded.raw_json,
                    updated_at = excluded.updated_at,
                    synced_at = excluded.synced_at
                """,
                (
                    str(technician["id"]),
                    _display_name(technician),
                    technician.get("email"),
                    _json(technician),
                    technician.get("updated") or technician.get("modified") or technician.get("updated_time"),
                    synced_at,
                ),
            )
        conn.commit()
    return len(technicians)


def upsert_tickets(db_path: Path, tickets: list[dict[str, Any]], synced_at: str | None = None) -> int:
    synced_at = synced_at or now_iso()
    with connect(db_path) as conn:
        for ticket in tickets:
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
                    str(ticket["account_id"]) if ticket.get("account_id") is not None else None,
                    str(ticket["user_id"]) if ticket.get("user_id") is not None else None,
                    str(ticket["tech_id"]) if ticket.get("tech_id") is not None else None,
                    ticket.get("subject"),
                    ticket.get("status"),
                    ticket.get("priority_name") or ticket.get("priority"),
                    ticket.get("creation_category_name") or ticket.get("category"),
                    ticket.get("created_time"),
                    ticket.get("updated_time"),
                    ticket.get("closed_time"),
                    _json(ticket),
                    synced_at,
                ),
            )
        conn.commit()
    return len(tickets)
