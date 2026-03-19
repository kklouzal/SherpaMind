from __future__ import annotations

import sqlite3
from pathlib import Path

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


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        conn.commit()
