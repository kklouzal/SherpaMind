from pathlib import Path

from sherpamind.db import (
    connect,
    finish_ingest_run,
    initialize_db,
    start_ingest_run,
    upsert_accounts,
    upsert_tickets,
    upsert_technicians,
    upsert_users,
)


def test_initialize_db_creates_core_tables(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    with connect(db) as conn:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = {row['name'] for row in rows}
    assert 'tickets' in names
    assert 'accounts' in names
    assert 'users' in names
    assert 'ticket_comments' in names
    assert 'sync_state' in names


def test_upsert_seed_entities_roundtrip(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    upsert_accounts(db, [{"id": 1, "name": "Acme", "updated": "2026-03-19T00:00:00Z"}], synced_at="2026-03-19T01:00:00Z")
    upsert_users(db, [{"id": 2, "account_id": 1, "FullName": "User One", "email": "u@example.com"}], synced_at="2026-03-19T01:00:00Z")
    upsert_technicians(db, [{"id": 3, "FullName": "Tech One", "email": "t@example.com"}], synced_at="2026-03-19T01:00:00Z")
    upsert_tickets(
        db,
        [{
            "id": 4,
            "account_id": 1,
            "user_id": 2,
            "tech_id": 3,
            "subject": "Printer is haunted",
            "status": "open",
            "priority_name": "High",
            "creation_category_name": "Hardware",
            "created_time": "2026-03-18T01:00:00Z",
            "updated_time": "2026-03-19T01:00:00Z",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    with connect(db) as conn:
        ticket = conn.execute("SELECT subject, priority, category FROM tickets WHERE id = '4'").fetchone()
        user = conn.execute("SELECT display_name FROM users WHERE id = '2'").fetchone()
        tech = conn.execute("SELECT display_name FROM technicians WHERE id = '3'").fetchone()
    assert ticket["subject"] == "Printer is haunted"
    assert ticket["priority"] == "High"
    assert ticket["category"] == "Hardware"
    assert user["display_name"] == "User One"
    assert tech["display_name"] == "Tech One"


def test_ingest_run_roundtrip(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    run_id = start_ingest_run(db, mode="seed", notes="test")
    finish_ingest_run(db, run_id, status="success", notes="done")
    with connect(db) as conn:
        row = conn.execute("SELECT mode, status, notes, finished_at FROM ingest_runs WHERE id = ?", (run_id,)).fetchone()
    assert row["mode"] == "seed"
    assert row["status"] == "success"
    assert row["notes"] == "done"
    assert row["finished_at"] is not None
