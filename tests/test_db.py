import os
from pathlib import Path

from sherpamind.db import (
    backfill_ticket_core_fields,
    backfill_ticket_entity_stubs,
    backfill_ticket_technician_stubs,
    cleanup_stale_ingest_runs,
    connect,
    finish_ingest_run,
    get_ticket_alert_state,
    get_ingest_mode_lease,
    initialize_db,
    mark_new_ticket_alert_sent,
    mark_ticket_closed_confirmed,
    mark_ticket_open_seen,
    mark_ticket_update_alert_sent,
    replace_ticket_document_chunks,
    replace_ticket_documents,
    replace_ticket_taxonomy_classes,
    start_ingest_run,
    try_acquire_ingest_mode_lease,
    upsert_accounts,
    upsert_ticket_details,
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
    assert 'ticket_details' in names
    assert 'ticket_logs' in names
    assert 'ticket_attachments' in names
    assert 'ticket_documents' in names
    assert 'ticket_document_chunks' in names
    assert 'api_request_events' in names
    assert 'ticket_detail_failures' in names
    assert 'ingest_mode_leases' in names
    assert 'ticket_taxonomy_classes' in names


def test_replace_ticket_taxonomy_classes_roundtrip(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    replace_ticket_taxonomy_classes(
        db,
        [{
            "id": "11",
            "parent_id": "1",
            "name": "Printer",
            "path": "Hardware / Printer",
            "hierarchy_level": 1,
            "is_lastchild": True,
            "is_active": True,
            "raw_json": {"id": 11, "name": "Printer"},
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    with connect(db) as conn:
        row = conn.execute("SELECT id, parent_id, name, path, is_lastchild, is_active, synced_at FROM ticket_taxonomy_classes").fetchone()
    assert row["id"] == "11"
    assert row["parent_id"] == "1"
    assert row["path"] == "Hardware / Printer"
    assert row["is_lastchild"] == 1
    assert row["is_active"] == 1
    assert row["synced_at"] == "2026-03-19T01:00:00Z"


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
            "status": "Open",
            "priority_name": "  High  ",
            "creation_category_name": "Hardware  /\n Printer",
            "created_time": "2026-03-18T01:00:00Z",
            "updated_time": "2026-03-19T01:00:00Z",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    upsert_ticket_details(
        db,
        [{
            "id": 4,
            "workpad": "secret work",
            "ticketlogs": [{"id": 99, "log_type": "Initial Post", "record_date": "2026-03-18T01:00:00Z", "plain_note": "printer broken"}],
            "timelogs": [],
            "attachments": [{"id": "a1", "name": "shot.png", "url": "https://example/shot.png", "size": 1234, "date": "2026-03-18T01:00:00Z"}],
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    replace_ticket_documents(
        db,
        [{"doc_id": "ticket:4", "ticket_id": 4, "status": "Open", "account": "Acme", "user_name": "User One", "technician": "Tech One", "updated_at": "2026-03-19T01:00:00Z", "text": "hello", "metadata": {}, "content_hash": "h1"}],
        synced_at="2026-03-19T01:00:00Z",
    )
    replace_ticket_document_chunks(
        db,
        [{"chunk_id": "ticket:4:chunk:0", "doc_id": "ticket:4", "ticket_id": 4, "chunk_index": 0, "text": "hello", "content_hash": "h1"}],
        synced_at="2026-03-19T01:00:00Z",
    )
    with connect(db) as conn:
        ticket = conn.execute("SELECT subject, priority, category FROM tickets WHERE id = '4'").fetchone()
        user = conn.execute("SELECT display_name FROM users WHERE id = '2'").fetchone()
        tech = conn.execute("SELECT display_name FROM technicians WHERE id = '3'").fetchone()
        detail = conn.execute("SELECT workpad, ticketlogs_count FROM ticket_details WHERE ticket_id = '4'").fetchone()
        attachment = conn.execute("SELECT name, size FROM ticket_attachments WHERE ticket_id = '4'").fetchone()
        doc = conn.execute("SELECT text FROM ticket_documents WHERE doc_id = 'ticket:4'").fetchone()
        chunk = conn.execute("SELECT text FROM ticket_document_chunks WHERE chunk_id = 'ticket:4:chunk:0'").fetchone()
    assert ticket["subject"] == "Printer is haunted"
    assert ticket["priority"] == "High"
    assert ticket["category"] == "Hardware / Printer"
    assert user["display_name"] == "User One"
    assert tech["display_name"] == "Tech One"
    assert detail["workpad"] == "secret work"
    assert detail["ticketlogs_count"] == 1
    assert attachment["name"] == "shot.png"
    assert attachment["size"] == 1234
    assert doc["text"] == "hello"
    assert chunk["text"] == "hello"


def test_upsert_tickets_backfills_entity_stub_rows(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    upsert_tickets(
        db,
        [{
            "id": 41,
            "account_id": 1,
            "account_name": "Acme Field Ops",
            "user_id": 2,
            "user_firstname": "Pat",
            "user_lastname": "Operator",
            "user_email": "pat@example.com",
            "tech_id": 333,
            "technician_firstname": "Queue",
            "technician_lastname": "Owner",
            "subject": "Printer is haunted",
            "status": "Open",
            "created_time": "2026-03-18T01:00:00Z",
            "updated_time": "2026-03-19T01:00:00Z",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    with connect(db) as conn:
        account = conn.execute("SELECT name, raw_json FROM accounts WHERE id = '1'").fetchone()
        user = conn.execute("SELECT account_id, display_name, email, raw_json FROM users WHERE id = '2'").fetchone()
        tech = conn.execute("SELECT display_name, email, raw_json FROM technicians WHERE id = '333'").fetchone()
    assert account["name"] == "Acme Field Ops"
    assert 'ticket_stub' in account["raw_json"]
    assert user["account_id"] == '1'
    assert user["display_name"] == "Pat Operator"
    assert user["email"] == "pat@example.com"
    assert 'ticket_stub' in user["raw_json"]
    assert tech["display_name"] == "Queue Owner"
    assert tech["email"] is None
    assert 'ticket_stub' in tech["raw_json"]


def test_upsert_seed_entities_beat_stub_rows(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    upsert_tickets(
        db,
        [{
            "id": 41,
            "account_id": 1,
            "account_name": "Acme Field Ops",
            "user_id": 2,
            "user_firstname": "Pat",
            "user_lastname": "Operator",
            "tech_id": 333,
            "technician_firstname": "Queue",
            "technician_lastname": "Owner",
            "status": "Open",
            "created_time": "2026-03-18T01:00:00Z",
            "updated_time": "2026-03-19T01:00:00Z",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    upsert_accounts(
        db,
        [{"id": 1, "name": "Acme", "updated": "2026-03-19T02:00:00Z"}],
        synced_at="2026-03-19T02:00:00Z",
    )
    upsert_users(
        db,
        [{"id": 2, "account_id": 1, "FullName": "Pat Operator", "email": "pat@example.com"}],
        synced_at="2026-03-19T02:00:00Z",
    )
    upsert_technicians(
        db,
        [{"id": 333, "FullName": "Queue Owner", "email": "queue@example.com", "type": "tech"}],
        synced_at="2026-03-19T02:00:00Z",
    )
    with connect(db) as conn:
        account = conn.execute("SELECT name, raw_json FROM accounts WHERE id = '1'").fetchone()
        user = conn.execute("SELECT display_name, email, raw_json FROM users WHERE id = '2'").fetchone()
        tech = conn.execute("SELECT display_name, email, raw_json FROM technicians WHERE id = '333'").fetchone()
    assert account["name"] == "Acme"
    assert 'ticket_stub' not in account["raw_json"]
    assert user["display_name"] == "Pat Operator"
    assert user["email"] == "pat@example.com"
    assert 'ticket_stub' not in user["raw_json"]
    assert tech["display_name"] == "Queue Owner"
    assert tech["email"] == "queue@example.com"
    assert 'ticket_stub' not in tech["raw_json"]


def test_backfill_ticket_entity_stubs_repairs_existing_ticket_rows(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    upsert_tickets(
        db,
        [{
            "id": 41,
            "account_id": 1,
            "account_name": "Acme Field Ops",
            "user_id": 2,
            "user_firstname": "Pat",
            "user_lastname": "Operator",
            "tech_id": 333,
            "technician_firstname": "Queue",
            "technician_lastname": "Owner",
            "status": "Open",
            "created_time": "2026-03-18T01:00:00Z",
            "updated_time": "2026-03-19T01:00:00Z",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    with connect(db) as conn:
        conn.execute("DELETE FROM accounts")
        conn.execute("DELETE FROM users")
        conn.execute("DELETE FROM technicians")
        conn.commit()
    result = backfill_ticket_entity_stubs(db, synced_at="2026-03-19T03:00:00Z")
    with connect(db) as conn:
        account = conn.execute("SELECT name FROM accounts WHERE id = '1'").fetchone()
        user = conn.execute("SELECT display_name FROM users WHERE id = '2'").fetchone()
        tech = conn.execute("SELECT display_name FROM technicians WHERE id = '333'").fetchone()
    assert result["account_rows_added"] == 1
    assert result["user_rows_added"] == 1
    assert result["technician_rows_added"] == 1
    assert account["name"] == "Acme Field Ops"
    assert user["display_name"] == "Pat Operator"
    assert tech["display_name"] == "Queue Owner"


def test_backfill_ticket_technician_stubs_reports_technician_slice(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    upsert_tickets(
        db,
        [{
            "id": 41,
            "tech_id": 333,
            "technician_firstname": "Queue",
            "technician_lastname": "Owner",
            "status": "Open",
            "created_time": "2026-03-18T01:00:00Z",
            "updated_time": "2026-03-19T01:00:00Z",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    with connect(db) as conn:
        conn.execute("DELETE FROM technicians")
        conn.commit()
    result = backfill_ticket_technician_stubs(db, synced_at="2026-03-19T03:00:00Z")
    with connect(db) as conn:
        tech = conn.execute("SELECT display_name FROM technicians WHERE id = '333'").fetchone()
    assert result["technician_rows_added"] == 1
    assert tech["display_name"] == "Queue Owner"


def test_backfill_ticket_core_fields_repairs_blank_structured_ticket_columns(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    upsert_tickets(
        db,
        [{
            "id": 41,
            "account_id": 1,
            "user_id": 2,
            "tech_id": 333,
            "subject": "  Printer queue stuck  ",
            "status": "Closed",
            "priority_name": "  High  ",
            "class_name": "Service Request/Move/Add/Change",
            "created_time": "2026-03-18T01:00:00Z",
            "updated_time": "2026-03-19T01:00:00Z",
            "closed_time": "2026-03-20T01:00:00Z",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    with connect(db) as conn:
        conn.execute(
            """
            UPDATE tickets
            SET account_id = NULL,
                user_id = NULL,
                assigned_technician_id = NULL,
                subject = NULL,
                status = NULL,
                priority = NULL,
                category = NULL,
                created_at = NULL,
                updated_at = NULL,
                closed_at = NULL,
                synced_at = '2026-03-19T01:00:00Z'
            WHERE id = '41'
            """
        )
        conn.commit()
    result = backfill_ticket_core_fields(db, synced_at="2026-03-19T03:00:00Z")
    with connect(db) as conn:
        ticket = conn.execute(
            "SELECT account_id, user_id, assigned_technician_id, subject, status, priority, category, created_at, updated_at, closed_at, synced_at FROM tickets WHERE id = '41'"
        ).fetchone()
    assert result["ticket_rows_repaired"] == 1
    assert result["field_repairs"] == {
        "account_id": 1,
        "user_id": 1,
        "assigned_technician_id": 1,
        "subject": 1,
        "status": 1,
        "priority": 1,
        "category": 1,
        "created_at": 1,
        "updated_at": 1,
        "closed_at": 1,
    }
    assert ticket["account_id"] == "1"
    assert ticket["user_id"] == "2"
    assert ticket["assigned_technician_id"] == "333"
    assert ticket["subject"] == "  Printer queue stuck  "
    assert ticket["status"] == "Closed"
    assert ticket["priority"] == "High"
    assert ticket["category"] == "Service Request / Move / Add / Change"
    assert ticket["created_at"] == "2026-03-18T01:00:00Z"
    assert ticket["updated_at"] == "2026-03-19T01:00:00Z"
    assert ticket["closed_at"] == "2026-03-20T01:00:00Z"
    assert ticket["synced_at"] == "2026-03-19T03:00:00Z"


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



def test_ingest_mode_lease_allows_dead_owner_takeover(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)

    acquired = try_acquire_ingest_mode_lease(
        db,
        "sync_hot_open",
        "ingest:999999:sync_hot_open:1",
        lease_seconds=1800,
        notes="stale dead owner",
    )
    assert acquired is True

    taken_over = try_acquire_ingest_mode_lease(
        db,
        "sync_hot_open",
        f"ingest:{os.getpid()}:sync_hot_open:2",
        lease_seconds=1800,
        notes="live replacement owner",
    )
    assert taken_over is True

    lease = get_ingest_mode_lease(db, "sync_hot_open")
    assert lease is not None
    assert lease["owner_id"] == f"ingest:{os.getpid()}:sync_hot_open:2"
    assert lease["notes"] == "live replacement owner"


def test_cleanup_stale_ingest_runs_marks_old_running_rows_abandoned(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    with connect(db) as conn:
        conn.execute(
            "INSERT INTO ingest_runs(mode, started_at, status, notes) VALUES(?, ?, ?, ?)",
            ("enrich_priority_ticket_details", "2026-03-01T00:00:00+00:00", "running", "limit=60"),
        )
        conn.commit()

    cleaned = cleanup_stale_ingest_runs(db, stale_after_seconds=1)
    assert cleaned == 1

    with connect(db) as conn:
        row = conn.execute("SELECT status, finished_at, notes FROM ingest_runs ORDER BY id DESC LIMIT 1").fetchone()
    assert row["status"] == "abandoned"
    assert row["finished_at"] is not None
    assert "auto-cleanup" in row["notes"]


def test_start_ingest_run_abandons_older_same_mode_running_rows(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    first_run = start_ingest_run(db, mode="enrich_priority_ticket_details", notes="limit=240")
    second_run = start_ingest_run(db, mode="enrich_priority_ticket_details", notes="limit=240")
    assert second_run > first_run

    with connect(db) as conn:
        rows = conn.execute(
            "SELECT id, status, finished_at, notes FROM ingest_runs WHERE mode = 'enrich_priority_ticket_details' ORDER BY id"
        ).fetchall()
    assert rows[0]["status"] == "abandoned"
    assert rows[0]["finished_at"] is not None
    assert "superseded by newer enrich_priority_ticket_details run" in rows[0]["notes"]
    assert rows[1]["status"] == "running"


def test_ticket_alert_state_tracks_open_cycle_and_alert_markers(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    state1 = mark_ticket_open_seen(db, {"id": 101, "status": "Open", "updated_time": "2026-04-01T10:00:00Z"})
    assert state1["is_currently_monitored_open"] == 1
    assert state1["open_cycle_id"] == 1
    state2 = mark_new_ticket_alert_sent(db, "101")
    assert state2["open_alert_sent_at"] is not None
    state3 = mark_ticket_update_alert_sent(db, "101", "evt-1")
    assert state3["last_non_tech_alerted_key"] == "evt-1"
    state4 = mark_ticket_closed_confirmed(db, "101", status="Closed", updated_time="2026-04-01T11:00:00Z")
    assert state4["is_currently_monitored_open"] == 0
    state5 = mark_ticket_open_seen(db, {"id": 101, "status": "Open", "updated_time": "2026-04-01T12:00:00Z"})
    assert state5["open_cycle_id"] == 2
    assert state5["is_currently_monitored_open"] == 1
    loaded = get_ticket_alert_state(db, "101")
    assert loaded is not None
    assert loaded["open_cycle_id"] == 2
