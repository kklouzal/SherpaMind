from pathlib import Path

from sherpamind.db import initialize_db, upsert_accounts, upsert_ticket_details, upsert_tickets, upsert_technicians, upsert_users
from sherpamind.summaries import get_account_summary, get_technician_summary


def seed_fixture(db: Path) -> None:
    initialize_db(db)
    upsert_accounts(db, [{"id": 1, "name": "Acme"}], synced_at="2026-03-19T01:00:00Z")
    upsert_users(db, [{"id": 11, "account_id": 1, "FullName": "Alice User"}], synced_at="2026-03-19T01:00:00Z")
    upsert_technicians(db, [{"id": 21, "FullName": "Tech One", "email": "tech@example.com"}], synced_at="2026-03-19T01:00:00Z")
    upsert_tickets(db, [
        {"id": 101, "account_id": 1, "user_id": 11, "tech_id": 21, "subject": "Issue A", "status": "Open", "updated_time": "2026-03-19T03:00:00Z", "created_time": "2026-03-18T01:00:00Z"},
        {"id": 102, "account_id": 1, "user_id": 11, "tech_id": 21, "subject": "Issue B", "status": "Closed", "updated_time": "2026-03-19T02:00:00Z", "created_time": "2026-03-18T01:00:00Z"},
    ], synced_at="2026-03-19T01:00:00Z")
    upsert_ticket_details(db, [{"id": 101, "ticketlogs": [{"id": 5001, "log_type": "Response", "plain_note": "done", "record_date": "2026-03-18T01:00:00Z"}], "timelogs": [], "attachments": []}], synced_at="2026-03-19T01:00:00Z")


def test_account_summary(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed_fixture(db)
    summary = get_account_summary(db, "Acme")
    assert summary["status"] == "ok"
    assert summary["stats"]["total_tickets"] == 2
    assert len(summary["open_tickets"]) == 1


def test_technician_summary(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed_fixture(db)
    summary = get_technician_summary(db, "Tech")
    assert summary["status"] == "ok"
    assert summary["stats"]["total_tickets"] == 2
    assert len(summary["open_tickets"]) == 1
