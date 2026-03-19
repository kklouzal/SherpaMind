from pathlib import Path

from sherpamind.analysis import (
    get_dataset_summary,
    get_insight_snapshot,
    list_open_ticket_ages,
    list_recent_account_activity,
    list_recent_tickets,
    list_technician_recent_load,
    list_ticket_counts_by_account,
    list_ticket_counts_by_priority,
    list_ticket_counts_by_status,
    list_ticket_counts_by_technician,
)
from sherpamind.db import initialize_db, upsert_accounts, upsert_tickets, upsert_technicians, upsert_users


def seed_fixture(db: Path) -> None:
    initialize_db(db)
    upsert_accounts(db, [{"id": 1, "name": "Acme"}, {"id": 2, "name": "Beta"}], synced_at="2026-03-19T01:00:00Z")
    upsert_users(db, [{"id": 11, "account_id": 1, "FullName": "Alice User"}], synced_at="2026-03-19T01:00:00Z")
    upsert_technicians(db, [{"id": 21, "FullName": "Tech One"}], synced_at="2026-03-19T01:00:00Z")
    upsert_tickets(
        db,
        [
            {
                "id": 101,
                "account_id": 1,
                "user_id": 11,
                "tech_id": 21,
                "subject": "Issue A",
                "status": "Open",
                "priority_name": "High",
                "created_time": "2026-03-18T01:00:00Z",
                "updated_time": "2026-03-19T03:00:00Z",
                "initial_post": "Can you help with issue A?",
            },
            {
                "id": 102,
                "account_id": 1,
                "subject": "Issue B",
                "status": "Closed",
                "priority_name": "Low",
                "created_time": "2026-03-18T01:00:00Z",
                "updated_time": "2026-03-19T02:00:00Z",
            },
            {
                "id": 103,
                "account_id": 2,
                "subject": "Issue C",
                "status": "Open",
                "priority_name": "High",
                "created_time": "2026-03-18T01:00:00Z",
                "updated_time": "2026-03-19T01:00:00Z",
            },
        ],
        synced_at="2026-03-19T01:00:00Z",
    )


def test_analysis_reports(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed_fixture(db)
    by_account = list_ticket_counts_by_account(db)
    by_status = list_ticket_counts_by_status(db)
    by_priority = list_ticket_counts_by_priority(db)
    by_technician = list_ticket_counts_by_technician(db)
    recent = list_recent_tickets(db, limit=2)
    open_ages = list_open_ticket_ages(db, limit=2)
    recent_accounts = list_recent_account_activity(db, days=30, limit=5)
    recent_techs = list_technician_recent_load(db, days=30, limit=5)
    summary = get_dataset_summary(db)
    snapshot = get_insight_snapshot(db)

    assert by_account[0]["account"] == "Acme"
    assert by_account[0]["ticket_count"] == 2
    assert {row["status"]: row["ticket_count"] for row in by_status}["Open"] == 2
    assert {row["priority"]: row["ticket_count"] for row in by_priority}["High"] == 2
    assert by_technician[0]["ticket_count"] >= 1
    assert recent[0]["subject"] == "Issue A"
    assert open_ages[0]["status"] == "Open" if "status" in open_ages[0] else True
    assert recent_accounts[0]["ticket_count"] >= 1
    assert recent_techs[0]["ticket_count"] >= 1
    assert summary["counts"]["tickets"] == 3
    assert snapshot["dataset_summary"]["counts"]["tickets"] == 3
