import json
from pathlib import Path

from sherpamind.db import initialize_db, upsert_accounts, upsert_tickets, upsert_technicians, upsert_users
from sherpamind.documents import build_ticket_documents, export_ticket_documents


def seed_fixture(db: Path) -> None:
    initialize_db(db)
    upsert_accounts(db, [{"id": 1, "name": "Acme"}], synced_at="2026-03-19T01:00:00Z")
    upsert_users(db, [{"id": 11, "account_id": 1, "FullName": "Alice User", "email": "alice@example.com"}], synced_at="2026-03-19T01:00:00Z")
    upsert_technicians(db, [{"id": 21, "FullName": "Tech One", "email": "tech@example.com"}], synced_at="2026-03-19T01:00:00Z")
    upsert_tickets(
        db,
        [{
            "id": 101,
            "account_id": 1,
            "user_id": 11,
            "tech_id": 21,
            "subject": "Issue A",
            "status": "Open",
            "priority_name": "High",
            "creation_category_name": "Hardware",
            "created_time": "2026-03-18T01:00:00Z",
            "updated_time": "2026-03-19T03:00:00Z",
            "initial_post": "Can you help with issue A?",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )


def test_build_and_export_ticket_documents(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed_fixture(db)
    docs = build_ticket_documents(db)
    assert docs[0]["doc_id"] == "ticket:101"
    assert "Issue A" in docs[0]["text"]

    output = tmp_path / "ticket-docs.jsonl"
    result = export_ticket_documents(db, output)
    assert result["status"] == "ok"
    assert result["document_count"] == 1
    lines = output.read_text().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["doc_id"] == "ticket:101"
