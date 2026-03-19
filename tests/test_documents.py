import json
from pathlib import Path

from sherpamind.db import initialize_db, upsert_accounts, upsert_ticket_details, upsert_tickets, upsert_technicians, upsert_users
from sherpamind.documents import build_ticket_document_chunks, build_ticket_documents, export_ticket_documents, materialize_ticket_documents


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
            "next_step": "Call back",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    upsert_ticket_details(
        db,
        [{
            "id": 101,
            "workpad": "Internal note",
            "initial_response": True,
            "ticketlogs": [{"id": 501, "log_type": "Initial Post", "record_date": "2026-03-18T01:00:00Z", "plain_note": "printer broken"}],
            "timelogs": [],
            "attachments": [{"id": "a1", "name": "shot.png", "url": "https://example/shot.png", "size": 1234, "date": "2026-03-18T01:00:00Z"}],
        }],
        synced_at="2026-03-19T01:00:00Z",
    )


def test_build_materialize_and_export_ticket_documents(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed_fixture(db)
    docs = build_ticket_documents(db)
    assert docs[0]["doc_id"] == "ticket:101"
    assert "Issue A" in docs[0]["text"]
    assert "Internal note" in docs[0]["text"]
    assert "printer broken" in docs[0]["text"]
    assert "Attachments (metadata only)" in docs[0]["text"]
    assert docs[0]["metadata"]["attachments"][0]["name"] == "shot.png"

    chunks = build_ticket_document_chunks(docs)
    assert chunks[0]["chunk_id"].startswith("ticket:101:chunk:")

    materialized = materialize_ticket_documents(db)
    assert materialized["status"] == "ok"
    assert materialized["chunk_count"] >= 1

    output = tmp_path / "ticket-docs.jsonl"
    result = export_ticket_documents(db, output)
    assert result["status"] == "ok"
    assert result["document_count"] == 1
    lines = output.read_text().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["doc_id"] == "ticket:101"
