import json
from pathlib import Path

from sherpamind.db import initialize_db, upsert_accounts, upsert_ticket_details, upsert_tickets, upsert_technicians, upsert_users
from sherpamind.documents import build_ticket_document_chunks, build_ticket_documents, export_ticket_chunks, export_ticket_documents, materialize_ticket_documents


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
            "class_name": "Hardware / Printer",
            "created_time": "2026-03-18T01:00:00Z",
            "updated_time": "2026-03-19T03:00:00Z",
            "initial_post": "<p>Can you help with issue A?</p><br>This ticket was created via the email parser.",
            "next_step": "Call back",
        }, {
            "id": 102,
            "account_id": 2,
            "user_id": 12,
            "tech_id": 999,
            "account_name": "Raw Account",
            "user_firstname": "Bob",
            "user_lastname": "Jones",
            "technician_firstname": "Queue",
            "technician_lastname": "Owner",
            "subject": "Issue B",
            "status": "Open",
            "priority_name": "Low",
            "created_time": "2026-03-18T02:00:00Z",
            "updated_time": "2026-03-19T04:00:00Z"
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
    docs_by_id = {doc["ticket_id"]: doc for doc in docs}

    primary = docs_by_id["101"]
    assert primary["doc_id"] == "ticket:101"
    assert "Issue A" in primary["text"]
    assert "Issue summary:" in primary["text"]
    assert "email parser" not in primary["text"].lower()
    assert "Internal note" in primary["text"]
    assert "printer broken" in primary["text"]
    assert "Attachments (metadata only)" in primary["text"]
    assert primary["metadata"]["attachments"][0]["name"] == "shot.png"
    assert primary["metadata"]["attachment_names"] == ["shot.png"]
    assert primary["metadata"]["has_attachments"] is True
    assert primary["metadata"]["category"] == "Hardware / Printer"
    assert primary["metadata"]["cleaned_subject"] == "Issue A"
    assert primary["metadata"]["cleaned_initial_post"] == "Can you help with issue A?"
    assert primary["metadata"]["cleaned_workpad"] == "Internal note"
    assert primary["metadata"]["cleaned_next_step"] == "Call back"
    assert primary["metadata"]["has_next_step"] is True
    assert primary["metadata"]["recent_log_types"] == ["Initial Post"]
    assert primary["metadata"]["recent_log_types_csv"] == "Initial Post"
    assert primary["metadata"]["initial_response_present"] is True
    assert primary["metadata"]["user_email"] == "alice@example.com"
    assert primary["metadata"]["detail_available"] is True
    assert primary["metadata"]["account_label_source"] == "joined"
    assert primary["metadata"]["user_label_source"] == "joined"
    assert primary["metadata"]["technician_label_source"] == "joined"

    fallback = docs_by_id["102"]
    assert fallback["account"] == "Raw Account"
    assert fallback["user_name"] == "Bob Jones"
    assert fallback["technician"] == "Queue Owner"
    assert "Account: Raw Account" in fallback["text"]
    assert "User: Bob Jones" in fallback["text"]
    assert "Technician: Queue Owner" in fallback["text"]
    assert fallback["metadata"]["account_label_source"] == "raw"
    assert fallback["metadata"]["user_label_source"] == "raw"
    assert fallback["metadata"]["technician_label_source"] == "joined"

    chunks = build_ticket_document_chunks(docs)
    chunks_by_ticket = {chunk["ticket_id"]: chunk for chunk in chunks}
    assert chunks_by_ticket["101"]["chunk_id"].startswith("ticket:101:chunk:")
    assert chunks_by_ticket["101"]["account"] == "Acme"

    materialized = materialize_ticket_documents(db)
    assert materialized["status"] == "ok"
    assert materialized["chunk_count"] >= 1

    output = tmp_path / "ticket-docs.jsonl"
    result = export_ticket_documents(db, output)
    assert result["status"] == "ok"
    assert result["document_count"] == 2

    chunk_output = tmp_path / "ticket-chunks.jsonl"
    chunk_result = export_ticket_chunks(db, chunk_output)
    assert chunk_result["status"] == "ok"
    assert chunk_result["chunk_count"] >= 1

    lines = output.read_text().splitlines()
    assert len(lines) == 2
    exported_ids = {json.loads(line)["doc_id"] for line in lines}
    assert exported_ids == {"ticket:101", "ticket:102"}
