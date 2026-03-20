import json
from pathlib import Path

from sherpamind.db import initialize_db, replace_ticket_document_chunks, replace_ticket_documents
from sherpamind.vector_exports import export_embedding_manifest, export_embedding_ready_chunks


def seed(db: Path) -> None:
    initialize_db(db)
    replace_ticket_documents(
        db,
        [{
            "doc_id": "ticket:101",
            "ticket_id": 101,
            "status": "Open",
            "account": "Acme",
            "user_name": "Alice",
            "technician": "Tech One",
            "updated_at": "2026-03-19T03:00:00Z",
            "text": "hello",
            "metadata": {
                "priority": "High",
                "category": "Hardware",
                "attachments_count": 1,
                "has_attachments": True,
                "ticketlogs_count": 5,
                "timelogs_count": 0,
                "cleaned_subject": "hello",
                "cleaned_initial_post": "Help me",
                "cleaned_next_step": "Call back tomorrow",
                "has_next_step": True,
                "recent_log_types_csv": "Initial Post, Response",
                "initial_response_present": True,
                "user_email": "alice@example.com",
                "resolution_summary": "Closed successfully",
                "has_resolution_summary": True,
            },
            "content_hash": "abc",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    replace_ticket_document_chunks(
        db,
        [{
            "chunk_id": "ticket:101:chunk:0",
            "doc_id": "ticket:101",
            "ticket_id": 101,
            "chunk_index": 0,
            "text": "chunk text",
            "content_hash": "def",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )


def test_export_embedding_ready_chunks(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed(db)
    output = tmp_path / "embedding.jsonl"
    result = export_embedding_ready_chunks(db, output)
    assert result["status"] == "ok"
    row = json.loads(output.read_text().splitlines()[0])
    assert row["id"] == "ticket:101:chunk:0"
    assert row["metadata"]["account"] == "Acme"
    assert row["metadata"]["priority"] == "High"
    assert row["metadata"]["ticketlogs_count"] == 5
    assert row["metadata"]["has_attachments"] is True
    assert row["metadata"]["cleaned_next_step"] == "Call back tomorrow"
    assert row["metadata"]["has_next_step"] is True
    assert row["metadata"]["recent_log_types"] == "Initial Post, Response"
    assert row["metadata"]["user_email"] == "alice@example.com"
    assert row["metadata"]["resolution_summary"] == "Closed successfully"
    assert row["metadata"]["has_resolution_summary"] is True


def test_export_embedding_manifest(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed(db)
    output = tmp_path / "manifest.json"
    result = export_embedding_manifest(db, output)
    assert result["status"] == "ok"
    manifest = json.loads(output.read_text())
    assert manifest["chunk_count"] == 1
    assert manifest["accounts"] == ["Acme"]
