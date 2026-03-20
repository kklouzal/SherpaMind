import json
from pathlib import Path

from sherpamind.db import initialize_db, replace_ticket_document_chunks, replace_ticket_documents
from sherpamind.vector_exports import export_embedding_ready_chunks


def test_export_embedding_ready_chunks(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
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
            "metadata": {"priority": "High", "category": "Hardware", "attachments_count": 1},
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
    output = tmp_path / "embedding.jsonl"
    result = export_embedding_ready_chunks(db, output)
    assert result["status"] == "ok"
    row = json.loads(output.read_text().splitlines()[0])
    assert row["id"] == "ticket:101:chunk:0"
    assert row["metadata"]["account"] == "Acme"
    assert row["metadata"]["priority"] == "High"
