from pathlib import Path

from sherpamind.db import initialize_db, replace_ticket_document_chunks, replace_ticket_documents
from sherpamind.vector_index import build_vector_index, search_vector_index, vectorize_text


def seed(db: Path) -> None:
    initialize_db(db)
    replace_ticket_documents(
        db,
        [
            {"doc_id": "ticket:101", "ticket_id": 101, "status": "Open", "account": "Acme", "user_name": "Alice", "technician": "Tech One", "updated_at": "2026-03-19T03:00:00Z", "text": "Printer issue in office", "metadata": {}, "content_hash": "a"},
            {"doc_id": "ticket:102", "ticket_id": 102, "status": "Closed", "account": "Beta", "user_name": "Bob", "technician": "Tech Two", "updated_at": "2026-03-19T02:00:00Z", "text": "Outlook email sync issue", "metadata": {}, "content_hash": "b"},
        ],
        synced_at="2026-03-19T01:00:00Z",
    )
    replace_ticket_document_chunks(
        db,
        [
            {"chunk_id": "ticket:101:chunk:0", "doc_id": "ticket:101", "ticket_id": 101, "chunk_index": 0, "text": "Printer issue in office", "content_hash": "a"},
            {"chunk_id": "ticket:102:chunk:0", "doc_id": "ticket:102", "ticket_id": 102, "chunk_index": 0, "text": "Outlook email sync issue", "content_hash": "b"},
        ],
        synced_at="2026-03-19T01:00:00Z",
    )


def test_vectorize_text_returns_fixed_dimensions() -> None:
    vec = vectorize_text("printer issue", dims=32)
    assert len(vec) == 32


def test_build_and_search_vector_index(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed(db)
    result = build_vector_index(db, dims=32)
    assert result["status"] == "ok"
    rows = search_vector_index(db, "printer", limit=5)
    assert rows[0]["ticket_id"] == "101"
    filtered = search_vector_index(db, "issue", limit=5, account="Beta", status="Closed")
    assert filtered[0]["ticket_id"] == "102"
