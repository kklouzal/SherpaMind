from pathlib import Path

from sherpamind.db import initialize_db, replace_ticket_document_chunks, replace_ticket_documents
from sherpamind.vector_index import build_vector_index, get_vector_index_status, search_vector_index, vectorize_text


def seed(db: Path) -> None:
    initialize_db(db)
    replace_ticket_documents(
        db,
        [
            {"doc_id": "ticket:101", "ticket_id": 101, "status": "Open", "account": "Acme", "user_name": "Alice", "technician": "Tech One", "updated_at": "2026-03-19T03:00:00Z", "text": "Printer issue in office", "metadata": {"priority": "High", "category": "Hardware", "class_name": "Technical Incident", "submission_category": "Portal", "resolution_category": "Completed", "department_label": "Managed Services"}, "content_hash": "a"},
            {"doc_id": "ticket:102", "ticket_id": 102, "status": "Closed", "account": "Beta", "user_name": "Bob", "technician": "Tech Two", "updated_at": "2026-03-19T02:00:00Z", "text": "Outlook email sync issue", "metadata": {"priority": "Low", "category": "Software", "class_name": "Service Request", "submission_category": "Email", "resolution_category": "Escalated", "department_label": "Dispatch"}, "content_hash": "b"},
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
    status = get_vector_index_status(db)
    assert status["indexed_chunks"] == 2
    assert status["total_chunk_rows"] == 2
    assert status["ready_ratio"] == 1.0
    assert status["missing_index_rows"] == 0
    assert status["outdated_content_rows"] == 0
    rows = search_vector_index(db, "printer", limit=5)
    assert rows[0]["ticket_id"] == "101"
    assert rows[0]["priority"] == "High"
    assert rows[0]["class_name"] == "Technical Incident"
    assert rows[0]["submission_category"] == "Portal"
    assert rows[0]["resolution_category"] == "Completed"
    assert rows[0]["department_label"] == "Managed Services"
    filtered = search_vector_index(
        db,
        "issue",
        limit=5,
        account="Beta",
        status="Closed",
        priority="Low",
        category="Soft",
        class_name="Request",
        submission_category="mail",
        resolution_category="Escal",
        department="patch",
    )
    assert filtered[0]["ticket_id"] == "102"


def test_vector_index_status_detects_outdated_rows(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed(db)
    build_vector_index(db, dims=32)
    replace_ticket_document_chunks(
        db,
        [
            {"chunk_id": "ticket:101:chunk:0", "doc_id": "ticket:101", "ticket_id": 101, "chunk_index": 0, "text": "Printer issue changed", "content_hash": "changed"},
            {"chunk_id": "ticket:102:chunk:0", "doc_id": "ticket:102", "ticket_id": 102, "chunk_index": 0, "text": "Outlook email sync issue", "content_hash": "b"},
        ],
        synced_at="2026-03-19T02:00:00Z",
    )
    status = get_vector_index_status(db)
    assert status["indexed_chunks"] == 2
    assert status["missing_index_rows"] == 0
    assert status["outdated_content_rows"] == 1
