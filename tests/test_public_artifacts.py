from pathlib import Path

from sherpamind.db import initialize_db, upsert_accounts, upsert_ticket_details, upsert_tickets, upsert_technicians, upsert_users
from sherpamind.public_artifacts import generate_public_snapshot


def seed_fixture(db: Path) -> None:
    initialize_db(db)
    upsert_accounts(
        db,
        [
            {"id": 1, "name": "Acme"},
            {"id": 2, "name": "Beta Org"},
        ],
        synced_at="2026-03-19T01:00:00Z",
    )
    upsert_users(
        db,
        [
            {"id": 11, "account_id": 1, "FullName": "Alice User"},
            {"id": 12, "account_id": 2, "FullName": "Bob User"},
        ],
        synced_at="2026-03-19T01:00:00Z",
    )
    upsert_technicians(
        db,
        [
            {"id": 21, "FullName": "Tech One"},
            {"id": 22, "FullName": "Tech Two"},
        ],
        synced_at="2026-03-19T01:00:00Z",
    )
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
                "creation_category_name": "Email",
                "created_time": "2026-03-18T01:00:00Z",
                "updated_time": "2026-03-19T03:00:00Z",
            },
            {
                "id": 102,
                "account_id": 2,
                "user_id": 12,
                "tech_id": 22,
                "subject": "Issue B",
                "status": "Closed",
                "priority_name": "Low",
                "creation_category_name": "Printer",
                "created_time": "2026-03-17T01:00:00Z",
                "updated_time": "2026-03-19T04:00:00Z",
                "closed_time": "2026-03-19T04:00:00Z",
            },
        ],
        synced_at="2026-03-19T01:00:00Z",
    )
    upsert_ticket_details(
        db,
        [
            {
                "id": 101,
                "ticketlogs": [
                    {
                        "id": "log-101",
                        "log_type": "Comment",
                        "record_date": "2026-03-19T02:00:00Z",
                        "note": "Investigating mail flow.",
                        "plain_note": "Investigating mail flow.",
                        "user_firstname": "Tech",
                        "user_lastname": "One",
                    }
                ],
                "timelogs": [],
                "attachments": [
                    {"id": "a1", "name": "shot.png", "url": "https://example/shot.png", "size": 1234, "date": "2026-03-18T01:00:00Z"}
                ],
            }
        ],
        synced_at="2026-03-19T01:00:00Z",
    )


def test_generate_public_snapshot(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    db = tmp_path / ".SherpaMind" / "private" / "data" / "sherpamind.sqlite3"
    seed_fixture(db)
    result = generate_public_snapshot(db)
    assert result["status"] == "ok"
    assert result["account_docs_generated"] == 2
    assert result["technician_docs_generated"] == 2

    output = Path(result["output_path"])
    text = output.read_text()
    assert "SherpaMind Public Insight Snapshot" in text
    assert "Enrichment coverage" in text
    assert "Sync freshness" in text
    assert "Attachment metadata summary" in text
    assert "Retrieval metadata readiness" in text
    assert "Account artifact coverage" in text
    assert "Technician artifact coverage" in text
    assert ".SherpaMind/private/data/sherpamind.sqlite3" in text

    account_index = tmp_path / ".SherpaMind" / "public" / "docs" / "accounts" / "index.md"
    technician_index = tmp_path / ".SherpaMind" / "public" / "docs" / "technicians" / "index.md"
    acme_doc = tmp_path / ".SherpaMind" / "public" / "docs" / "accounts" / "Acme.md"
    beta_doc = tmp_path / ".SherpaMind" / "public" / "docs" / "accounts" / "Beta_Org.md"
    tech_one_doc = tmp_path / ".SherpaMind" / "public" / "docs" / "technicians" / "Tech_One.md"
    tech_two_doc = tmp_path / ".SherpaMind" / "public" / "docs" / "technicians" / "Tech_Two.md"

    assert account_index.exists()
    assert technician_index.exists()
    assert acme_doc.exists()
    assert beta_doc.exists()
    assert tech_one_doc.exists()
    assert tech_two_doc.exists()

    assert "Total account docs: `2`" in account_index.read_text()
    assert "Total technician docs: `2`" in technician_index.read_text()
    assert "Status breakdown" in acme_doc.read_text()
    assert "Category breakdown" in tech_one_doc.read_text()

    assert len(result["generated_files"]) >= 9
    for generated in result["generated_files"]:
        assert Path(generated).exists()
