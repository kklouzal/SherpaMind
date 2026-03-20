from pathlib import Path

from sherpamind.db import initialize_db, upsert_accounts, upsert_ticket_details, upsert_tickets, upsert_technicians, upsert_users
from sherpamind.public_artifacts import generate_public_snapshot


def seed_fixture(db: Path) -> None:
    initialize_db(db)
    upsert_accounts(db, [{"id": 1, "name": "Acme"}], synced_at="2026-03-19T01:00:00Z")
    upsert_users(db, [{"id": 11, "account_id": 1, "FullName": "Alice User"}], synced_at="2026-03-19T01:00:00Z")
    upsert_technicians(db, [{"id": 21, "FullName": "Tech One"}], synced_at="2026-03-19T01:00:00Z")
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
            "created_time": "2026-03-18T01:00:00Z",
            "updated_time": "2026-03-19T03:00:00Z",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    upsert_ticket_details(
        db,
        [{"id": 101, "ticketlogs": [], "timelogs": [], "attachments": [{"id": "a1", "name": "shot.png", "url": "https://example/shot.png", "size": 1234, "date": "2026-03-18T01:00:00Z"}]}],
        synced_at="2026-03-19T01:00:00Z",
    )


def test_generate_public_snapshot(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    db = tmp_path / ".SherpaMind" / "private" / "sherpamind.sqlite3"
    seed_fixture(db)
    result = generate_public_snapshot(db)
    assert result["status"] == "ok"
    output = Path(result["output_path"])
    text = output.read_text()
    assert "SherpaMind Public Insight Snapshot" in text
    assert "Attachment metadata summary" in text
    assert ".SherpaMind/private/sherpamind.sqlite3" in text
    assert len(result["generated_files"]) >= 5
    for generated in result["generated_files"]:
        assert Path(generated).exists()
