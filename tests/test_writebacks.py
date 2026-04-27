from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from sherpamind.db import connect, initialize_db, upsert_ticket_details, upsert_tickets
from sherpamind.settings import Settings
from sherpamind.writebacks import confirm_stale_unconfirmed_closed_tickets, list_stale_unconfirmed_closed_tickets


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        api_base_url="https://api.sherpadesk.com",
        api_key="secret",
        api_user=None,
        org_key="org",
        instance_key="inst",
        db_path=tmp_path / "sherpamind.sqlite3",
        watch_state_path=tmp_path / "watch_state.json",
        notify_channel=None,
        new_ticket_alerts_enabled=False,
        ticket_update_alerts_enabled=False,
        openclaw_webhook_url=None,
        openclaw_webhook_token=None,
        new_ticket_alert_channel=None,
        ticket_update_alert_channel=None,
        request_min_interval_seconds=0,
        request_timeout_seconds=30,
        seed_page_size=100,
        seed_max_pages=None,
        hot_open_pages=5,
        warm_closed_pages=10,
        warm_closed_days=7,
        cold_closed_pages_per_run=2,
    )


class FakeClient:
    def __init__(self) -> None:
        self.put_calls: list[tuple[str, dict]] = []
        self.get_calls: list[str] = []

    def put(self, path: str, data: dict | None = None):
        self.put_calls.append((path, data or {}))
        return {"status": "ok"}

    def get(self, path: str):
        self.get_calls.append(path)
        ticket_id = path.rsplit("/", 1)[-1]
        return {
            "id": ticket_id,
            "ticket_number": ticket_id,
            "subject": "Old closed ticket",
            "status": "Closed",
            "created_time": "2024-01-01T00:00:00Z",
            "updated_time": "2026-01-02T00:00:00Z",
            "closed_time": "2025-01-01T00:00:00Z",
            "is_confirmed": True,
            "ticketlogs": [],
            "timelogs": [],
            "attachments": [],
        }


def seed_ticket(settings: Settings, ticket_id: int, *, status: str, closed_time: str | None, is_confirmed) -> None:
    row = {
        "id": ticket_id,
        "ticket_number": ticket_id,
        "subject": f"Ticket {ticket_id}",
        "status": status,
        "created_time": "2024-01-01T00:00:00Z",
        "updated_time": "2026-01-01T00:00:00Z",
        "closed_time": closed_time,
    }
    upsert_tickets(settings.db_path, [row], synced_at="2026-04-27T00:00:00Z")
    detail = {
        **row,
        "is_confirmed": is_confirmed,
        "ticketlogs": [],
        "timelogs": [],
        "attachments": [],
    }
    upsert_ticket_details(settings.db_path, [detail], synced_at="2026-04-27T00:00:00Z")


def test_list_stale_unconfirmed_closed_tickets_only_returns_old_closed_false_rows(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    seed_ticket(settings, 101, status="Closed", closed_time="2025-01-01T00:00:00Z", is_confirmed=False)
    seed_ticket(settings, 102, status="Closed", closed_time="2025-12-01T00:00:00Z", is_confirmed=False)
    seed_ticket(settings, 103, status="Closed", closed_time="2025-01-01T00:00:00Z", is_confirmed=True)
    seed_ticket(settings, 104, status="Open", closed_time=None, is_confirmed=False)

    candidates = list_stale_unconfirmed_closed_tickets(
        settings.db_path,
        min_closed_days=365,
        limit=10,
        now=datetime(2026, 4, 27, tzinfo=timezone.utc),
    )

    assert [candidate.ticket_id for candidate in candidates] == ["101"]
    assert candidates[0].closed_days == 481


def test_confirm_stale_unconfirmed_closed_tickets_defaults_to_dry_run(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    seed_ticket(settings, 101, status="Closed", closed_time="2025-01-01T00:00:00Z", is_confirmed=False)

    result = confirm_stale_unconfirmed_closed_tickets(settings, apply=False, limit=10)

    assert result.status == "ok"
    assert result.mode == "dry_run"
    assert result.candidate_count == 1
    assert result.updated_count == 0
    assert result.candidates[0]["ticket_id"] == "101"


def test_confirm_stale_unconfirmed_closed_tickets_apply_updates_and_refreshes(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    seed_ticket(settings, 101, status="Closed", closed_time="2025-01-01T00:00:00Z", is_confirmed=False)
    fake = FakeClient()

    result = confirm_stale_unconfirmed_closed_tickets(settings, client=fake, apply=True, limit=10)

    assert result.status == "ok"
    assert result.updated_count == 1
    assert fake.put_calls == [("tickets/101", {"is_confirmed": "true"})]
    assert fake.get_calls == ["tickets/101"]
    with connect(settings.db_path) as conn:
        confirmed = conn.execute("SELECT json_extract(raw_json, '$.is_confirmed') AS c FROM ticket_details WHERE ticket_id = '101'").fetchone()["c"]
    assert confirmed == 1
