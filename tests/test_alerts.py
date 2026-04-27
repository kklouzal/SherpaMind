from pathlib import Path
from urllib import request

from sherpamind.db import initialize_db, upsert_accounts, upsert_ticket_details, upsert_tickets, upsert_technicians, upsert_users
from sherpamind.documents import materialize_ticket_documents
from sherpamind.settings import Settings
from sherpamind.alerts import _alert_payload_json, _build_hook_payload, _build_ticket_update_payload, _post_hook_payload
from sherpamind.summaries import get_ticket_summary


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        api_base_url="https://api.sherpadesk.com",
        api_key="secret",
        api_user=None,
        org_key="org",
        instance_key="inst",
        db_path=tmp_path / "sherpamind.sqlite3",
        watch_state_path=tmp_path / "watch_state.json",
        new_ticket_alerts_enabled=True,
        ticket_update_alerts_enabled=True,
        openclaw_webhook_url="http://127.0.0.1:18789/hooks/agent",
        openclaw_webhook_token=None,
        new_ticket_alert_channel="channel:1488924125736079492",
        ticket_update_alert_channel="channel:1488924125736079492",
        request_min_interval_seconds=0,
        request_timeout_seconds=30,
        seed_page_size=100,
        seed_max_pages=None,
        hot_open_pages=5,
        warm_closed_pages=10,
        warm_closed_days=7,
        cold_closed_pages_per_run=2,
    )


def seed_fixture(db: Path) -> None:
    initialize_db(db)
    upsert_accounts(db, [{"id": 1, "name": "Acme Co"}], synced_at="2026-03-19T01:00:00Z")
    upsert_users(db, [{"id": 11, "account_id": 1, "FullName": "Alice User", "email": "alice@example.com"}], synced_at="2026-03-19T01:00:00Z")
    upsert_technicians(db, [{"id": 21, "FullName": "Tech One", "email": "tech@example.com"}], synced_at="2026-03-19T01:00:00Z")
    upsert_tickets(db, [{
        "id": 101,
        "account_id": 1,
        "user_id": 11,
        "tech_id": 21,
        "subject": "Printer offline in front office",
        "status": "Open",
        "priority_name": "High",
        "category": "Printer",
        "updated_time": "2026-03-19T03:00:00Z",
        "created_time": "2026-03-18T01:00:00Z",
    }], synced_at="2026-03-19T01:00:00Z")
    upsert_ticket_details(db, [{
        "id": 101,
        "support_group_name": "Managed Services",
        "default_contract_name": "Gold",
        "ticketlogs": [{"id": 5001, "log_type": "Initial Post", "plain_note": "Printer went offline after a paper jam.", "record_date": "2026-03-18T01:00:00Z"}],
        "timelogs": [],
        "attachments": [],
    }], synced_at="2026-03-19T01:00:00Z")
    materialize_ticket_documents(db)


def test_build_hook_payload_contains_ticket_triage_context(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    seed_fixture(settings.db_path)
    summary = get_ticket_summary(settings.db_path, "101", limit_logs=5, limit_attachments=5)
    payload = _build_hook_payload(settings, "101", summary)

    assert payload["agentId"] == "main"
    assert payload["channel"] == "discord"
    assert payload["to"] == "channel:1488924125736079492"
    assert payload["deliver"] is True
    assert set(payload) == {"agentId", "name", "message", "wakeMode", "deliver", "channel", "to", "timeoutSeconds"}
    message = payload["message"]
    assert "new SherpaDesk ticket" in message
    assert "INITIAL POST / original user-submitted issue only" in message
    assert "Use the SherpaMind skill/query surface" in message
    assert "Seen before:" in message
    assert "3-5 short sentences or bullet-style lines" in message
    assert "Issue synopsis:" in message
    assert "First checks / next steps:" in message
    assert "Printer offline in front office" in message
    assert "Alice User" in message or "alice@example.com" in message
    assert "Acme Co" in message
    assert "recent_logs" not in message
    assert "next_step_context" not in message


def test_build_hook_payload_omits_target_when_no_alert_channel_is_configured(tmp_path: Path) -> None:
    settings = Settings(**{**make_settings(tmp_path).__dict__, "new_ticket_alert_channel": None, "ticket_update_alert_channel": None})
    seed_fixture(settings.db_path)
    summary = get_ticket_summary(settings.db_path, "101", limit_logs=5, limit_attachments=5)
    payload = _build_hook_payload(settings, "101", summary)

    assert payload["channel"] == "discord"
    assert "to" not in payload


def test_alert_payload_json_tolerates_bad_queue_payloads() -> None:
    assert _alert_payload_json({"payload_json": '{"event_key":"abc"}'}) == {"event_key": "abc"}
    assert _alert_payload_json({"payload_json": "not-json"}) == {}
    assert _alert_payload_json({"payload_json": "[]"}) == {}
    assert _alert_payload_json({}) == {}


def test_post_hook_payload_uses_current_openclaw_hook_auth_headers(monkeypatch, tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    settings = Settings(**{**settings.__dict__, "openclaw_webhook_token": "hook-token"})
    captured = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self) -> bytes:
            return b"{}"

    def fake_urlopen(req: request.Request, timeout: int):
        captured["url"] = req.full_url
        captured["timeout"] = timeout
        captured["headers"] = dict(req.header_items())
        captured["body"] = req.data
        return Response()

    monkeypatch.setattr(request, "urlopen", fake_urlopen)
    result = _post_hook_payload(settings, "101", {"message": "hello"})

    assert result.status == "ok"
    assert captured["url"] == "http://127.0.0.1:18789/hooks/agent"
    assert captured["headers"]["Authorization"] == "Bearer hook-token"
    assert captured["headers"]["X-openclaw-token"] == "hook-token"
    assert captured["body"] == b'{"message": "hello"}'


def test_build_ticket_update_payload_allows_broader_history_context(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    seed_fixture(settings.db_path)
    summary = get_ticket_summary(settings.db_path, "101", limit_logs=5, limit_attachments=5)
    payload = _build_ticket_update_payload(settings, "101", summary)

    assert payload["agentId"] == "main"
    assert payload["channel"] == "discord"
    assert payload["to"] == "channel:1488924125736079492"
    assert set(payload) == {"agentId", "name", "message", "wakeMode", "deliver", "channel", "to", "timeoutSeconds"}
    message = payload["message"]
    assert "new NON-TECH update" in message
    assert "broader ticket history" in message
    assert "Use the SherpaMind skill/query surface" in message
    assert "Seen before:" in message
    assert "3-5 short sentences or bullet-style lines" in message
    assert "Update synopsis:" in message
    assert "Recommended follow-up / next steps:" in message
    assert "OpenClaw hook delivery will send it" in message
    assert "recent_logs" in message
    assert "retrieval_metadata" in message
