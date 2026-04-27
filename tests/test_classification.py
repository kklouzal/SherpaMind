from pathlib import Path

from sherpamind.classification import build_classification_prompt, dispatch_ticket_classification_events, enqueue_initial_ticket_classification, record_classification
from sherpamind.db import connect, initialize_db, replace_ticket_taxonomy_classes, upsert_tickets
from sherpamind.settings import Settings


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        api_base_url="https://api.sherpadesk.com",
        api_key="secret",
        api_user=None,
        org_key="org",
        instance_key="inst",
        db_path=tmp_path / "sherpamind.sqlite3",
        watch_state_path=tmp_path / "watch_state.json",
        new_ticket_alerts_enabled=False,
        ticket_update_alerts_enabled=False,
        openclaw_webhook_url="http://127.0.0.1:18789/hooks/agent",
        openclaw_webhook_token="hook-token",
        new_ticket_alert_channel=None,
        ticket_update_alert_channel=None,
        request_min_interval_seconds=0,
        request_timeout_seconds=30,
        seed_page_size=100,
        seed_max_pages=None,
        hot_open_pages=3,
        warm_closed_pages=10,
        warm_closed_days=7,
        cold_closed_pages_per_run=2,
    )


def seed_taxonomy(db: Path) -> None:
    replace_ticket_taxonomy_classes(
        db,
        [
            {"id": "10", "parent_id": None, "name": "Hardware", "path": "Hardware", "is_lastchild": False, "is_active": True},
            {"id": "11", "parent_id": "10", "name": "Printer", "path": "Hardware / Printer", "is_lastchild": True, "is_active": True},
            {"id": "12", "parent_id": "10", "name": "Desktop", "path": "Hardware / Desktop", "is_lastchild": True, "is_active": True},
        ],
        synced_at="2026-03-19T01:00:00Z",
    )


def test_initial_classification_prompt_is_bounded_to_event_context(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    seed_taxonomy(settings.db_path)
    upsert_tickets(settings.db_path, [{"id": 1, "subject": "Printer jam", "status": "Open", "created_time": "2026-03-19T00:00:00Z", "updated_time": "2026-03-19T00:10:00Z"}])
    enqueue = enqueue_initial_ticket_classification(settings, {"id": 1, "subject": "Printer jam", "status": "Open", "updated_time": "2026-03-19T00:10:00Z"}, trigger_source="watch.last_state")
    with connect(settings.db_path) as conn:
        event = dict(conn.execute("SELECT * FROM ticket_classification_events WHERE id = ?", (enqueue["id"],)).fetchone())

    prompt = build_classification_prompt(settings, event)
    message = prompt["message"]
    assert "Do not do retrieval or broad ticket searches" in message
    assert "Hardware / Printer" in message
    assert "record-ticket-classification-json" in message
    assert "candidate_count_total" in message
    assert "candidate_truncated" in message
    assert "do not perform extra searches unless" not in message
    assert "Printer jam" in message


def test_dispatch_classification_posts_hook_and_waits_for_record(tmp_path: Path, monkeypatch) -> None:
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    seed_taxonomy(settings.db_path)
    upsert_tickets(settings.db_path, [{"id": 1, "subject": "Printer jam", "status": "Open", "created_time": "2026-03-19T00:00:00Z", "updated_time": "2026-03-19T00:10:00Z"}])
    enqueue_initial_ticket_classification(settings, {"id": 1, "subject": "Printer jam", "status": "Open", "updated_time": "2026-03-19T00:10:00Z"}, trigger_source="watch.last_state")
    calls = []
    monkeypatch.setattr("sherpamind.classification._post_hook_payload", lambda settings, payload: calls.append(payload) or (True, "http:202"))

    dispatched = dispatch_ticket_classification_events(settings, limit=1)

    assert dispatched["dispatched"] == 1
    assert calls[0]["deliver"] is False
    with connect(settings.db_path) as conn:
        row = conn.execute("SELECT status, prompt_json FROM ticket_classification_events").fetchone()
    assert row["status"] == "awaiting_result"
    assert row["prompt_json"]


def test_record_classification_validates_taxonomy_class(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    seed_taxonomy(settings.db_path)
    enqueue = enqueue_initial_ticket_classification(settings, {"id": 1, "subject": "Printer jam", "status": "Open", "updated_time": "2026-03-19T00:10:00Z"}, trigger_source="watch.last_state")

    result = record_classification(settings, event_id=enqueue["id"], class_id="11", confidence="high", rationale="Printer issue maps to printer subclass.")

    assert result["status"] == "ok"
    assert result["class_path"] == "Hardware / Printer"
    with connect(settings.db_path) as conn:
        row = conn.execute("SELECT status, result_class_id, confidence FROM ticket_classification_events").fetchone()
    assert row["status"] == "completed"
    assert row["result_class_id"] == "11"
    assert row["confidence"] == "high"


def test_record_classification_rejects_non_leaf_taxonomy_class(tmp_path: Path) -> None:
    base = make_settings(tmp_path)
    settings = Settings(**{**base.__dict__, "api_key": None})
    initialize_db(settings.db_path)
    seed_taxonomy(settings.db_path)
    enqueue = enqueue_initial_ticket_classification(settings, {"id": 1, "subject": "Printer jam", "status": "Open", "updated_time": "2026-03-19T00:10:00Z"}, trigger_source="watch.last_state")

    try:
        record_classification(settings, event_id=enqueue["id"], class_id="10", confidence="high", rationale="Parent class should not write back.")
    except ValueError as exc:
        assert "not a leaf" in str(exc)
    else:  # pragma: no cover - assertion clarity
        raise AssertionError("expected non-leaf class rejection")


def test_writeback_completed_ticket_classification_updates_changed_leaf_class(tmp_path: Path) -> None:
    from sherpamind.classification import write_back_completed_ticket_classifications

    settings = Settings(**{**make_settings(tmp_path).__dict__, "ticket_class_taxonomy_max_age_seconds": 999999999})
    initialize_db(settings.db_path)
    seed_taxonomy(settings.db_path)
    upsert_tickets(settings.db_path, [{"id": 1, "subject": "Printer jam", "status": "Open", "class_id": 12, "class_name": "Hardware / Desktop", "updated_time": "2026-03-19T00:10:00Z"}])
    enqueue = enqueue_initial_ticket_classification(settings, {"id": 1, "subject": "Printer jam", "status": "Open", "class_id": 12, "class_name": "Hardware / Desktop", "updated_time": "2026-03-19T00:10:00Z"}, trigger_source="watch.last_state")
    record_classification(settings, event_id=enqueue["id"], class_id="11", confidence="high", rationale="Printer issue maps to printer subclass.")

    class FakeClient:
        def __init__(self):
            self.class_id = "12"
            self.puts = []

        def list_ticket_classes(self):
            raise AssertionError("taxonomy is fresh; should not refresh")

        def get(self, path):
            assert path == "tickets/1"
            return {"id": 1, "subject": "Printer jam", "status": "Open", "class_id": self.class_id, "class_name": "Hardware / Desktop" if self.class_id == "12" else "Hardware / Printer"}

        def put(self, path, data=None):
            assert path == "tickets/1"
            self.puts.append(dict(data or {}))
            self.class_id = str(data["class_id"])
            return ""

    fake = FakeClient()
    result = write_back_completed_ticket_classifications(settings, client=fake, limit=1, apply=True)

    assert result["updated"] == 1
    assert fake.puts == [{"class_id": "11"}]
    with connect(settings.db_path) as conn:
        row = conn.execute("SELECT writeback_status, writeback_attempt_count FROM ticket_classification_events WHERE id = ?", (enqueue["id"],)).fetchone()
    assert row["writeback_status"] == "succeeded"
    assert row["writeback_attempt_count"] == 1


def test_writeback_completed_ticket_classification_skips_same_class(tmp_path: Path) -> None:
    from sherpamind.classification import write_back_completed_ticket_classifications

    settings = Settings(**{**make_settings(tmp_path).__dict__, "ticket_class_taxonomy_max_age_seconds": 999999999})
    initialize_db(settings.db_path)
    seed_taxonomy(settings.db_path)
    enqueue = enqueue_initial_ticket_classification(settings, {"id": 1, "subject": "Printer jam", "status": "Open", "class_id": 11, "class_name": "Hardware / Printer", "updated_time": "2026-03-19T00:10:00Z"}, trigger_source="watch.last_state")
    record_classification(settings, event_id=enqueue["id"], class_id="11", confidence="high", rationale="Already correct.")

    class FakeClient:
        def list_ticket_classes(self):
            raise AssertionError("taxonomy is fresh; should not refresh")

        def get(self, path):
            return {"id": 1, "subject": "Printer jam", "status": "Open", "class_id": 11, "class_name": "Hardware / Printer"}

        def put(self, path, data=None):
            raise AssertionError("same class should not be written")

    result = write_back_completed_ticket_classifications(settings, client=FakeClient(), limit=1, apply=True)

    assert result["updated"] == 0
    assert result["skipped"] == 1
    with connect(settings.db_path) as conn:
        row = conn.execute("SELECT writeback_status FROM ticket_classification_events WHERE id = ?", (enqueue["id"],)).fetchone()
    assert row["writeback_status"] == "skipped_same_class"
