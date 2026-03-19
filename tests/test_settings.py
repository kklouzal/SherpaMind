from pathlib import Path

from sherpamind.settings import load_settings


def test_load_settings_reads_request_controls(monkeypatch) -> None:
    monkeypatch.setenv("SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS", "3.5")
    monkeypatch.setenv("SHERPAMIND_REQUEST_TIMEOUT_SECONDS", "45")
    settings = load_settings()
    assert settings.request_min_interval_seconds == 3.5
    assert settings.request_timeout_seconds == 45.0


def test_load_settings_defaults_paths(monkeypatch) -> None:
    monkeypatch.delenv("SHERPAMIND_DB_PATH", raising=False)
    monkeypatch.delenv("SHERPAMIND_WATCH_STATE_PATH", raising=False)
    settings = load_settings()
    assert settings.db_path == Path("state/sherpamind.sqlite3")
    assert settings.watch_state_path == Path("state/watch_state.json")


def test_load_settings_reads_seed_controls(monkeypatch) -> None:
    monkeypatch.setenv("SHERPAMIND_SEED_PAGE_SIZE", "50")
    monkeypatch.setenv("SHERPAMIND_SEED_MAX_PAGES", "3")
    settings = load_settings()
    assert settings.seed_page_size == 50
    assert settings.seed_max_pages == 3
