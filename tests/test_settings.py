from pathlib import Path

from sherpamind.settings import load_settings


def test_load_settings_reads_request_controls(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS", "3.5")
    monkeypatch.setenv("SHERPAMIND_REQUEST_TIMEOUT_SECONDS", "45")
    settings = load_settings()
    assert settings.request_min_interval_seconds == 3.5
    assert settings.request_timeout_seconds == 45.0


def test_load_settings_defaults_paths(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    settings = load_settings()
    assert settings.db_path == tmp_path / ".SherpaMind" / "private" / "sherpamind.sqlite3"
    assert settings.watch_state_path == tmp_path / ".SherpaMind" / "private" / "watch_state.json"


def test_load_settings_reads_seed_controls(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("SHERPAMIND_SEED_PAGE_SIZE", "50")
    monkeypatch.setenv("SHERPAMIND_SEED_MAX_PAGES", "3")
    settings = load_settings()
    assert settings.seed_page_size == 50
    assert settings.seed_max_pages == 3
