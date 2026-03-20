from pathlib import Path

from sherpamind.db import initialize_db, record_api_request_event
from sherpamind.service_runtime import run_pending_tasks
from sherpamind.settings import Settings


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        api_base_url='https://api.sherpadesk.com',
        api_key=None,
        api_user=None,
        org_key=None,
        instance_key=None,
        db_path=tmp_path / '.SherpaMind' / 'private' / 'sherpamind.sqlite3',
        watch_state_path=tmp_path / '.SherpaMind' / 'private' / 'watch_state.json',
        notify_channel=None,
        request_min_interval_seconds=0,
        request_timeout_seconds=30,
        seed_page_size=100,
        seed_max_pages=None,
        hot_open_pages=5,
        warm_closed_pages=10,
        warm_closed_days=7,
        cold_closed_pages_per_run=2,
        service_hot_open_every_seconds=999999,
        service_warm_closed_every_seconds=999999,
        service_cold_closed_every_seconds=999999,
        service_enrichment_every_seconds=999999,
        service_public_snapshot_every_seconds=999999,
        service_vector_refresh_every_seconds=0,
        service_doctor_every_seconds=0,
        service_enrichment_limit=25,
        api_hourly_limit=600,
        api_budget_warn_ratio=0.7,
        api_budget_critical_ratio=0.85,
        api_request_log_retention_days=14,
    )


def test_run_pending_tasks_writes_service_state(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    result = run_pending_tasks(settings)
    assert result['status'] == 'ok'
    assert (tmp_path / '.SherpaMind' / 'private' / 'service-state.json').exists()


def test_run_pending_tasks_prunes_old_request_events(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    settings = make_settings(tmp_path)
    settings = Settings(**{**settings.__dict__, 'api_request_log_retention_days': 0})
    initialize_db(settings.db_path)
    record_api_request_event(settings.db_path, method='GET', path='tickets', status_code=200, outcome='http_response')
    result = run_pending_tasks(settings)
    assert result['pruned_request_events'] >= 1


def test_run_pending_tasks_builds_vector_index(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    result = run_pending_tasks(settings)
    assert result['status'] == 'ok'
