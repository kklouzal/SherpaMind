from pathlib import Path

from sherpamind.db import initialize_db, record_api_request_event, replace_ticket_document_chunks, replace_ticket_documents
from sherpamind.service_runtime import run_pending_tasks
from sherpamind.settings import Settings
from sherpamind.vector_index import get_vector_index_status


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
    replace_ticket_documents(
        settings.db_path,
        [{
            'doc_id': 'ticket:101',
            'ticket_id': 101,
            'status': 'Open',
            'account': 'Acme',
            'user_name': 'Alice',
            'technician': 'Tech One',
            'updated_at': '2026-03-19T03:00:00Z',
            'text': 'Printer issue in office',
            'metadata': {'priority': 'High', 'category': 'Hardware'},
            'content_hash': 'doc-a',
        }],
        synced_at='2026-03-19T01:00:00Z',
    )
    replace_ticket_document_chunks(
        settings.db_path,
        [{
            'chunk_id': 'ticket:101:chunk:0',
            'doc_id': 'ticket:101',
            'ticket_id': 101,
            'chunk_index': 0,
            'text': 'Printer issue in office',
            'content_hash': 'chunk-a',
        }],
        synced_at='2026-03-19T01:00:00Z',
    )

    result = run_pending_tasks(settings)
    assert result['status'] == 'ok'
    by_task = {item['task']: item for item in result['results']}
    assert by_task['vector_refresh']['status'] == 'ok'
    assert by_task['runtime_status']['status'] == 'ok'
    assert by_task['doctor_marker']['status'] == 'ok'

    vector_status = get_vector_index_status(settings.db_path)
    assert vector_status['indexed_chunks'] == 1
    assert vector_status['missing_index_rows'] == 0
    assert vector_status['outdated_content_rows'] == 0

    runtime_status_path = tmp_path / '.SherpaMind' / 'public' / 'docs' / 'runtime' / 'status.md'
    assert runtime_status_path.exists()
    assert 'Vector index status' in runtime_status_path.read_text()
