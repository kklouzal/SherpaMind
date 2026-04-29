import json
import time
from pathlib import Path

from sherpamind.cli import _json_ready

from sherpamind.db import initialize_db, record_api_request_event, replace_ticket_document_chunks, replace_ticket_documents, start_ingest_run, upsert_ticket_details, upsert_tickets
from sherpamind.service_runtime import run_pending_tasks
from sherpamind.settings import Settings
from sherpamind.vector_index import build_vector_index, get_vector_index_status


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        api_base_url='https://api.sherpadesk.com',
        api_key=None,
        api_user=None,
        org_key=None,
        instance_key=None,
        db_path=tmp_path / '.SherpaMind' / 'private' / 'data' / 'sherpamind.sqlite3',
        watch_state_path=tmp_path / '.SherpaMind' / 'private' / 'state' / 'watch_state.json',
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
        service_hot_open_every_seconds=999999,
        service_warm_closed_every_seconds=999999,
        service_cold_closed_every_seconds=999999,
        service_enrichment_every_seconds=999999,
        service_public_snapshot_every_seconds=999999,
        service_vector_refresh_every_seconds=0,
        service_doctor_every_seconds=0,
        service_enrichment_limit=25,
        service_cold_bootstrap_every_seconds=1800,
        service_enrichment_bootstrap_every_seconds=900,
        service_enrichment_bootstrap_limit=240,
        cold_closed_bootstrap_pages_per_run=10,
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
    state_path = tmp_path / '.SherpaMind' / 'private' / 'state' / 'service-state.json'
    assert state_path.exists()
    state = json.loads(state_path.read_text())
    assert state['loop_status'] == 'idle'
    assert state['last_loop_started_at']


def test_run_pending_tasks_skips_when_lock_is_held(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    from sherpamind.service_runtime import _task_run_lock
    with _task_run_lock(wait=False):
        result = run_pending_tasks(settings)
    assert result['status'] == 'skipped'
    assert result['reason'] == 'task_runner_already_active'


def test_run_pending_tasks_cleans_stale_ingest_rows(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    run_id = start_ingest_run(settings.db_path, mode='enrich_priority_ticket_details', notes='limit=25')
    with settings.db_path.open('rb+'):
        pass
    from sherpamind.db import connect
    with connect(settings.db_path) as conn:
        conn.execute("UPDATE ingest_runs SET started_at = '2026-03-01T00:00:00+00:00' WHERE id = ?", (run_id,))
        conn.commit()

    result = run_pending_tasks(settings)
    assert result['cleaned_stale_ingest_runs'] >= 1


def test_json_ready_normalizes_dataclasses(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    payload = _json_ready(run_pending_tasks(settings))
    json.dumps(payload)
    hot_open = next(item for item in payload['results'] if item['task'] == 'hot_open')
    watch_result, sync_result = hot_open['result']
    assert watch_result['status'] == 'needs_config'
    assert sync_result['status'] == 'needs_config'


def test_run_pending_tasks_prunes_old_request_events(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    settings = make_settings(tmp_path)
    settings = Settings(**{**settings.__dict__, 'api_request_log_retention_days': 0})
    initialize_db(settings.db_path)
    record_api_request_event(settings.db_path, method='GET', path='tickets', status_code=200, outcome='http_response')
    result = run_pending_tasks(settings)
    assert result['pruned_request_events'] >= 1


def test_run_pending_tasks_marks_cold_bootstrap_complete_when_closed_detail_catches_up(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    upsert_tickets(
        settings.db_path,
        [
            {'id': 1, 'subject': 'Cold A', 'status': 'Closed', 'created_time': '2026-03-01T01:00:00Z', 'updated_time': '2026-03-01T02:00:00Z', 'closed_time': '2026-03-01T02:00:00Z'},
            {'id': 2, 'subject': 'Cold B', 'status': 'Closed', 'created_time': '2026-03-02T01:00:00Z', 'updated_time': '2026-03-02T02:00:00Z', 'closed_time': '2026-03-02T02:00:00Z'},
        ],
        synced_at='2026-03-03T00:00:00Z',
    )
    upsert_ticket_details(
        settings.db_path,
        [
            {'id': 1, 'subject': 'Cold A', 'status': 'Closed', 'created_time': '2026-03-01T01:00:00Z', 'updated_time': '2026-03-01T02:00:00Z', 'closed_time': '2026-03-01T02:00:00Z'},
            {'id': 2, 'subject': 'Cold B', 'status': 'Closed', 'created_time': '2026-03-02T01:00:00Z', 'updated_time': '2026-03-02T02:00:00Z', 'closed_time': '2026-03-02T02:00:00Z'},
        ],
        synced_at='2026-03-03T00:00:00Z',
    )
    (tmp_path / '.SherpaMind' / 'private' / 'state' / 'service-state.json').parent.mkdir(parents=True, exist_ok=True)
    from sherpamind.sync_state import set_json_state, get_json_state
    set_json_state(settings.db_path, 'sync.cold_closed.last_state', {'next_page': 0, 'completed_cycles': 1})

    result = run_pending_tasks(settings)
    assert result['cold_bootstrap']['bootstrap_complete'] is True
    bootstrap_state = get_json_state(settings.db_path, 'service.cold_bootstrap')
    assert bootstrap_state['bootstrap_complete'] is True
    assert bootstrap_state['bootstrap_completed_at']


def test_run_pending_tasks_skips_remote_api_tasks_during_auth_failure_cooldown(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    settings = make_settings(tmp_path)
    settings = Settings(
        **{
            **settings.__dict__,
            'service_hot_open_every_seconds': 0,
        }
    )
    initialize_db(settings.db_path)
    for _ in range(20):
        record_api_request_event(
            settings.db_path,
            method='GET',
            path='tickets',
            status_code=404,
            outcome='http_response',
            extra={
                'detail': "Client error '404 User with this token was not found.' for url 'https://api.sherpadesk.com/tickets'",
                'response_body_preview': 'User with this token was not found.',
            },
        )

    result = run_pending_tasks(settings)
    assert result['status'] == 'ok'
    assert result['remote_api_guard']['active'] is True
    assert result['remote_api_guard']['reason'] == 'Invalid or unknown API token'
    by_task = {item['task']: item for item in result['results']}
    assert by_task['hot_open']['status'] == 'skipped'
    assert by_task['hot_open']['reason'].startswith('remote_api_cooldown cause=Invalid or unknown API token')
    assert by_task['vector_refresh']['status'] == 'ok'
    assert by_task['runtime_status']['status'] == 'ok'



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
    runtime_status = runtime_status_path.read_text()
    assert 'Vector index status' in runtime_status
    assert 'API status breakdown' in runtime_status
    assert 'API top failing paths' in runtime_status


def test_run_pending_tasks_forces_local_retrieval_repair_when_materialization_is_stale(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    settings = make_settings(tmp_path)
    settings = Settings(
        **{
            **settings.__dict__,
            'service_vector_refresh_every_seconds': 999999,
            'service_doctor_every_seconds': 999999,
        }
    )
    initialize_db(settings.db_path)
    upsert_tickets(
        settings.db_path,
        [{
            'id': 999,
            'account_id': 1,
            'user_id': 2,
            'tech_id': 3,
            'subject': 'Stale printer issue',
            'status': 'Open',
            'priority_name': 'High',
            'creation_category_name': 'Hardware',
            'created_time': '2026-03-19T01:00:00Z',
            'updated_time': '2026-03-19T03:00:00Z',
            'account_name': 'Acme',
            'user_name': 'Alice',
            'tech_name': 'Tech One',
            'plain_initial_post': 'Printer is still broken',
        }],
        synced_at='2026-03-19T01:00:00Z',
    )
    replace_ticket_documents(
        settings.db_path,
        [{
            'doc_id': 'ticket:999',
            'ticket_id': 999,
            'status': 'Open',
            'account': 'Acme',
            'user_name': 'Alice',
            'technician': 'Tech One',
            'updated_at': '2026-03-19T03:00:00Z',
            'text': 'stale doc',
            'metadata': {'materialization_version': 3},
            'content_hash': 'doc-stale',
        }],
        synced_at='2026-03-19T01:00:00Z',
    )
    replace_ticket_document_chunks(
        settings.db_path,
        [{
            'chunk_id': 'ticket:999:chunk:0',
            'doc_id': 'ticket:999',
            'ticket_id': 999,
            'chunk_index': 0,
            'text': 'stale doc',
            'content_hash': 'chunk-stale',
        }],
        synced_at='2026-03-19T01:00:00Z',
    )

    service_state_path = tmp_path / '.SherpaMind' / 'private' / 'state' / 'service-state.json'
    service_state_path.parent.mkdir(parents=True, exist_ok=True)
    now = time.time()
    service_state_path.write_text(json.dumps({
        'started_at': '2026-03-19T00:00:00Z',
        'tasks': {
            'retrieval_artifacts': {'last_run_epoch': now},
            'public_snapshot': {'last_run_epoch': now},
            'vector_refresh': {'last_run_epoch': now},
            'runtime_status': {'last_run_epoch': now},
        },
    }))

    result = run_pending_tasks(settings)
    by_task = {item['task']: item for item in result['results']}
    assert by_task['retrieval_artifacts']['status'] == 'ok'
    assert by_task['retrieval_artifacts']['forced'] is True
    assert by_task['public_snapshot']['status'] == 'ok'
    assert by_task['public_snapshot']['forced'] is True
    assert by_task['vector_refresh']['status'] == 'ok'
    assert by_task['vector_refresh']['forced'] is True
    assert by_task['runtime_status']['status'] == 'ok'
    assert by_task['runtime_status']['forced'] is True

    vector_status = get_vector_index_status(settings.db_path)
    assert vector_status['indexed_chunks'] == 1
    assert vector_status['missing_index_rows'] == 0
    assert vector_status['outdated_content_rows'] == 0


def test_run_pending_tasks_forces_vector_repair_when_index_drifts(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    settings = make_settings(tmp_path)
    settings = Settings(
        **{
            **settings.__dict__,
            'service_vector_refresh_every_seconds': 999999,
            'service_doctor_every_seconds': 999999,
        }
    )
    initialize_db(settings.db_path)
    upsert_tickets(
        settings.db_path,
        [{
            'id': 101,
            'account_id': 1,
            'user_id': 2,
            'tech_id': 3,
            'subject': 'Printer issue',
            'status': 'Open',
            'priority_name': 'High',
            'creation_category_name': 'Hardware',
            'created_time': '2026-03-19T01:00:00Z',
            'updated_time': '2026-03-19T03:00:00Z',
            'account_name': 'Acme',
            'user_name': 'Alice',
            'tech_name': 'Tech One',
        }],
        synced_at='2026-03-19T01:00:00Z',
    )
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
            'metadata': {'materialization_version': 4, 'priority': 'High', 'category': 'Hardware'},
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
    build_vector_index(settings.db_path)
    replace_ticket_document_chunks(
        settings.db_path,
        [{
            'chunk_id': 'ticket:101:chunk:0',
            'doc_id': 'ticket:101',
            'ticket_id': 101,
            'chunk_index': 0,
            'text': 'Printer issue in office and warehouse',
            'content_hash': 'chunk-b',
        }],
        synced_at='2026-03-19T02:00:00Z',
    )

    service_state_path = tmp_path / '.SherpaMind' / 'private' / 'state' / 'service-state.json'
    service_state_path.parent.mkdir(parents=True, exist_ok=True)
    now = time.time()
    service_state_path.write_text(json.dumps({
        'started_at': '2026-03-19T00:00:00Z',
        'tasks': {
            'vector_refresh': {'last_run_epoch': now},
            'runtime_status': {'last_run_epoch': now},
        },
    }))

    result = run_pending_tasks(settings)
    by_task = {item['task']: item for item in result['results']}
    assert by_task['vector_refresh']['status'] == 'ok'
    assert by_task['vector_refresh']['forced'] is True
    assert by_task['runtime_status']['status'] == 'ok'
    assert by_task['runtime_status']['forced'] is True

    vector_status = get_vector_index_status(settings.db_path)
    assert vector_status['missing_index_rows'] == 0
    assert vector_status['outdated_content_rows'] == 0


def test_run_pending_tasks_uses_configured_hourly_budget_for_remote_gates(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    settings = make_settings(tmp_path)
    settings = Settings(
        **{
            **settings.__dict__,
            'api_hourly_limit': 200,
            'service_hot_open_every_seconds': 0,
            'service_vector_refresh_every_seconds': 0,
            'service_doctor_every_seconds': 0,
        }
    )
    initialize_db(settings.db_path)
    for _ in range(200):
        record_api_request_event(settings.db_path, method='GET', path='tickets', status_code=200, outcome='http_response')

    result = run_pending_tasks(settings)

    assert result['api_usage']['remaining_hourly_budget'] == 0
    assert result['budget_plan']['remaining_requests'] == 0
    by_task = {item['task']: item for item in result['results']}
    assert by_task['hot_open']['status'] == 'skipped'
    assert by_task['hot_open']['reason'].startswith('budget_exhausted')
    assert by_task['vector_refresh']['status'] == 'ok'
    assert by_task['runtime_status']['status'] == 'ok'
