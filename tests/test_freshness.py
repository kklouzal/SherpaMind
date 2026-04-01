import json
from pathlib import Path

from sherpamind.db import initialize_db, start_ingest_run, finish_ingest_run
from sherpamind.freshness import get_sync_freshness


def test_get_sync_freshness_returns_latest_runs_and_summary(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    db = tmp_path / 'sherpamind.sqlite3'
    initialize_db(db)

    hot_run_id = start_ingest_run(db, mode='sync_hot_open', notes='{"pages": 3}')
    finish_ingest_run(db, hot_run_id, status='success', notes='{"pages": 5, "synced_at": "2026-03-19T03:00:00Z"}')

    warm_run_id = start_ingest_run(db, mode='sync_warm_closed', notes='warm test')
    finish_ingest_run(db, warm_run_id, status='error', notes='timeout')

    freshness = get_sync_freshness(db)

    assert freshness['summary']['lane_count'] == 5
    assert freshness['summary']['healthy_lanes'] >= 1
    assert freshness['summary']['overall_status'] in {'critical', 'degraded', 'stale', 'active', 'healthy'}

    hot = freshness['sync_hot_open']
    assert hot['latest_run']['status'] == 'success'
    assert hot['latest_run_notes_json']['pages'] == 5
    assert hot['success_count'] == 1
    assert hot['last_success_finished_at'] == hot['latest_run']['finished_at']
    assert hot['freshness_status'] in {'healthy', 'stale', 'critical'}

    warm = freshness['sync_warm_closed']
    assert warm['latest_run']['status'] == 'error'
    assert warm['error_count'] == 1
    assert warm['consecutive_non_success_runs'] == 1
    assert warm['freshness_status'] == 'error'

    assert freshness['lanes']['sync_hot_open']['mode'] == 'sync_hot_open'
    assert freshness['lanes']['seed']['freshness_status'] == 'missing'


def test_get_sync_freshness_uses_service_state_when_ingest_runs_are_stale(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    db = tmp_path / 'sherpamind.sqlite3'
    initialize_db(db)

    hot_run_id = start_ingest_run(db, mode='sync_hot_open', notes='old')
    finish_ingest_run(db, hot_run_id, status='success', notes='old success')
    from sherpamind.db import connect
    with connect(db) as conn:
        conn.execute("UPDATE ingest_runs SET started_at = '2026-03-01T00:00:00+00:00', finished_at = '2026-03-01T00:10:00+00:00' WHERE id = ?", (hot_run_id,))
        conn.commit()

    state_path = tmp_path / '.SherpaMind' / 'private' / 'state' / 'service-state.json'
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps({
        'tasks': {
            'hot_open': {
                'last_status': 'ok',
                'last_run_at': '2026-04-01T15:22:47+00:00',
            }
        }
    }))

    freshness = get_sync_freshness(db)
    hot = freshness['sync_hot_open']
    assert hot['service_state_used_for_freshness'] is True
    assert hot['freshness_status'] in {'healthy', 'stale'}
    assert hot['service_state']['task_name'] == 'hot_open'
