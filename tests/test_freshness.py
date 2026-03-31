from pathlib import Path

from sherpamind.db import initialize_db, start_ingest_run, finish_ingest_run
from sherpamind.freshness import get_sync_freshness


def test_get_sync_freshness_returns_latest_runs_and_summary(tmp_path: Path) -> None:
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
