from pathlib import Path

from sherpamind.enrichment import enrich_priority_ticket_details
from sherpamind.settings import Settings
from sherpamind.db import initialize_db, upsert_ticket_details, upsert_tickets, connect


class FakeClient:
    def get(self, path, params=None):
        ticket_id = path.split('/')[-1]
        return {
            'id': int(ticket_id),
            'subject': f'Ticket {ticket_id}',
            'status': 'Open',
            'created_time': '2026-03-18T01:00:00Z',
            'updated_time': '2026-03-19T01:00:00Z',
            'ticketlogs': [{'id': int(ticket_id) * 10, 'log_type': 'Initial Post', 'plain_note': 'hello', 'record_date': '2026-03-18T01:00:00Z'}],
            'timelogs': [],
            'attachments': [],
        }


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        api_base_url='https://api.sherpadesk.com',
        api_key='secret',
        api_user=None,
        org_key='org',
        instance_key='inst',
        db_path=tmp_path / 'sherpamind.sqlite3',
        watch_state_path=tmp_path / 'watch_state.json',
        notify_channel=None,
        request_min_interval_seconds=0,
        request_timeout_seconds=30,
        seed_page_size=100,
        seed_max_pages=None,
        hot_open_pages=5,
        warm_closed_pages=10,
        warm_closed_days=7,
        cold_closed_pages_per_run=2,
    )


def test_enrich_priority_ticket_details_populates_detail_tables(tmp_path: Path, monkeypatch) -> None:
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    upsert_tickets(settings.db_path, [
        {'id': 101, 'subject': 'Open A', 'status': 'Open', 'created_time': '2026-03-19T01:00:00Z', 'updated_time': '2026-03-19T02:00:00Z'},
        {'id': 102, 'subject': 'Closed B', 'status': 'Closed', 'created_time': '2026-03-18T01:00:00Z', 'updated_time': '2026-03-19T01:00:00Z', 'closed_time': '2999-03-18T01:00:00Z'},
    ])
    monkeypatch.setattr('sherpamind.enrichment._build_client', lambda settings: FakeClient())
    result = enrich_priority_ticket_details(settings, limit=2, materialize_docs=True)
    assert result.status == 'ok'
    with connect(settings.db_path) as conn:
        detail_count = conn.execute('SELECT COUNT(*) AS c FROM ticket_details').fetchone()['c']
        log_count = conn.execute('SELECT COUNT(*) AS c FROM ticket_logs').fetchone()['c']
        doc_count = conn.execute('SELECT COUNT(*) AS c FROM ticket_documents').fetchone()['c']
    assert detail_count == 2
    assert log_count == 2
    assert doc_count >= 2


def test_enrichment_prioritizes_unenriched_open_tickets(tmp_path: Path, monkeypatch) -> None:
    settings = make_settings(tmp_path)
    initialize_db(settings.db_path)
    upsert_tickets(settings.db_path, [
        {'id': 101, 'subject': 'Open A', 'status': 'Open', 'created_time': '2026-03-19T01:00:00Z', 'updated_time': '2026-03-19T02:00:00Z'},
        {'id': 102, 'subject': 'Open B', 'status': 'Open', 'created_time': '2026-03-19T01:00:00Z', 'updated_time': '2026-03-19T01:30:00Z'},
    ])
    upsert_ticket_details(settings.db_path, [{'id': 101, 'ticketlogs': [], 'timelogs': [], 'attachments': []}])
    seen = []

    class RecordingClient(FakeClient):
        def get(self, path, params=None):
            seen.append(path)
            return super().get(path, params=params)

    monkeypatch.setattr('sherpamind.enrichment._build_client', lambda settings: RecordingClient())
    enrich_priority_ticket_details(settings, limit=1, materialize_docs=False)
    assert seen == ['tickets/102']
