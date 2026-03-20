from __future__ import annotations

from dataclasses import dataclass
import json

from .client import SherpaDeskClient
from .db import finish_ingest_run, initialize_db, now_iso, start_ingest_run, upsert_ticket_details, upsert_tickets
from .documents import materialize_ticket_documents
from .settings import Settings
from .sync_state import set_json_state


@dataclass
class EnrichmentResult:
    status: str
    message: str
    stats: dict | None = None


def _build_client(settings: Settings) -> SherpaDeskClient:
    assert settings.api_key is not None
    return SherpaDeskClient(
        api_base_url=settings.api_base_url,
        api_key=settings.api_key,
        api_user=settings.api_user,
        org_key=settings.org_key,
        instance_key=settings.instance_key,
        timeout_seconds=settings.request_timeout_seconds,
        min_interval_seconds=settings.request_min_interval_seconds,
        request_tracking_db_path=settings.db_path,
    )


def _candidate_ticket_rows(db_path, limit: int) -> list[dict]:
    from .db import connect

    with connect(db_path) as conn:
        rows = conn.execute(
            """
            WITH prioritized AS (
                SELECT t.id,
                       CASE
                           WHEN t.status = 'Open' THEN 0
                           WHEN t.status = 'Closed'
                                AND t.closed_at IS NOT NULL
                                AND julianday(REPLACE(substr(t.closed_at, 1, 19), 'T', ' ')) >= julianday('now', '-7 days') THEN 1
                           ELSE 2
                       END AS bucket,
                       CASE WHEN td.ticket_id IS NULL THEN 0 ELSE 1 END AS has_detail,
                       COALESCE(t.updated_at, t.created_at) AS activity_at
                FROM tickets t
                LEFT JOIN ticket_details td ON td.ticket_id = t.id
            )
            SELECT id, bucket, has_detail, activity_at
            FROM prioritized
            ORDER BY bucket ASC, has_detail ASC, activity_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def enrich_priority_ticket_details(settings: Settings, limit: int = 50, materialize_docs: bool = True) -> EnrichmentResult:
    initialize_db(settings.db_path)
    if not settings.api_key or not settings.org_key or not settings.instance_key:
        return EnrichmentResult(
            status='needs_config',
            message='Live API config is required before enrichment can run.',
        )

    run_id = start_ingest_run(settings.db_path, mode='enrich_priority_ticket_details', notes=f'limit={limit}')
    try:
        client = _build_client(settings)
        candidates = _candidate_ticket_rows(settings.db_path, limit=limit)
        ticket_ids = [str(row['id']) for row in candidates]
        details = []
        synced_at = now_iso()
        for ticket_id in ticket_ids:
            detail = client.get(f'tickets/{ticket_id}')
            if isinstance(detail, dict):
                details.append(detail)
        upsert_tickets(settings.db_path, details, synced_at=synced_at)
        upsert_ticket_details(settings.db_path, details, synced_at=synced_at)
        doc_stats = materialize_ticket_documents(settings.db_path, limit=None) if materialize_docs else None
        stats = {
            'candidate_ticket_count': len(ticket_ids),
            'enriched_ticket_count': len(details),
            'unenriched_candidates': sum(1 for row in candidates if row['has_detail'] == 0),
            'open_candidates': sum(1 for row in candidates if row['bucket'] == 0),
            'warm_candidates': sum(1 for row in candidates if row['bucket'] == 1),
            'cold_candidates': sum(1 for row in candidates if row['bucket'] == 2),
            'synced_at': synced_at,
            'materialized_documents': doc_stats['document_count'] if doc_stats else None,
        }
        set_json_state(settings.db_path, 'enrichment.priority_tickets.last_state', stats)
        finish_ingest_run(settings.db_path, run_id, status='success', notes=json.dumps(stats, sort_keys=True))
        return EnrichmentResult(status='ok', message='Priority ticket detail enrichment completed.', stats=stats)
    except Exception as exc:
        finish_ingest_run(settings.db_path, run_id, status='failed', notes=f'{type(exc).__name__}: {exc}')
        raise
