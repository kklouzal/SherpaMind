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
    )


def _candidate_ticket_ids(db_path, limit: int) -> list[str]:
    from .db import connect

    with connect(db_path) as conn:
        rows = conn.execute(
            """
            WITH prioritized AS (
                SELECT id,
                       CASE
                           WHEN status = 'Open' THEN 0
                           WHEN status = 'Closed'
                                AND closed_at IS NOT NULL
                                AND julianday(REPLACE(substr(closed_at, 1, 19), 'T', ' ')) >= julianday('now', '-7 days') THEN 1
                           ELSE 2
                       END AS bucket,
                       COALESCE(updated_at, created_at) AS activity_at
                FROM tickets
            )
            SELECT id
            FROM prioritized
            ORDER BY bucket ASC, activity_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [str(row['id']) for row in rows]


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
        ticket_ids = _candidate_ticket_ids(settings.db_path, limit=limit)
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
            'synced_at': synced_at,
            'materialized_documents': doc_stats['document_count'] if doc_stats else None,
        }
        set_json_state(settings.db_path, 'enrichment.priority_tickets.last_state', stats)
        finish_ingest_run(settings.db_path, run_id, status='success', notes=json.dumps(stats, sort_keys=True))
        return EnrichmentResult(status='ok', message='Priority ticket detail enrichment completed.', stats=stats)
    except Exception as exc:
        finish_ingest_run(settings.db_path, run_id, status='failed', notes=f'{type(exc).__name__}: {exc}')
        raise
