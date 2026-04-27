from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .client import SherpaDeskClient
from .db import connect, initialize_db, now_iso, upsert_ticket_details, upsert_tickets
from .documents import materialize_ticket_documents
from .settings import Settings
from .time_utils import parse_sherpadesk_timestamp


@dataclass(frozen=True)
class StaleUnconfirmedCandidate:
    ticket_id: str
    ticket_number: str | None
    subject: str | None
    status: str | None
    closed_at: str
    closed_days: int
    is_confirmed: Any


@dataclass(frozen=True)
class StaleUnconfirmedResult:
    status: str
    mode: str
    min_closed_days: int
    cutoff_at: str
    candidate_count: int
    updated_count: int
    failed_count: int
    candidates: list[dict[str, Any]]
    updates: list[dict[str, Any]]
    failures: list[dict[str, Any]]


def _confirmation_sql_expr() -> str:
    return "COALESCE(json_extract(td.raw_json, '$.is_confirmed'), json_extract(t.raw_json, '$.is_confirmed'))"


def _closed_days(closed_at: str, *, now: datetime) -> int:
    closed_dt = parse_sherpadesk_timestamp(closed_at)
    if closed_dt is None:
        return 0
    return max((now - closed_dt).days, 0)


def list_stale_unconfirmed_closed_tickets(
    db_path: Path,
    *,
    min_closed_days: int = 365,
    limit: int = 100,
    now: datetime | None = None,
) -> list[StaleUnconfirmedCandidate]:
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=min_closed_days)
    cutoff_sql = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    confirmation_expr = _confirmation_sql_expr()
    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT t.id,
                   json_extract(t.raw_json, '$.ticket_number') AS ticket_number,
                   t.subject,
                   t.status,
                   t.closed_at,
                   {confirmation_expr} AS is_confirmed
            FROM tickets t
            LEFT JOIN ticket_details td ON td.ticket_id = t.id
            WHERE lower(COALESCE(t.status, '')) = 'closed'
              AND t.closed_at IS NOT NULL
              AND julianday(REPLACE(substr(t.closed_at, 1, 19), 'T', ' ')) <= julianday(?)
              AND lower(CAST({confirmation_expr} AS TEXT)) IN ('0', 'false')
            ORDER BY julianday(REPLACE(substr(t.closed_at, 1, 19), 'T', ' ')) ASC, t.id ASC
            LIMIT ?
            """,
            (cutoff_sql, limit),
        ).fetchall()
    return [
        StaleUnconfirmedCandidate(
            ticket_id=str(row["id"]),
            ticket_number=str(row["ticket_number"]) if row["ticket_number"] is not None else None,
            subject=row["subject"],
            status=row["status"],
            closed_at=row["closed_at"],
            closed_days=_closed_days(row["closed_at"], now=now),
            is_confirmed=row["is_confirmed"],
        )
        for row in rows
    ]


def _candidate_to_dict(candidate: StaleUnconfirmedCandidate) -> dict[str, Any]:
    return {
        "ticket_id": candidate.ticket_id,
        "ticket_number": candidate.ticket_number,
        "subject": candidate.subject,
        "status": candidate.status,
        "closed_at": candidate.closed_at,
        "closed_days": candidate.closed_days,
        "is_confirmed": candidate.is_confirmed,
    }


def _refresh_ticket_after_write(settings: Settings, client: SherpaDeskClient, ticket_id: str) -> dict[str, Any]:
    detail = client.get(f"tickets/{ticket_id}")
    if isinstance(detail, dict):
        synced_at = now_iso()
        upsert_tickets(settings.db_path, [detail], synced_at=synced_at)
        upsert_ticket_details(settings.db_path, [detail], synced_at=synced_at)
        materialize_ticket_documents(settings.db_path, ticket_ids=[ticket_id])
        return {"status": "ok", "refreshed": True}
    return {"status": "skipped", "refreshed": False, "reason": f"unexpected_detail_shape:{type(detail).__name__}"}


def confirm_stale_unconfirmed_closed_tickets(
    settings: Settings,
    *,
    client: SherpaDeskClient | None = None,
    apply: bool = False,
    min_closed_days: int = 365,
    limit: int = 25,
) -> StaleUnconfirmedResult:
    initialize_db(settings.db_path)
    now = datetime.now(timezone.utc)
    candidates = list_stale_unconfirmed_closed_tickets(
        settings.db_path,
        min_closed_days=min_closed_days,
        limit=limit,
        now=now,
    )
    cutoff_at = (now - timedelta(days=min_closed_days)).isoformat()
    candidate_dicts = [_candidate_to_dict(candidate) for candidate in candidates]
    if not apply or not candidates:
        return StaleUnconfirmedResult(
            status="ok",
            mode="dry_run" if not apply else "apply",
            min_closed_days=min_closed_days,
            cutoff_at=cutoff_at,
            candidate_count=len(candidates),
            updated_count=0,
            failed_count=0,
            candidates=candidate_dicts,
            updates=[],
            failures=[],
        )

    if not settings.api_key:
        return StaleUnconfirmedResult(
            status="needs_config",
            mode="apply",
            min_closed_days=min_closed_days,
            cutoff_at=cutoff_at,
            candidate_count=len(candidates),
            updated_count=0,
            failed_count=0,
            candidates=candidate_dicts,
            updates=[],
            failures=[{"message": "SHERPADESK_API_KEY is required for live write-back"}],
        )

    client = client or SherpaDeskClient(
        api_base_url=settings.api_base_url,
        api_key=settings.api_key,
        api_user=settings.api_user,
        org_key=settings.org_key,
        instance_key=settings.instance_key,
        timeout_seconds=settings.request_timeout_seconds,
        min_interval_seconds=settings.request_min_interval_seconds,
        request_tracking_db_path=settings.db_path,
    )
    updates: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for candidate in candidates:
        try:
            response = client.put(f"tickets/{candidate.ticket_id}", data={"is_confirmed": "true"})
            refresh = _refresh_ticket_after_write(settings, client, candidate.ticket_id)
            updates.append(
                {
                    "ticket_id": candidate.ticket_id,
                    "ticket_number": candidate.ticket_number,
                    "closed_days": candidate.closed_days,
                    "write_payload": {"is_confirmed": "true"},
                    "response_type": type(response).__name__,
                    "refresh": refresh,
                }
            )
        except Exception as exc:  # noqa: BLE001 - keep batch write-back moving and report per-ticket failures.
            failures.append(
                {
                    "ticket_id": candidate.ticket_id,
                    "ticket_number": candidate.ticket_number,
                    "closed_days": candidate.closed_days,
                    "error": type(exc).__name__,
                    "message": str(exc),
                }
            )
    return StaleUnconfirmedResult(
        status="ok" if not failures else "partial" if updates else "error",
        mode="apply",
        min_closed_days=min_closed_days,
        cutoff_at=cutoff_at,
        candidate_count=len(candidates),
        updated_count=len(updates),
        failed_count=len(failures),
        candidates=candidate_dicts,
        updates=updates,
        failures=failures,
    )
