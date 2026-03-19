from __future__ import annotations

from dataclasses import dataclass
import json

from .client import SherpaDeskClient
from .db import (
    finish_ingest_run,
    initialize_db,
    now_iso,
    start_ingest_run,
    upsert_accounts,
    upsert_tickets,
    upsert_technicians,
    upsert_users,
)
from .settings import Settings
from .sync_state import set_sync_state


@dataclass
class SeedResult:
    status: str
    message: str
    stats: dict | None = None


@dataclass
class DeltaSyncResult:
    status: str
    message: str


def _missing_api_config_message() -> str:
    return (
        "Database initialized, but live SherpaDesk ingest is still blocked until SHERPADESK_API_KEY is configured, "
        "organization/instance access is confirmed, and the real endpoint contract is verified."
    )


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


def seed_all(settings: Settings) -> SeedResult:
    initialize_db(settings.db_path)
    if not settings.api_key:
        return SeedResult(status="needs_config", message=_missing_api_config_message())
    if not settings.org_key or not settings.instance_key:
        return SeedResult(
            status="needs_org_context",
            message=(
                "API token is present, but SHERPADESK_ORG_KEY and SHERPADESK_INSTANCE_KEY are still missing. "
                "Run organization discovery first, then wire the real seed endpoints."
            ),
        )

    run_id = start_ingest_run(
        settings.db_path,
        mode="seed",
        notes=f"page_size={settings.seed_page_size}, max_pages={settings.seed_max_pages}",
    )
    try:
        client = _build_client(settings)
        synced_at = now_iso()
        accounts = client.list_paginated("accounts", page_size=settings.seed_page_size, max_pages=settings.seed_max_pages)
        upsert_accounts(settings.db_path, accounts, synced_at=synced_at)

        users = client.list_paginated("users", page_size=settings.seed_page_size, max_pages=settings.seed_max_pages)
        upsert_users(settings.db_path, users, synced_at=synced_at)

        technicians = client.list_paginated("technicians", page_size=settings.seed_page_size, max_pages=settings.seed_max_pages)
        upsert_technicians(settings.db_path, technicians, synced_at=synced_at)

        tickets = client.list_paginated("tickets", page_size=settings.seed_page_size, max_pages=settings.seed_max_pages)
        upsert_tickets(settings.db_path, tickets, synced_at=synced_at)

        stats = {
            "accounts": len(accounts),
            "users": len(users),
            "technicians": len(technicians),
            "tickets": len(tickets),
            "synced_at": synced_at,
            "page_size": settings.seed_page_size,
            "max_pages": settings.seed_max_pages,
        }
        set_sync_state(settings.db_path, "seed.last_success_at", synced_at)
        set_sync_state(settings.db_path, "seed.last_stats", json.dumps(stats, sort_keys=True))
        finish_ingest_run(settings.db_path, run_id, status="success", notes=json.dumps(stats, sort_keys=True))
        return SeedResult(
            status="ok",
            message="Initial seed completed for accounts, users, technicians, and tickets.",
            stats=stats,
        )
    except Exception as exc:
        finish_ingest_run(settings.db_path, run_id, status="failed", notes=f"{type(exc).__name__}: {exc}")
        raise


def sync_delta(settings: Settings) -> DeltaSyncResult:
    initialize_db(settings.db_path)
    if not settings.api_key:
        return DeltaSyncResult(status="needs_config", message=_missing_api_config_message())
    if not settings.org_key or not settings.instance_key:
        return DeltaSyncResult(
            status="needs_org_context",
            message=(
                "API token is present, but SHERPADESK_ORG_KEY and SHERPADESK_INSTANCE_KEY are still missing. "
                "Run organization discovery first, then verify changed-record behavior before enabling delta sync."
            ),
        )
    return DeltaSyncResult(
        status="stub",
        message=(
            "Delta sync still needs verified changed-record/update-field behavior. Seed is now the real implemented path; "
            "delta should be built next on top of observed live update semantics."
        ),
    )
