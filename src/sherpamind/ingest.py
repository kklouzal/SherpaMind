from __future__ import annotations

from dataclasses import dataclass

from .db import initialize_db
from .settings import Settings


@dataclass
class SeedResult:
    status: str
    message: str


@dataclass
class DeltaSyncResult:
    status: str
    message: str


def _missing_api_config_message() -> str:
    return (
        "Database initialized, but live SherpaDesk ingest is still blocked until SHERPADESK_API_KEY is configured, "
        "organization/instance access is confirmed, and the real endpoint contract is verified."
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
    return SeedResult(
        status="stub",
        message=(
            "Config is present and the DB is initialized. The next implementation step is wiring verified SherpaDesk "
            "seed endpoints into the ingest pipeline carefully and conservatively."
        ),
    )


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
            "Config is present and the DB is initialized. Delta sync still needs verified SherpaDesk changed-record "
            "behavior before live implementation."
        ),
    )
