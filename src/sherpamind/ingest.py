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
        "Database initialized, but live SherpaDesk ingest is still blocked until "
        "SHERPADESK_BASE_URL and SHERPADESK_API_KEY are configured and the real auth/header contract is verified."
    )


def seed_all(settings: Settings) -> SeedResult:
    initialize_db(settings.db_path)
    if not settings.base_url or not settings.api_key:
        return SeedResult(status="needs_config", message=_missing_api_config_message())
    return SeedResult(
        status="stub",
        message=(
            "Config is present and the DB is initialized. The next implementation step is wiring verified SherpaDesk "
            "seed endpoints into the ingest pipeline carefully and conservatively."
        ),
    )


def sync_delta(settings: Settings) -> DeltaSyncResult:
    initialize_db(settings.db_path)
    if not settings.base_url or not settings.api_key:
        return DeltaSyncResult(status="needs_config", message=_missing_api_config_message())
    return DeltaSyncResult(
        status="stub",
        message=(
            "Config is present and the DB is initialized. Delta sync still needs verified SherpaDesk changed-record "
            "behavior before live implementation."
        ),
    )
