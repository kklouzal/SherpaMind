from __future__ import annotations

from dataclasses import dataclass

from .settings import Settings
from .db import initialize_db


@dataclass
class WatchResult:
    status: str
    message: str


def watch_new_tickets(settings: Settings) -> WatchResult:
    initialize_db(settings.db_path)
    if not settings.base_url or not settings.api_key:
        return WatchResult(
            status="needs_config",
            message=(
                "Watcher scaffold is ready, but live polling is blocked until SHERPADESK_BASE_URL and "
                "SHERPADESK_API_KEY are configured and the ticket-listing contract is verified."
            ),
        )
    if not settings.notify_channel:
        return WatchResult(
            status="needs_notify_target",
            message=(
                "Watcher scaffold is ready and API config is present, but no notification destination is configured yet "
                "for new-ticket alerts."
            ),
        )
    return WatchResult(
        status="stub",
        message=(
            "Watcher preconditions are mostly in place. The next step is implementing conservative new-ticket polling, "
            "state tracking, and alert payload generation."
        ),
    )
