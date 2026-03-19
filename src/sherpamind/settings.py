from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    api_key: str | None
    api_user: str | None
    org_key: str | None
    instance_key: str | None
    db_path: Path
    watch_state_path: Path
    notify_channel: str | None
    request_min_interval_seconds: float
    request_timeout_seconds: float
    seed_page_size: int
    seed_max_pages: int | None
    hot_open_pages: int
    warm_closed_pages: int
    warm_closed_days: int
    cold_closed_pages_per_run: int


def load_settings() -> Settings:
    db_path = Path(os.getenv("SHERPAMIND_DB_PATH", "state/sherpamind.sqlite3"))
    watch_state_path = Path(os.getenv("SHERPAMIND_WATCH_STATE_PATH", "state/watch_state.json"))
    seed_max_pages_raw = os.getenv("SHERPAMIND_SEED_MAX_PAGES")
    return Settings(
        api_base_url=os.getenv("SHERPADESK_API_BASE_URL", "https://api.sherpadesk.com"),
        api_key=os.getenv("SHERPADESK_API_KEY"),
        api_user=os.getenv("SHERPADESK_API_USER"),
        org_key=os.getenv("SHERPADESK_ORG_KEY"),
        instance_key=os.getenv("SHERPADESK_INSTANCE_KEY"),
        db_path=db_path,
        watch_state_path=watch_state_path,
        notify_channel=os.getenv("SHERPAMIND_NOTIFY_CHANNEL"),
        request_min_interval_seconds=float(os.getenv("SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS", "8.0")),
        request_timeout_seconds=float(os.getenv("SHERPAMIND_REQUEST_TIMEOUT_SECONDS", "30.0")),
        seed_page_size=int(os.getenv("SHERPAMIND_SEED_PAGE_SIZE", "100")),
        seed_max_pages=int(seed_max_pages_raw) if seed_max_pages_raw else None,
        hot_open_pages=int(os.getenv("SHERPAMIND_HOT_OPEN_PAGES", "5")),
        warm_closed_pages=int(os.getenv("SHERPAMIND_WARM_CLOSED_PAGES", "10")),
        warm_closed_days=int(os.getenv("SHERPAMIND_WARM_CLOSED_DAYS", "7")),
        cold_closed_pages_per_run=int(os.getenv("SHERPAMIND_COLD_CLOSED_PAGES_PER_RUN", "2")),
    )
