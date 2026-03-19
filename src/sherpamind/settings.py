from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class Settings:
    base_url: str | None
    api_key: str | None
    api_user: str | None
    db_path: Path
    watch_state_path: Path
    notify_channel: str | None
    request_min_interval_seconds: float
    request_timeout_seconds: float


def load_settings() -> Settings:
    db_path = Path(os.getenv("SHERPAMIND_DB_PATH", "state/sherpamind.sqlite3"))
    watch_state_path = Path(os.getenv("SHERPAMIND_WATCH_STATE_PATH", "state/watch_state.json"))
    return Settings(
        base_url=os.getenv("SHERPADESK_BASE_URL"),
        api_key=os.getenv("SHERPADESK_API_KEY"),
        api_user=os.getenv("SHERPADESK_API_USER"),
        db_path=db_path,
        watch_state_path=watch_state_path,
        notify_channel=os.getenv("SHERPAMIND_NOTIFY_CHANNEL"),
        request_min_interval_seconds=float(os.getenv("SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS", "2.0")),
        request_timeout_seconds=float(os.getenv("SHERPAMIND_REQUEST_TIMEOUT_SECONDS", "30.0")),
    )
