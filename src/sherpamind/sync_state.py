from __future__ import annotations

from datetime import datetime, timezone
from .db import connect
from pathlib import Path


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_sync_state(db_path: Path, key: str) -> str | None:
    with connect(db_path) as conn:
        row = conn.execute('SELECT value FROM sync_state WHERE key = ?', (key,)).fetchone()
        return row['value'] if row else None


def set_sync_state(db_path: Path, key: str, value: str) -> None:
    with connect(db_path) as conn:
        conn.execute(
            '''
            INSERT INTO sync_state(key, value, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            ''',
            (key, value, now_iso())
        )
        conn.commit()
