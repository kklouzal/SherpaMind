from pathlib import Path

from sherpamind.db import initialize_db
from sherpamind.sync_state import get_sync_state, set_sync_state


def test_sync_state_roundtrip(tmp_path: Path) -> None:
    db = tmp_path / 'sherpamind.sqlite3'
    initialize_db(db)
    assert get_sync_state(db, 'tickets.updated_after') is None
    set_sync_state(db, 'tickets.updated_after', '2026-03-19T00:00:00Z')
    assert get_sync_state(db, 'tickets.updated_after') == '2026-03-19T00:00:00Z'
