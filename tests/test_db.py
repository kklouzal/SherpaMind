from pathlib import Path

from sherpamind.db import initialize_db, connect


def test_initialize_db_creates_core_tables(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    with connect(db) as conn:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    names = {row['name'] for row in rows}
    assert 'tickets' in names
    assert 'accounts' in names
    assert 'users' in names
    assert 'ticket_comments' in names
    assert 'sync_state' in names
