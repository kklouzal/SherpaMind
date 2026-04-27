from pathlib import Path

from sherpamind.db import connect, initialize_db
from sherpamind.taxonomy import flatten_ticket_classes, sync_ticket_classes


class FakeClient:
    def list_ticket_classes(self):
        return [
            {
                "id": 1,
                "name": "Hardware",
                "parent_id": 0,
                "hierarchy_level": 0,
                "is_active": True,
                "sub": [
                    {
                        "id": 2,
                        "name": " Printer\n Scanner ",
                        "parent_id": 1,
                        "hierarchy_level": 1,
                        "is_lastchild": True,
                        "is_active": True,
                        "sub": None,
                    }
                ],
            }
        ]


def test_flatten_ticket_classes_preserves_paths() -> None:
    rows = flatten_ticket_classes(FakeClient().list_ticket_classes())
    assert [row["path"] for row in rows] == ["Hardware", "Hardware / Printer Scanner"]
    assert rows[1]["parent_id"] == "1"


def test_sync_ticket_classes_caches_flattened_taxonomy(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    initialize_db(db)
    result = sync_ticket_classes(FakeClient(), db)  # type: ignore[arg-type]
    assert result["root_count"] == 1
    assert result["class_count"] == 2
    assert result["leaf_count"] == 1
    with connect(db) as conn:
        rows = conn.execute("SELECT path FROM ticket_taxonomy_classes ORDER BY path").fetchall()
    assert [row["path"] for row in rows] == ["Hardware", "Hardware / Printer Scanner"]
