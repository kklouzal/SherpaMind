from __future__ import annotations

from pathlib import Path
from typing import Any

from .client import SherpaDeskClient
from .db import connect, list_ticket_taxonomy_classes, now_iso, replace_ticket_taxonomy_classes
from .text_cleanup import normalize_metadata_label


def flatten_ticket_classes(classes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def visit(node: dict[str, Any], ancestors: list[str]) -> None:
        name = normalize_metadata_label(node.get("name")) or str(node.get("id") or "").strip()
        if not name:
            return
        path_parts = [*ancestors, name]
        row = dict(node)
        row["id"] = str(node["id"])
        row["parent_id"] = str(node["parent_id"]) if node.get("parent_id") not in (None, "") else None
        row["name"] = name
        row["path"] = " / ".join(path_parts)
        row["raw_json"] = node
        rows.append(row)
        for child in node.get("sub") or []:
            if isinstance(child, dict):
                visit(child, path_parts)

    for item in classes:
        if isinstance(item, dict):
            visit(item, [])
    return rows


def sync_ticket_classes(client: SherpaDeskClient, db_path: Path) -> dict[str, Any]:
    payload = client.list_ticket_classes()
    if not isinstance(payload, list):
        raise TypeError(f"Expected list response from classes/, got {type(payload).__name__}")
    rows = flatten_ticket_classes(payload)
    synced_at = now_iso()
    count = replace_ticket_taxonomy_classes(db_path, rows, synced_at=synced_at)
    leaf_count = sum(1 for row in rows if row.get("is_lastchild") is True)
    active_count = sum(1 for row in rows if row.get("is_active") is not False)
    return {
        "status": "ok",
        "synced_at": synced_at,
        "root_count": len(payload),
        "class_count": count,
        "leaf_count": leaf_count,
        "active_count": active_count,
    }


def get_ticket_class_report(db_path: Path, *, active_only: bool = False, leaves_only: bool = False) -> dict[str, Any]:
    rows = list_ticket_taxonomy_classes(db_path, active_only=active_only, leaves_only=leaves_only)
    return {
        "status": "ok",
        "class_count": len(rows),
        "classes": rows,
    }


def get_ticket_class_coverage(db_path: Path, *, limit: int = 25) -> dict[str, Any]:
    with connect(db_path) as conn:
        summary = dict(conn.execute(
            """
            SELECT
                COUNT(*) AS ticket_count,
                SUM(CASE WHEN json_extract(t.raw_json, '$.class_id') IS NOT NULL THEN 1 ELSE 0 END) AS class_id_rows,
                SUM(CASE WHEN json_extract(t.raw_json, '$.class_id') IS NOT NULL AND tc.id IS NOT NULL THEN 1 ELSE 0 END) AS mapped_class_rows,
                COUNT(DISTINCT json_extract(t.raw_json, '$.class_id')) AS distinct_class_ids,
                SUM(CASE WHEN json_extract(t.raw_json, '$.resolution_category_name') IS NOT NULL
                          AND trim(json_extract(t.raw_json, '$.resolution_category_name')) <> ''
                         THEN 1 ELSE 0 END) AS resolution_label_rows
            FROM tickets t
            LEFT JOIN ticket_taxonomy_classes tc
              ON tc.id = CAST(json_extract(t.raw_json, '$.class_id') AS TEXT)
            """
        ).fetchone())
        top_classes = [dict(row) for row in conn.execute(
            """
            SELECT COALESCE(NULLIF(tc.path, ''), NULLIF(json_extract(t.raw_json, '$.class_name'), ''), 'unknown') AS class_path,
                   CAST(json_extract(t.raw_json, '$.class_id') AS TEXT) AS class_id,
                   COUNT(*) AS ticket_count
            FROM tickets t
            LEFT JOIN ticket_taxonomy_classes tc
              ON tc.id = CAST(json_extract(t.raw_json, '$.class_id') AS TEXT)
            GROUP BY class_path, class_id
            ORDER BY ticket_count DESC, class_path COLLATE NOCASE
            LIMIT ?
            """,
            (limit,),
        ).fetchall()]
    return {
        "status": "ok",
        "summary": summary,
        "top_classes": top_classes,
    }
