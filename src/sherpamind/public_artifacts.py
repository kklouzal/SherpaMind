from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from .analysis import (
    get_dataset_summary,
    list_open_ticket_ages,
    list_recent_account_activity,
    list_recent_tickets,
    list_technician_recent_load,
    list_ticket_attachment_summary,
    list_ticket_counts_by_status,
)
from .paths import ensure_path_layout


def _markdown_table(rows: list[dict], columns: list[tuple[str, str]]) -> str:
    if not rows:
        return "_No data available._"
    header = "| " + " | ".join(label for _, label in columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    body = []
    for row in rows:
        body.append("| " + " | ".join(str(row.get(key, "")) for key, _ in columns) + " |")
    return "\n".join([header, sep, *body])


def generate_public_snapshot(db_path: Path) -> dict:
    paths = ensure_path_layout()
    snapshot_path = paths.docs_root / "insight-snapshot.md"
    generated_at = datetime.now(timezone.utc).isoformat()

    dataset_summary = get_dataset_summary(db_path)
    status_counts = list_ticket_counts_by_status(db_path)
    open_ages = list_open_ticket_ages(db_path, limit=10)
    account_activity = list_recent_account_activity(db_path, days=7, limit=10)
    technician_load = list_technician_recent_load(db_path, days=7, limit=10)
    attachment_summary = list_ticket_attachment_summary(db_path, limit=10)
    recent_tickets = list_recent_tickets(db_path, limit=10)

    md = [
        "# SherpaMind Public Insight Snapshot",
        "",
        f"Generated: `{generated_at}`",
        "",
        "## Dataset summary",
        "",
        "```json",
        json.dumps(dataset_summary, indent=2),
        "```",
        "",
        "## Status counts",
        "",
        _markdown_table(status_counts, [("status", "Status"), ("ticket_count", "Ticket Count")]),
        "",
        "## Oldest open tickets",
        "",
        _markdown_table(open_ages, [
            ("id", "Ticket ID"),
            ("subject", "Subject"),
            ("account", "Account"),
            ("technician", "Technician"),
            ("age_days", "Age Days"),
            ("days_since_update", "Days Since Update"),
        ]),
        "",
        "## Recent account activity (7d)",
        "",
        _markdown_table(account_activity, [
            ("account", "Account"),
            ("ticket_count", "Tickets"),
            ("open_count", "Open"),
            ("closed_count", "Closed"),
            ("latest_activity_at", "Latest Activity"),
        ]),
        "",
        "## Recent technician load (7d)",
        "",
        _markdown_table(technician_load, [
            ("technician", "Technician"),
            ("ticket_count", "Tickets"),
            ("open_count", "Open"),
            ("closed_count", "Closed"),
            ("latest_activity_at", "Latest Activity"),
        ]),
        "",
        "## Attachment metadata summary",
        "",
        _markdown_table(attachment_summary, [
            ("ticket_id", "Ticket ID"),
            ("subject", "Subject"),
            ("attachment_count", "Attachments"),
            ("total_attachment_bytes", "Total Bytes"),
            ("latest_attachment_at", "Latest Attachment"),
        ]),
        "",
        "## Recent tickets",
        "",
        _markdown_table(recent_tickets, [
            ("id", "Ticket ID"),
            ("subject", "Subject"),
            ("status", "Status"),
            ("account", "Account"),
            ("technician", "Technician"),
            ("updated_at", "Updated"),
        ]),
        "",
        "## Notes",
        "",
        "- This file is a derived public artifact for OpenClaw-friendly consumption.",
        "- Canonical truth remains in `.SherpaMind/private/sherpamind.sqlite3`.",
        "- Attachment bodies are not downloaded by default; this snapshot reflects metadata only.",
    ]

    snapshot_path.write_text("\n".join(md) + "\n")
    return {
        "status": "ok",
        "output_path": str(snapshot_path),
        "generated_at": generated_at,
    }
