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
    generated_at = datetime.now(timezone.utc).isoformat()

    dataset_summary = get_dataset_summary(db_path)
    status_counts = list_ticket_counts_by_status(db_path)
    open_ages = list_open_ticket_ages(db_path, limit=10)
    account_activity = list_recent_account_activity(db_path, days=7, limit=10)
    technician_load = list_technician_recent_load(db_path, days=7, limit=10)
    attachment_summary = list_ticket_attachment_summary(db_path, limit=10)
    recent_tickets = list_recent_tickets(db_path, limit=10)

    snapshot_path = paths.docs_root / "insight-snapshot.md"
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

    stale_open_path = paths.docs_root / "stale-open-tickets.md"
    stale_open_md = [
        "# SherpaMind Stale Open Tickets",
        "",
        f"Generated: `{generated_at}`",
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
        "- Derived from the canonical SQLite store under `.SherpaMind/private/`.",
    ]
    stale_open_path.write_text("\n".join(stale_open_md) + "\n")

    account_activity_path = paths.docs_root / "recent-account-activity.md"
    account_activity_md = [
        "# SherpaMind Recent Account Activity",
        "",
        f"Generated: `{generated_at}`",
        "",
        _markdown_table(account_activity, [
            ("account", "Account"),
            ("ticket_count", "Tickets"),
            ("open_count", "Open"),
            ("closed_count", "Closed"),
            ("latest_activity_at", "Latest Activity"),
        ]),
    ]
    account_activity_path.write_text("\n".join(account_activity_md) + "\n")

    technician_load_path = paths.docs_root / "recent-technician-load.md"
    technician_load_md = [
        "# SherpaMind Recent Technician Load",
        "",
        f"Generated: `{generated_at}`",
        "",
        _markdown_table(technician_load, [
            ("technician", "Technician"),
            ("ticket_count", "Tickets"),
            ("open_count", "Open"),
            ("closed_count", "Closed"),
            ("latest_activity_at", "Latest Activity"),
        ]),
    ]
    technician_load_path.write_text("\n".join(technician_load_md) + "\n")

    index_path = paths.docs_root / "index.md"
    index_md = [
        "# SherpaMind Public Docs Index",
        "",
        f"Generated: `{generated_at}`",
        "",
        "Available derived artifacts:",
        "- `insight-snapshot.md`",
        "- `stale-open-tickets.md`",
        "- `recent-account-activity.md`",
        "- `recent-technician-load.md`",
        "",
        "These are derived/public artifacts for OpenClaw-friendly access. Canonical truth remains in `.SherpaMind/private/`.",
    ]
    index_path.write_text("\n".join(index_md) + "\n")

    account_dir = paths.docs_root / "accounts"
    technician_dir = paths.docs_root / "technicians"
    account_dir.mkdir(parents=True, exist_ok=True)
    technician_dir.mkdir(parents=True, exist_ok=True)

    generated_files = [
        str(index_path),
        str(snapshot_path),
        str(stale_open_path),
        str(account_activity_path),
        str(technician_load_path),
    ]

    for account_name in [row.get('account') for row in account_activity[:5] if row.get('account') and row.get('account') != 'unknown']:
        summary = get_account_summary(db_path, account_name)
        if summary.get('status') != 'ok':
            continue
        safe_name = ''.join(ch if ch.isalnum() or ch in ('-', '_') else '_' for ch in account_name)[:80]
        path = account_dir / f"{safe_name}.md"
        lines = [
            f"# Account Summary: {summary['account']['name']}",
            "",
            f"Generated: `{generated_at}`",
            "",
            "## Stats",
            "",
            "```json",
            json.dumps(summary['stats'], indent=2),
            "```",
            "",
            "## Open tickets",
            "",
            _markdown_table(summary['open_tickets'], [
                ('id', 'Ticket ID'), ('subject', 'Subject'), ('priority', 'Priority'), ('updated_at', 'Updated')
            ]),
            "",
            "## Recent tickets",
            "",
            _markdown_table(summary['recent_tickets'], [
                ('id', 'Ticket ID'), ('subject', 'Subject'), ('status', 'Status'), ('updated_at', 'Updated')
            ]),
            "",
            "## Recent log types",
            "",
            _markdown_table(summary['recent_log_types'], [('log_type', 'Log Type'), ('log_count', 'Count')]),
        ]
        path.write_text('\n'.join(lines) + '\n')
        generated_files.append(str(path))

    for technician_name in [row.get('technician') for row in technician_load[:5] if row.get('technician') and row.get('technician') != 'unassigned']:
        summary = get_technician_summary(db_path, technician_name)
        if summary.get('status') != 'ok':
            continue
        safe_name = ''.join(ch if ch.isalnum() or ch in ('-', '_') else '_' for ch in technician_name)[:80]
        path = technician_dir / f"{safe_name}.md"
        lines = [
            f"# Technician Summary: {summary['technician']['display_name']}",
            "",
            f"Generated: `{generated_at}`",
            "",
            "## Stats",
            "",
            "```json",
            json.dumps(summary['stats'], indent=2),
            "```",
            "",
            "## Open tickets",
            "",
            _markdown_table(summary['open_tickets'], [
                ('id', 'Ticket ID'), ('subject', 'Subject'), ('priority', 'Priority'), ('updated_at', 'Updated')
            ]),
            "",
            "## Recent tickets",
            "",
            _markdown_table(summary['recent_tickets'], [
                ('id', 'Ticket ID'), ('subject', 'Subject'), ('status', 'Status'), ('updated_at', 'Updated')
            ]),
            "",
            "## Recent log types",
            "",
            _markdown_table(summary['recent_log_types'], [('log_type', 'Log Type'), ('log_count', 'Count')]),
        ]
        path.write_text('\n'.join(lines) + '\n')
        generated_files.append(str(path))

    return {
        "status": "ok",
        "output_path": str(snapshot_path),
        "generated_at": generated_at,
        "generated_files": generated_files,
    }
