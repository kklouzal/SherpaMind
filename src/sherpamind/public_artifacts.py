from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from .analysis import (
    get_dataset_summary,
    get_enrichment_coverage,
    list_open_ticket_ages,
    list_recent_account_activity,
    list_recent_tickets,
    list_technician_recent_load,
    list_ticket_attachment_summary,
    list_ticket_counts_by_status,
)
from .freshness import get_sync_freshness
from .paths import ensure_path_layout
from .summaries import (
    get_account_summary,
    get_technician_summary,
    list_account_artifact_summaries,
    list_technician_artifact_summaries,
)


def _markdown_table(rows: list[dict], columns: list[tuple[str, str]]) -> str:
    if not rows:
        return "_No data available._"
    header = "| " + " | ".join(label for _, label in columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    body = [
        "| " + " | ".join(str(row.get(key, "")) for key, _ in columns) + " |"
        for row in rows
    ]
    return "\n".join([header, sep, *body])


def _safe_doc_name(name: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in name)[:80]


def generate_public_snapshot(db_path: Path) -> dict:
    paths = ensure_path_layout()
    generated_at = datetime.now(timezone.utc).isoformat()

    dataset_summary = get_dataset_summary(db_path)
    enrichment_coverage = get_enrichment_coverage(db_path)
    sync_freshness = get_sync_freshness(db_path)
    status_counts = list_ticket_counts_by_status(db_path)
    open_ages = list_open_ticket_ages(db_path, limit=10)
    account_activity = list_recent_account_activity(db_path, days=7, limit=10)
    technician_load = list_technician_recent_load(db_path, days=7, limit=10)
    attachment_summary = list_ticket_attachment_summary(db_path, limit=10)
    recent_tickets = list_recent_tickets(db_path, limit=10)
    account_summaries = list_account_artifact_summaries(db_path)
    technician_summaries = list_technician_artifact_summaries(db_path)

    account_dir = paths.docs_root / "accounts"
    technician_dir = paths.docs_root / "technicians"
    account_dir.mkdir(parents=True, exist_ok=True)
    technician_dir.mkdir(parents=True, exist_ok=True)

    snapshot_path = paths.docs_root / "insight-snapshot.md"
    snapshot_md = [
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
        "## Enrichment coverage",
        "",
        "```json",
        json.dumps(enrichment_coverage, indent=2),
        "```",
        "",
        "## Sync freshness",
        "",
        "```json",
        json.dumps(sync_freshness, indent=2),
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
        "## Retrieval metadata readiness",
        "",
        "```json",
        json.dumps(enrichment_coverage.get("metadata", {}), indent=2),
        "```",
        "",
        "## Account artifact coverage",
        "",
        _markdown_table(account_summaries[:10], [
            ("account", "Account"),
            ("total_tickets", "Tickets"),
            ("detail_tickets", "Detail Tickets"),
            ("log_tickets", "Log Tickets"),
            ("attachment_tickets", "Attachment Tickets"),
            ("document_tickets", "Document Tickets"),
            ("chunk_count", "Chunks"),
        ]),
        "",
        "## Technician artifact coverage",
        "",
        _markdown_table(technician_summaries[:10], [
            ("technician", "Technician"),
            ("total_tickets", "Tickets"),
            ("detail_tickets", "Detail Tickets"),
            ("log_tickets", "Log Tickets"),
            ("attachment_tickets", "Attachment Tickets"),
            ("document_tickets", "Document Tickets"),
            ("chunk_count", "Chunks"),
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
        "- Canonical truth remains in `.SherpaMind/private/data/sherpamind.sqlite3`.",
        "- Attachment bodies are not downloaded by default; this snapshot reflects metadata only.",
    ]
    snapshot_path.write_text("\n".join(snapshot_md) + "\n")

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
        "- Derived from the canonical SQLite store under `.SherpaMind/private/data/`.",
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

    account_index_path = account_dir / "index.md"
    account_index_md = [
        "# SherpaMind Account Artifact Index",
        "",
        f"Generated: `{generated_at}`",
        "",
        f"Total account docs: `{len(account_summaries)}`",
        "",
        _markdown_table(account_summaries, [
            ("account", "Account"),
            ("total_tickets", "Tickets"),
            ("open_tickets", "Open"),
            ("closed_tickets", "Closed"),
            ("detail_tickets", "Detail Tickets"),
            ("document_tickets", "Document Tickets"),
            ("chunk_count", "Chunks"),
            ("latest_activity_at", "Latest Activity"),
        ]),
        "",
        "These are derived per-account retrieval/support artifacts, not canonical truth.",
    ]
    account_index_path.write_text("\n".join(account_index_md) + "\n")

    technician_index_path = technician_dir / "index.md"
    technician_index_md = [
        "# SherpaMind Technician Artifact Index",
        "",
        f"Generated: `{generated_at}`",
        "",
        f"Total technician docs: `{len(technician_summaries)}`",
        "",
        _markdown_table(technician_summaries, [
            ("technician", "Technician"),
            ("total_tickets", "Tickets"),
            ("open_tickets", "Open"),
            ("closed_tickets", "Closed"),
            ("detail_tickets", "Detail Tickets"),
            ("document_tickets", "Document Tickets"),
            ("chunk_count", "Chunks"),
            ("latest_activity_at", "Latest Activity"),
        ]),
        "",
        "These are derived per-technician retrieval/support artifacts, not canonical truth.",
    ]
    technician_index_path.write_text("\n".join(technician_index_md) + "\n")

    generated_files = [
        str(snapshot_path),
        str(stale_open_path),
        str(account_activity_path),
        str(technician_load_path),
        str(account_index_path),
        str(technician_index_path),
    ]
    account_docs_written = 0
    technician_docs_written = 0

    for account_row in account_summaries:
        account_name = account_row["account"]
        summary = get_account_summary(db_path, str(account_row["account_ref"] or account_name))
        if summary.get("status") != "ok":
            continue
        path = account_dir / f"{_safe_doc_name(account_name)}.md"
        lines = [
            f"# Account Summary: {summary['account']['name']}",
            "",
            f"Generated: `{generated_at}`",
            "",
            "## Stats",
            "",
            "```json",
            json.dumps(summary["stats"], indent=2),
            "```",
            "",
            "## Status breakdown",
            "",
            _markdown_table(summary["status_breakdown"], [("status", "Status"), ("ticket_count", "Ticket Count")]),
            "",
            "## Priority breakdown",
            "",
            _markdown_table(summary["priority_breakdown"], [("priority", "Priority"), ("ticket_count", "Ticket Count")]),
            "",
            "## Category breakdown",
            "",
            _markdown_table(summary["category_breakdown"], [("category", "Category"), ("ticket_count", "Ticket Count")]),
            "",
            "## Open tickets",
            "",
            _markdown_table(summary["open_tickets"], [
                ("id", "Ticket ID"),
                ("subject", "Subject"),
                ("priority", "Priority"),
                ("category", "Category"),
                ("updated_at", "Updated"),
            ]),
            "",
            "## Recent tickets",
            "",
            _markdown_table(summary["recent_tickets"], [
                ("id", "Ticket ID"),
                ("subject", "Subject"),
                ("status", "Status"),
                ("priority", "Priority"),
                ("category", "Category"),
                ("updated_at", "Updated"),
            ]),
            "",
            "## Recent log types",
            "",
            _markdown_table(summary["recent_log_types"], [("log_type", "Log Type"), ("log_count", "Count")]),
        ]
        path.write_text("\n".join(lines) + "\n")
        generated_files.append(str(path))
        account_docs_written += 1

    for technician_row in technician_summaries:
        technician_name = technician_row["technician"]
        summary = get_technician_summary(db_path, str(technician_row["technician_ref"] or technician_name))
        if summary.get("status") != "ok":
            continue
        path = technician_dir / f"{_safe_doc_name(technician_name)}.md"
        lines = [
            f"# Technician Summary: {summary['technician']['display_name']}",
            "",
            f"Generated: `{generated_at}`",
            "",
            "## Stats",
            "",
            "```json",
            json.dumps(summary["stats"], indent=2),
            "```",
            "",
            "## Status breakdown",
            "",
            _markdown_table(summary["status_breakdown"], [("status", "Status"), ("ticket_count", "Ticket Count")]),
            "",
            "## Priority breakdown",
            "",
            _markdown_table(summary["priority_breakdown"], [("priority", "Priority"), ("ticket_count", "Ticket Count")]),
            "",
            "## Category breakdown",
            "",
            _markdown_table(summary["category_breakdown"], [("category", "Category"), ("ticket_count", "Ticket Count")]),
            "",
            "## Open tickets",
            "",
            _markdown_table(summary["open_tickets"], [
                ("id", "Ticket ID"),
                ("subject", "Subject"),
                ("priority", "Priority"),
                ("category", "Category"),
                ("updated_at", "Updated"),
            ]),
            "",
            "## Recent tickets",
            "",
            _markdown_table(summary["recent_tickets"], [
                ("id", "Ticket ID"),
                ("subject", "Subject"),
                ("status", "Status"),
                ("priority", "Priority"),
                ("category", "Category"),
                ("updated_at", "Updated"),
            ]),
            "",
            "## Recent log types",
            "",
            _markdown_table(summary["recent_log_types"], [("log_type", "Log Type"), ("log_count", "Count")]),
        ]
        path.write_text("\n".join(lines) + "\n")
        generated_files.append(str(path))
        technician_docs_written += 1

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
        "- `accounts/index.md`",
        "- `technicians/index.md`",
        f"- account docs directory: `{account_dir}` ({account_docs_written} docs)",
        f"- technician docs directory: `{technician_dir}` ({technician_docs_written} docs)",
        "",
        "These are derived/public artifacts for OpenClaw-friendly access. Canonical truth remains in `.SherpaMind/private/data/`.",
        "The matching vector-ready export lives under `.SherpaMind/public/exports/embedding-ticket-chunks.jsonl` when generated.",
        "The matching vector export manifest lives under `.SherpaMind/public/exports/embedding-ticket-chunks.manifest.json` when generated.",
    ]
    index_path.write_text("\n".join(index_md) + "\n")
    generated_files.insert(0, str(index_path))

    return {
        "status": "ok",
        "output_path": str(snapshot_path),
        "generated_at": generated_at,
        "generated_files": generated_files,
        "account_docs_generated": account_docs_written,
        "technician_docs_generated": technician_docs_written,
        "account_artifact_candidates": len(account_summaries),
        "technician_artifact_candidates": len(technician_summaries),
    }
