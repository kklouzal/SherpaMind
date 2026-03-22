from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

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
from .vector_exports import get_retrieval_readiness_summary
from .vector_index import get_vector_index_status


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


def _cleanup_stale_entity_docs(directory: Path, desired_paths: set[Path]) -> list[str]:
    removed: list[str] = []
    desired_names = {path.name for path in desired_paths}
    for existing in sorted(directory.glob("*.md")):
        if existing.name == "index.md":
            continue
        if existing.name in desired_names:
            continue
        existing.unlink()
        removed.append(str(existing))
    return removed


def _format_ratio(value: Any) -> str:
    if value is None:
        return ""
    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return str(value)


def _format_number(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.1f}" if not value.is_integer() else str(int(value))
    return str(value)


def _top_metadata_gaps(coverage: dict[str, dict[str, Any]], *, count_key: str, limit: int = 12) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field, stats in coverage.items():
        ratio = stats.get("ratio")
        rows.append({
            "field": field,
            count_key: stats.get(count_key, 0),
            "ratio": _format_ratio(ratio),
            "missing_ratio_value": 1.0 - float(ratio or 0.0),
        })
    rows.sort(key=lambda row: (-row["missing_ratio_value"], row["field"]))
    return [{k: v for k, v in row.items() if k != "missing_ratio_value"} for row in rows[:limit]]


def _source_materialization_gap_rows(coverage: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field, stats in coverage.items():
        status = str(stats.get("status") or "")
        if status not in {"partial_materialization", "missing_materialization"}:
            continue
        rows.append({
            "field": field,
            "status": status,
            "source_documents": stats.get("source_documents", 0),
            "materialized_documents": stats.get("materialized_documents", 0),
            "promotion_gap": stats.get("promotion_gap", 0),
            "materialized_ratio": _format_ratio(stats.get("materialized_ratio")),
        })
    rows.sort(key=lambda row: (-int(row["promotion_gap"] or 0), row["field"]))
    return rows


def _upstream_absent_rows(coverage: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field, stats in coverage.items():
        if str(stats.get("status") or "") != "upstream_absent":
            continue
        rows.append({
            "field": field,
            "ticket_rows": stats.get("ticket_rows", 0),
            "detail_rows": stats.get("detail_rows", 0),
            "source_documents": stats.get("source_documents", 0),
            "materialized_documents": stats.get("materialized_documents", 0),
        })
    rows.sort(key=lambda row: row["field"])
    return rows


def _entity_label_quality_rows(entity_quality: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entity, stats in entity_quality.items():
        rows.append({
            "entity": entity,
            "readable_ratio": _format_ratio(stats.get("readable_ratio")),
            "identifier_like_ratio": _format_ratio(stats.get("identifier_like_ratio")),
            "fallback_source_ratio": _format_ratio(stats.get("fallback_source_ratio")),
            "sample_identifier_values": ", ".join(stats.get("identifier_like_distinct_value_sample", [])[:3]),
        })
    return rows


def _source_breakdown_rows(source_counts: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source, stats in source_counts.items():
        rows.append({
            "source": source,
            "chunks": stats.get("chunks", 0),
            "ratio": _format_ratio(stats.get("ratio")),
        })
    rows.sort(key=lambda row: (-int(row["chunks"] or 0), row["source"]))
    return rows


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
    retrieval_readiness = get_retrieval_readiness_summary(db_path)
    vector_index_status = get_vector_index_status(db_path)

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

    retrieval_path = paths.docs_root / "retrieval-readiness.md"
    retrieval_md = [
        "# SherpaMind Retrieval Readiness",
        "",
        f"Generated: `{generated_at}`",
        "",
        "## Summary",
        "",
        f"- Documents: `{retrieval_readiness.get('document_count', 0)}`",
        f"- Chunks: `{retrieval_readiness.get('chunk_count', 0)}`",
        f"- Indexed chunks: `{vector_index_status.get('indexed_chunks', 0)}` / `{vector_index_status.get('total_chunk_rows', 0)}`",
        f"- Vector ready ratio: `{_format_ratio(vector_index_status.get('ready_ratio'))}`",
        f"- Materialization version: `{retrieval_readiness.get('materialization', {}).get('current_version')}`",
        f"- Current-version chunk ratio: `{_format_ratio(retrieval_readiness.get('materialization', {}).get('chunk_rows_at_current_version_ratio'))}`",
        "",
        "## Chunk quality",
        "",
        _markdown_table([
            {
                "avg_chunk_chars": _format_number(retrieval_readiness.get("chunk_quality", {}).get("avg_chunk_chars")),
                "min_chunk_chars": retrieval_readiness.get("chunk_quality", {}).get("min_chunk_chars", 0),
                "max_chunk_chars": retrieval_readiness.get("chunk_quality", {}).get("max_chunk_chars", 0),
                "tiny_chunk_count": retrieval_readiness.get("chunk_quality", {}).get("tiny_chunk_count", 0),
                "over_target_chunk_count": retrieval_readiness.get("chunk_quality", {}).get("over_target_chunk_count", 0),
            }
        ], [
            ("avg_chunk_chars", "Avg Chunk Chars"),
            ("min_chunk_chars", "Min"),
            ("max_chunk_chars", "Max"),
            ("tiny_chunk_count", "Tiny Chunks"),
            ("over_target_chunk_count", "Over Target"),
        ]),
        "",
        "## Document chunk topology",
        "",
        _markdown_table([
            {
                "avg_chunks_per_document": _format_number(retrieval_readiness.get("document_chunk_topology", {}).get("avg_chunks_per_document")),
                "single_chunk_document_count": retrieval_readiness.get("document_chunk_topology", {}).get("single_chunk_document_count", 0),
                "multi_chunk_document_count": retrieval_readiness.get("document_chunk_topology", {}).get("multi_chunk_document_count", 0),
                "multi_chunk_document_ratio": _format_ratio(retrieval_readiness.get("document_chunk_topology", {}).get("multi_chunk_document_ratio")),
                "max_chunks_per_document": retrieval_readiness.get("document_chunk_topology", {}).get("max_chunks_per_document", 0),
            }
        ], [
            ("avg_chunks_per_document", "Avg Chunks / Doc"),
            ("single_chunk_document_count", "Single-Chunk Docs"),
            ("multi_chunk_document_count", "Multi-Chunk Docs"),
            ("multi_chunk_document_ratio", "Multi-Chunk Ratio"),
            ("max_chunks_per_document", "Max Chunks / Doc"),
        ]),
        "",
        "## Freshness",
        "",
        _markdown_table([
            {
                "earliest_updated_at": retrieval_readiness.get("freshness", {}).get("earliest_updated_at", ""),
                "latest_updated_at": retrieval_readiness.get("freshness", {}).get("latest_updated_at", ""),
                "earliest_chunk_synced_at": retrieval_readiness.get("freshness", {}).get("earliest_chunk_synced_at", ""),
                "latest_chunk_synced_at": retrieval_readiness.get("freshness", {}).get("latest_chunk_synced_at", ""),
            }
        ], [
            ("earliest_updated_at", "Earliest Ticket Update"),
            ("latest_updated_at", "Latest Ticket Update"),
            ("earliest_chunk_synced_at", "Chunk Sync Started"),
            ("latest_chunk_synced_at", "Chunk Sync Finished"),
        ]),
        "",
        "## Materialization and vector status",
        "",
        _markdown_table([
            {
                "stale_docs": retrieval_readiness.get("materialization", {}).get("stale_docs", 0),
                "unversioned_docs": retrieval_readiness.get("materialization", {}).get("unversioned_docs", 0),
                "missing_index_rows": vector_index_status.get("missing_index_rows", 0),
                "dangling_index_rows": vector_index_status.get("dangling_index_rows", 0),
                "outdated_content_rows": vector_index_status.get("outdated_content_rows", 0),
            }
        ], [
            ("stale_docs", "Stale Docs"),
            ("unversioned_docs", "Unversioned Docs"),
            ("missing_index_rows", "Missing Index Rows"),
            ("dangling_index_rows", "Dangling Index Rows"),
            ("outdated_content_rows", "Outdated Index Rows"),
        ]),
        "",
        "## Lowest chunk-level metadata coverage",
        "",
        _markdown_table(
            _top_metadata_gaps(retrieval_readiness.get("metadata_coverage", {}), count_key="chunks"),
            [("field", "Field"), ("chunks", "Chunks"), ("ratio", "Coverage")],
        ),
        "",
        "## Lowest document-level metadata coverage",
        "",
        _markdown_table(
            _top_metadata_gaps(retrieval_readiness.get("document_metadata_coverage", {}), count_key="documents"),
            [("field", "Field"), ("documents", "Documents"), ("ratio", "Coverage")],
        ),
        "",
        "## Follow-up cue source coverage",
        "",
        _markdown_table(
            _source_breakdown_rows(retrieval_readiness.get("label_source_summary", {}).get("followup_note_source", {})),
            [("source", "Source"), ("chunks", "Chunks"), ("ratio", "Coverage")],
        ),
        "",
        "## Action cue source coverage",
        "",
        _markdown_table(
            _source_breakdown_rows(retrieval_readiness.get("label_source_summary", {}).get("action_cue_source", {})),
            [("source", "Source"), ("chunks", "Chunks"), ("ratio", "Coverage")],
        ),
        "",
        "## Source-backed metadata promotion gaps",
        "",
        _markdown_table(
            _source_materialization_gap_rows(retrieval_readiness.get("source_metadata_coverage", {})),
            [
                ("field", "Field"),
                ("status", "Status"),
                ("source_documents", "Source Docs"),
                ("materialized_documents", "Materialized Docs"),
                ("promotion_gap", "Gap"),
                ("materialized_ratio", "Materialized Ratio"),
            ],
        ),
        "",
        "## Source-backed metadata still upstream-absent",
        "",
        _markdown_table(
            _upstream_absent_rows(retrieval_readiness.get("source_metadata_coverage", {})),
            [
                ("field", "Field"),
                ("ticket_rows", "Ticket Rows"),
                ("detail_rows", "Detail Rows"),
                ("source_documents", "Source Docs"),
                ("materialized_documents", "Materialized Docs"),
            ],
        ),
        "",
        "## Entity label quality",
        "",
        _markdown_table(
            _entity_label_quality_rows(retrieval_readiness.get("entity_label_quality", {})),
            [
                ("entity", "Entity"),
                ("readable_ratio", "Readable"),
                ("identifier_like_ratio", "Identifier-Like"),
                ("fallback_source_ratio", "Fallback Source"),
                ("sample_identifier_values", "Identifier Samples"),
            ],
        ),
        "",
        "## Raw retrieval readiness JSON",
        "",
        "```json",
        json.dumps(retrieval_readiness, indent=2),
        "```",
        "",
        "## Notes",
        "",
        "- This file is a derived public artifact for OpenClaw-friendly inspection of retrieval quality and drift.",
        "- Canonical truth remains in `.SherpaMind/private/data/sherpamind.sqlite3`.",
        "- Materialized docs, chunks, vector rows, and Markdown outputs remain replaceable caches.",
    ]
    retrieval_path.write_text("\n".join(retrieval_md) + "\n")

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
        str(retrieval_path),
        str(stale_open_path),
        str(account_activity_path),
        str(technician_load_path),
        str(account_index_path),
        str(technician_index_path),
    ]
    account_docs_written = 0
    technician_docs_written = 0
    desired_account_paths: set[Path] = set()
    desired_technician_paths: set[Path] = set()

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
        desired_account_paths.add(path)
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
        desired_technician_paths.add(path)
        generated_files.append(str(path))
        technician_docs_written += 1

    removed_account_docs = _cleanup_stale_entity_docs(account_dir, desired_account_paths)
    removed_technician_docs = _cleanup_stale_entity_docs(technician_dir, desired_technician_paths)

    index_path = paths.docs_root / "index.md"
    index_md = [
        "# SherpaMind Public Docs Index",
        "",
        f"Generated: `{generated_at}`",
        "",
        "Available derived artifacts:",
        "- `insight-snapshot.md`",
        "- `retrieval-readiness.md`",
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
        "stale_account_docs_removed": len(removed_account_docs),
        "stale_technician_docs_removed": len(removed_technician_docs),
        "removed_files": removed_account_docs + removed_technician_docs,
    }
