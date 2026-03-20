from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .db import connect, initialize_db
from .documents import DOCUMENT_MATERIALIZATION_VERSION, get_ticket_document_materialization_status
from .vector_index import get_vector_index_status


def _load_rows(db_path: Path, limit: int | None = None) -> list[dict[str, Any]]:
    query = """
        SELECT c.chunk_id,
               c.doc_id,
               c.ticket_id,
               c.chunk_index,
               c.text,
               LENGTH(c.text) AS chunk_chars,
               c.content_hash,
               c.synced_at AS chunk_synced_at,
               d.updated_at,
               d.status,
               d.account,
               d.user_name,
               d.technician,
               json_extract(d.raw_json, '$.account_id') AS account_id,
               json_extract(d.raw_json, '$.user_id') AS user_id,
               json_extract(d.raw_json, '$.technician_id') AS technician_id,
               json_extract(d.raw_json, '$.created_at') AS created_at,
               COALESCE(json_extract(d.raw_json, '$.materialization_version'), json_extract(d.raw_json, '$.metadata.materialization_version')) AS materialization_version,
               json_extract(d.raw_json, '$.metadata.priority') AS priority,
               json_extract(d.raw_json, '$.metadata.category') AS category,
               json_extract(d.raw_json, '$.metadata.closed_at') AS closed_at,
               json_extract(d.raw_json, '$.metadata.attachments_count') AS attachments_count,
               json_extract(d.raw_json, '$.metadata.ticketlogs_count') AS ticketlogs_count,
               json_extract(d.raw_json, '$.metadata.timelogs_count') AS timelogs_count,
               json_extract(d.raw_json, '$.metadata.cleaned_subject') AS cleaned_subject,
               json_extract(d.raw_json, '$.metadata.cleaned_initial_post') AS cleaned_initial_post,
               json_extract(d.raw_json, '$.metadata.cleaned_detail_note') AS cleaned_detail_note,
               json_extract(d.raw_json, '$.metadata.cleaned_workpad') AS cleaned_workpad,
               json_extract(d.raw_json, '$.metadata.cleaned_followup_note') AS cleaned_followup_note,
               json_extract(d.raw_json, '$.metadata.cleaned_request_completion_note') AS cleaned_request_completion_note,
               json_extract(d.raw_json, '$.metadata.cleaned_next_step') AS cleaned_next_step,
               json_extract(d.raw_json, '$.metadata.cleaned_latest_response_note') AS cleaned_latest_response_note,
               json_extract(d.raw_json, '$.metadata.latest_response_date') AS latest_response_date,
               json_extract(d.raw_json, '$.metadata.cleaned_resolution_log_note') AS cleaned_resolution_log_note,
               json_extract(d.raw_json, '$.metadata.resolution_log_date') AS resolution_log_date,
               json_extract(d.raw_json, '$.metadata.next_step_date') AS next_step_date,
               json_extract(d.raw_json, '$.metadata.followup_date') AS followup_date,
               json_extract(d.raw_json, '$.metadata.request_completion_date') AS request_completion_date,
               json_extract(d.raw_json, '$.metadata.recent_log_types_csv') AS recent_log_types,
               json_extract(d.raw_json, '$.metadata.initial_response_present') AS initial_response_present,
               json_extract(d.raw_json, '$.metadata.user_email') AS user_email,
               json_extract(d.raw_json, '$.metadata.support_group_name') AS support_group_name,
               json_extract(d.raw_json, '$.metadata.default_contract_name') AS default_contract_name,
               json_extract(d.raw_json, '$.metadata.location_name') AS location_name,
               json_extract(d.raw_json, '$.metadata.account_location_name') AS account_location_name,
               json_extract(d.raw_json, '$.metadata.department_key') AS department_key,
               json_extract(d.raw_json, '$.metadata.confirmed_by_name') AS confirmed_by_name,
               json_extract(d.raw_json, '$.metadata.confirmed_date') AS confirmed_date,
               json_extract(d.raw_json, '$.metadata.is_via_email_parser') AS is_via_email_parser,
               json_extract(d.raw_json, '$.metadata.is_handle_by_callcentre') AS is_handle_by_callcentre,
               json_extract(d.raw_json, '$.metadata.is_waiting_on_response') AS is_waiting_on_response,
               json_extract(d.raw_json, '$.metadata.is_resolved') AS is_resolved,
               json_extract(d.raw_json, '$.metadata.is_confirmed') AS is_confirmed,
               json_extract(d.raw_json, '$.metadata.account_label_source') AS account_label_source,
               json_extract(d.raw_json, '$.metadata.user_label_source') AS user_label_source,
               json_extract(d.raw_json, '$.metadata.technician_label_source') AS technician_label_source,
               json_extract(d.raw_json, '$.metadata.has_attachments') AS has_attachments,
               json_extract(d.raw_json, '$.metadata.has_next_step') AS has_next_step,
               json_extract(d.raw_json, '$.metadata.resolution_summary') AS resolution_summary,
               json_extract(d.raw_json, '$.metadata.has_resolution_summary') AS has_resolution_summary,
               json_extract(d.raw_json, '$.metadata.detail_available') AS detail_available
        FROM ticket_document_chunks c
        JOIN ticket_documents d ON d.doc_id = c.doc_id
        ORDER BY c.ticket_id DESC, c.chunk_index ASC
    """
    params: tuple = ()
    if limit is not None:
        query += " LIMIT ?"
        params = (limit,)

    with connect(db_path) as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def export_embedding_ready_chunks(db_path: Path, output_path: Path, limit: int | None = None) -> dict:
    rows = _load_rows(db_path, limit=limit)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with output_path.open("w", encoding="utf-8") as f:
        for record in rows:
            payload = {
                "id": record["chunk_id"],
                "text": record["text"],
                "metadata": {
                    "doc_id": record["doc_id"],
                    "ticket_id": record["ticket_id"],
                    "chunk_index": record["chunk_index"],
                    "status": record["status"],
                    "account": record["account"],
                    "account_id": record["account_id"],
                    "user_name": record["user_name"],
                    "user_id": record["user_id"],
                    "user_email": record["user_email"],
                    "technician": record["technician"],
                    "technician_id": record["technician_id"],
                    "account_label_source": record["account_label_source"],
                    "user_label_source": record["user_label_source"],
                    "technician_label_source": record["technician_label_source"],
                    "priority": record["priority"],
                    "category": record["category"],
                    "closed_at": record["closed_at"],
                    "attachments_count": record["attachments_count"],
                    "has_attachments": bool(record["has_attachments"]),
                    "ticketlogs_count": record["ticketlogs_count"],
                    "timelogs_count": record["timelogs_count"],
                    "cleaned_subject": record["cleaned_subject"],
                    "cleaned_initial_post": record["cleaned_initial_post"],
                    "cleaned_detail_note": record["cleaned_detail_note"],
                    "cleaned_workpad": record["cleaned_workpad"],
                    "cleaned_followup_note": record["cleaned_followup_note"],
                    "cleaned_request_completion_note": record["cleaned_request_completion_note"],
                    "cleaned_next_step": record["cleaned_next_step"],
                    "cleaned_latest_response_note": record["cleaned_latest_response_note"],
                    "latest_response_date": record["latest_response_date"],
                    "cleaned_resolution_log_note": record["cleaned_resolution_log_note"],
                    "resolution_log_date": record["resolution_log_date"],
                    "next_step_date": record["next_step_date"],
                    "followup_date": record["followup_date"],
                    "request_completion_date": record["request_completion_date"],
                    "has_next_step": bool(record["has_next_step"]),
                    "recent_log_types": record["recent_log_types"],
                    "initial_response_present": bool(record["initial_response_present"]),
                    "support_group_name": record["support_group_name"],
                    "default_contract_name": record["default_contract_name"],
                    "location_name": record["location_name"],
                    "account_location_name": record["account_location_name"],
                    "department_key": record["department_key"],
                    "confirmed_by_name": record["confirmed_by_name"],
                    "confirmed_date": record["confirmed_date"],
                    "is_via_email_parser": None if record["is_via_email_parser"] is None else bool(record["is_via_email_parser"]),
                    "is_handle_by_callcentre": None if record["is_handle_by_callcentre"] is None else bool(record["is_handle_by_callcentre"]),
                    "is_waiting_on_response": None if record["is_waiting_on_response"] is None else bool(record["is_waiting_on_response"]),
                    "is_resolved": None if record["is_resolved"] is None else bool(record["is_resolved"]),
                    "is_confirmed": None if record["is_confirmed"] is None else bool(record["is_confirmed"]),
                    "resolution_summary": record["resolution_summary"],
                    "has_resolution_summary": bool(record["has_resolution_summary"]),
                    "created_at": record["created_at"],
                    "updated_at": record["updated_at"],
                    "content_hash": record["content_hash"],
                },
            }
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
            count += 1
    return {
        "status": "ok",
        "output_path": str(output_path),
        "chunk_count": count,
    }


def _present(value: Any) -> bool:
    return value is not None and value != ""


def get_retrieval_readiness_summary(db_path: Path, limit: int | None = None) -> dict[str, Any]:
    initialize_db(db_path)
    rows = _load_rows(db_path, limit=limit)
    vector = get_vector_index_status(db_path)
    materialization = get_ticket_document_materialization_status(db_path)

    chunk_count = len(rows)
    document_ids = {row["doc_id"] for row in rows}
    accounts = sorted({row.get("account") for row in rows if row.get("account")})
    technicians = sorted({row.get("technician") for row in rows if row.get("technician")})
    statuses = sorted({row.get("status") for row in rows if row.get("status")})
    priorities = sorted({row.get("priority") for row in rows if row.get("priority")})
    categories = sorted({row.get("category") for row in rows if row.get("category")})

    chunk_lengths = [int(row.get("chunk_chars") or 0) for row in rows]
    chunk_counts_by_doc: dict[str, int] = {}
    for row in rows:
        chunk_counts_by_doc[row["doc_id"]] = chunk_counts_by_doc.get(row["doc_id"], 0) + 1

    metadata_fields = {
        "account": lambda row: _present(row.get("account")),
        "status": lambda row: _present(row.get("status")),
        "technician": lambda row: _present(row.get("technician")),
        "priority": lambda row: _present(row.get("priority")),
        "category": lambda row: _present(row.get("category")),
        "user_email": lambda row: _present(row.get("user_email")),
        "cleaned_subject": lambda row: _present(row.get("cleaned_subject")),
        "cleaned_initial_post": lambda row: _present(row.get("cleaned_initial_post")),
        "cleaned_detail_note": lambda row: _present(row.get("cleaned_detail_note")),
        "cleaned_workpad": lambda row: _present(row.get("cleaned_workpad")),
        "cleaned_followup_note": lambda row: _present(row.get("cleaned_followup_note")),
        "cleaned_request_completion_note": lambda row: _present(row.get("cleaned_request_completion_note")),
        "cleaned_next_step": lambda row: _present(row.get("cleaned_next_step")),
        "cleaned_latest_response_note": lambda row: _present(row.get("cleaned_latest_response_note")),
        "latest_response_date": lambda row: _present(row.get("latest_response_date")),
        "cleaned_resolution_log_note": lambda row: _present(row.get("cleaned_resolution_log_note")),
        "resolution_log_date": lambda row: _present(row.get("resolution_log_date")),
        "followup_date": lambda row: _present(row.get("followup_date")),
        "request_completion_date": lambda row: _present(row.get("request_completion_date")),
        "support_group_name": lambda row: _present(row.get("support_group_name")),
        "default_contract_name": lambda row: _present(row.get("default_contract_name")),
        "location_name": lambda row: _present(row.get("location_name")),
        "account_location_name": lambda row: _present(row.get("account_location_name")),
        "department_key": lambda row: _present(row.get("department_key")),
        "confirmed_by_name": lambda row: _present(row.get("confirmed_by_name")),
        "confirmed_date": lambda row: _present(row.get("confirmed_date")),
        "recent_log_types": lambda row: _present(row.get("recent_log_types")),
        "resolution_summary": lambda row: _present(row.get("resolution_summary")),
        "detail_available": lambda row: bool(row.get("detail_available")),
        "has_attachments": lambda row: bool(row.get("has_attachments")),
        "has_next_step": lambda row: bool(row.get("has_next_step")),
        "initial_response_present": lambda row: bool(row.get("initial_response_present")),
        "is_via_email_parser": lambda row: row.get("is_via_email_parser") is not None,
        "is_handle_by_callcentre": lambda row: row.get("is_handle_by_callcentre") is not None,
        "is_waiting_on_response": lambda row: row.get("is_waiting_on_response") is not None,
        "is_resolved": lambda row: row.get("is_resolved") is not None,
        "is_confirmed": lambda row: row.get("is_confirmed") is not None,
    }
    metadata_coverage = {}
    for key, predicate in metadata_fields.items():
        covered = sum(1 for row in rows if predicate(row))
        metadata_coverage[key] = {
            "chunks": covered,
            "ratio": round(covered / chunk_count, 4) if chunk_count else 0.0,
        }

    label_source_summary = {}
    for field in ("account_label_source", "user_label_source", "technician_label_source"):
        counts: dict[str, int] = {}
        for row in rows:
            value = row.get(field) or "missing"
            counts[str(value)] = counts.get(str(value), 0) + 1
        label_source_summary[field] = {
            key: {
                "chunks": value,
                "ratio": round(value / chunk_count, 4) if chunk_count else 0.0,
            }
            for key, value in sorted(counts.items())
        }

    return {
        "chunk_count": chunk_count,
        "document_count": len(document_ids),
        "limit_applied": limit,
        "freshness": {
            "earliest_updated_at": min((row.get("updated_at") for row in rows if row.get("updated_at")), default=None),
            "latest_updated_at": max((row.get("updated_at") for row in rows if row.get("updated_at")), default=None),
            "earliest_chunk_synced_at": min((row.get("chunk_synced_at") for row in rows if row.get("chunk_synced_at")), default=None),
            "latest_chunk_synced_at": max((row.get("chunk_synced_at") for row in rows if row.get("chunk_synced_at")), default=None),
        },
        "chunk_quality": {
            "avg_chunk_chars": round(sum(chunk_lengths) / chunk_count, 1) if chunk_count else 0.0,
            "min_chunk_chars": min(chunk_lengths, default=0),
            "max_chunk_chars": max(chunk_lengths, default=0),
            "tiny_chunk_count": sum(1 for length in chunk_lengths if length < 200),
            "target_or_smaller_count": sum(1 for length in chunk_lengths if length <= 1800),
            "over_target_chunk_count": sum(1 for length in chunk_lengths if length > 1800),
            "multi_chunk_document_count": sum(1 for count in chunk_counts_by_doc.values() if count > 1),
        },
        "filter_facets": {
            "accounts": accounts,
            "account_count": len(accounts),
            "technicians": technicians,
            "technician_count": len(technicians),
            "statuses": statuses,
            "status_count": len(statuses),
            "priorities": priorities,
            "priority_count": len(priorities),
            "categories": categories,
            "category_count": len(categories),
        },
        "metadata_coverage": metadata_coverage,
        "label_source_summary": label_source_summary,
        "vector_index": vector,
        "materialization": {
            **materialization,
            "chunk_rows_at_current_version": sum(
                1
                for row in rows
                if int(row.get("materialization_version") or 0) == DOCUMENT_MATERIALIZATION_VERSION
            ),
            "chunk_rows_at_current_version_ratio": round(
                (
                    sum(1 for row in rows if int(row.get("materialization_version") or 0) == DOCUMENT_MATERIALIZATION_VERSION)
                    / chunk_count
                ),
                4,
            ) if chunk_count else 0.0,
        },
        "content_hash_summary": {
            "present_count": sum(1 for row in rows if _present(row.get("content_hash"))),
            "missing_count": sum(1 for row in rows if not _present(row.get("content_hash"))),
            "sample": [row.get("content_hash") for row in rows[:10] if _present(row.get("content_hash"))],
        },
    }


def export_embedding_manifest(db_path: Path, output_path: Path, limit: int | None = None) -> dict:
    manifest = get_retrieval_readiness_summary(db_path, limit=limit)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return {
        "status": "ok",
        "output_path": str(output_path),
        "chunk_count": manifest["chunk_count"],
    }
