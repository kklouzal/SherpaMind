from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .db import connect, initialize_db
from .documents import DOCUMENT_MATERIALIZATION_VERSION, get_ticket_document_materialization_status
from .time_utils import parse_sherpadesk_timestamp
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
               json_extract(d.raw_json, '$.metadata.class_name') AS class_name,
               json_extract(d.raw_json, '$.metadata.submission_category') AS submission_category,
               json_extract(d.raw_json, '$.metadata.resolution_category') AS resolution_category,
               json_extract(d.raw_json, '$.metadata.closed_at') AS closed_at,
               json_extract(d.raw_json, '$.metadata.attachments_count') AS attachments_count,
               json_extract(d.raw_json, '$.metadata.attachment_extensions_csv') AS attachment_extensions_csv,
               json_extract(d.raw_json, '$.metadata.attachment_kinds_csv') AS attachment_kinds_csv,
               json_extract(d.raw_json, '$.metadata.attachment_kind_primary') AS attachment_kind_primary,
               json_extract(d.raw_json, '$.metadata.attachment_total_size_bytes') AS attachment_total_size_bytes,
               json_extract(d.raw_json, '$.metadata.attachment_size_known_count') AS attachment_size_known_count,
               json_extract(d.raw_json, '$.metadata.attachment_image_count') AS attachment_image_count,
               json_extract(d.raw_json, '$.metadata.attachment_document_count') AS attachment_document_count,
               json_extract(d.raw_json, '$.metadata.attachment_spreadsheet_count') AS attachment_spreadsheet_count,
               json_extract(d.raw_json, '$.metadata.attachment_archive_count') AS attachment_archive_count,
               json_extract(d.raw_json, '$.metadata.attachment_log_count') AS attachment_log_count,
               json_extract(d.raw_json, '$.metadata.attachment_data_count') AS attachment_data_count,
               json_extract(d.raw_json, '$.metadata.attachment_audio_count') AS attachment_audio_count,
               json_extract(d.raw_json, '$.metadata.attachment_video_count') AS attachment_video_count,
               json_extract(d.raw_json, '$.metadata.attachment_message_count') AS attachment_message_count,
               json_extract(d.raw_json, '$.metadata.attachment_other_count') AS attachment_other_count,
               json_extract(d.raw_json, '$.metadata.attachment_unknown_count') AS attachment_unknown_count,
               json_extract(d.raw_json, '$.metadata.ticketlogs_count') AS ticketlogs_count,
               json_extract(d.raw_json, '$.metadata.public_log_count') AS public_log_count,
               json_extract(d.raw_json, '$.metadata.internal_log_count') AS internal_log_count,
               json_extract(d.raw_json, '$.metadata.waiting_log_count') AS waiting_log_count,
               json_extract(d.raw_json, '$.metadata.response_log_count') AS response_log_count,
               json_extract(d.raw_json, '$.metadata.resolution_log_count') AS resolution_log_count,
               json_extract(d.raw_json, '$.metadata.latest_log_date') AS latest_log_date,
               json_extract(d.raw_json, '$.metadata.latest_public_log_date') AS latest_public_log_date,
               json_extract(d.raw_json, '$.metadata.latest_internal_log_date') AS latest_internal_log_date,
               json_extract(d.raw_json, '$.metadata.latest_waiting_log_date') AS latest_waiting_log_date,
               json_extract(d.raw_json, '$.metadata.latest_resolution_log_date') AS latest_resolution_log_date,
               json_extract(d.raw_json, '$.metadata.timelogs_count') AS timelogs_count,
               json_extract(d.raw_json, '$.metadata.cleaned_subject') AS cleaned_subject,
               json_extract(d.raw_json, '$.metadata.cleaned_initial_post') AS cleaned_initial_post,
               json_extract(d.raw_json, '$.metadata.cleaned_detail_note') AS cleaned_detail_note,
               json_extract(d.raw_json, '$.metadata.cleaned_workpad') AS cleaned_workpad,
               json_extract(d.raw_json, '$.metadata.cleaned_followup_note') AS cleaned_followup_note,
               json_extract(d.raw_json, '$.metadata.cleaned_explicit_followup_note') AS cleaned_explicit_followup_note,
               json_extract(d.raw_json, '$.metadata.cleaned_waiting_log_note') AS cleaned_waiting_log_note,
               json_extract(d.raw_json, '$.metadata.followup_note_source') AS followup_note_source,
               json_extract(d.raw_json, '$.metadata.cleaned_request_completion_note') AS cleaned_request_completion_note,
               json_extract(d.raw_json, '$.metadata.cleaned_next_step') AS cleaned_next_step,
               json_extract(d.raw_json, '$.metadata.cleaned_action_cue') AS cleaned_action_cue,
               json_extract(d.raw_json, '$.metadata.action_cue_source') AS action_cue_source,
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
               json_extract(d.raw_json, '$.metadata.department_label') AS department_label,
               json_extract(d.raw_json, '$.metadata.department_label_source') AS department_label_source,
               json_extract(d.raw_json, '$.metadata.ticket_number') AS ticket_number,
               json_extract(d.raw_json, '$.metadata.ticket_key') AS ticket_key,
               json_extract(d.raw_json, '$.metadata.technician_email') AS technician_email,
               json_extract(d.raw_json, '$.metadata.user_phone') AS user_phone,
               json_extract(d.raw_json, '$.metadata.user_created_name') AS user_created_name,
               json_extract(d.raw_json, '$.metadata.user_created_email') AS user_created_email,
               json_extract(d.raw_json, '$.metadata.technician_type') AS technician_type,
               json_extract(d.raw_json, '$.metadata.project_id') AS project_id,
               json_extract(d.raw_json, '$.metadata.project_name') AS project_name,
               json_extract(d.raw_json, '$.metadata.scheduled_ticket_id') AS scheduled_ticket_id,
               json_extract(d.raw_json, '$.metadata.related_tickets_count') AS related_tickets_count,
               json_extract(d.raw_json, '$.metadata.estimated_time') AS estimated_time,
               json_extract(d.raw_json, '$.metadata.remaining_hours') AS remaining_hours,
               json_extract(d.raw_json, '$.metadata.total_hours') AS total_hours,
               json_extract(d.raw_json, '$.metadata.total_time_in_minutes') AS total_time_in_minutes,
               json_extract(d.raw_json, '$.metadata.labor_cost') AS labor_cost,
               json_extract(d.raw_json, '$.metadata.percentage_complete') AS percentage_complete,
               json_extract(d.raw_json, '$.metadata.has_project_context') AS has_project_context,
               json_extract(d.raw_json, '$.metadata.has_scheduled_parent') AS has_scheduled_parent,
               json_extract(d.raw_json, '$.metadata.has_related_tickets') AS has_related_tickets,
               json_extract(d.raw_json, '$.metadata.has_effort_tracking') AS has_effort_tracking,
               json_extract(d.raw_json, '$.metadata.has_public_logs') AS has_public_logs,
               json_extract(d.raw_json, '$.metadata.has_internal_logs') AS has_internal_logs,
               json_extract(d.raw_json, '$.metadata.has_waiting_logs') AS has_waiting_logs,
               json_extract(d.raw_json, '$.metadata.has_resolution_logs') AS has_resolution_logs,
               json_extract(d.raw_json, '$.metadata.days_old_in_minutes') AS days_old_in_minutes,
               json_extract(d.raw_json, '$.metadata.waiting_minutes') AS waiting_minutes,
               json_extract(d.raw_json, '$.metadata.confirmed_by_name') AS confirmed_by_name,
               json_extract(d.raw_json, '$.metadata.confirmed_date') AS confirmed_date,
               json_extract(d.raw_json, '$.metadata.cleaned_confirmed_note') AS cleaned_confirmed_note,
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
    chunk_counts_by_doc: dict[str, int] = {}
    doc_total_chunk_chars: dict[str, int] = {}
    rows_by_doc: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        doc_id = str(row["doc_id"])
        chunk_counts_by_doc[doc_id] = chunk_counts_by_doc.get(doc_id, 0) + 1
        doc_total_chunk_chars[doc_id] = doc_total_chunk_chars.get(doc_id, 0) + int(row.get("chunk_chars") or 0)
        rows_by_doc.setdefault(doc_id, []).append(row)

    chunk_position_by_id: dict[str, dict[str, Any]] = {}
    for doc_id, doc_rows in rows_by_doc.items():
        ordered_rows = sorted(doc_rows, key=lambda item: int(item.get("chunk_index") or 0))
        running_start = 0
        doc_total_chars = doc_total_chunk_chars.get(doc_id, 0)
        for idx, row in enumerate(ordered_rows):
            chunk_chars = int(row.get("chunk_chars") or 0)
            chunk_start_char = running_start
            chunk_end_char = running_start + chunk_chars
            chunk_position_by_id[str(row["chunk_id"])] = {
                "chunk_start_char": chunk_start_char,
                "chunk_end_char": chunk_end_char,
                "chunk_char_span_ratio": round((chunk_chars / doc_total_chars), 4) if doc_total_chars else 0.0,
                "previous_chunk_id": ordered_rows[idx - 1]["chunk_id"] if idx > 0 else None,
                "next_chunk_id": ordered_rows[idx + 1]["chunk_id"] if idx + 1 < len(ordered_rows) else None,
            }
            running_start = chunk_end_char

    count = 0
    with output_path.open("w", encoding="utf-8") as f:
        for record in rows:
            doc_id = str(record["doc_id"])
            chunk_count_for_doc = chunk_counts_by_doc.get(doc_id, 0)
            chunk_chars = int(record.get("chunk_chars") or 0)
            doc_chunk_chars = doc_total_chunk_chars.get(doc_id, 0)
            position = chunk_position_by_id.get(str(record["chunk_id"]), {})
            chunk_sections = _infer_chunk_sections(record.get("text"))
            payload = {
                "id": record["chunk_id"],
                "text": record["text"],
                "metadata": {
                    "doc_id": record["doc_id"],
                    "ticket_id": record["ticket_id"],
                    "chunk_index": record["chunk_index"],
                    "chunk_chars": chunk_chars,
                    "chunk_count_for_doc": chunk_count_for_doc,
                    "doc_total_chunk_chars": doc_chunk_chars,
                    "chunk_start_char": position.get("chunk_start_char", 0),
                    "chunk_end_char": position.get("chunk_end_char", chunk_chars),
                    "chunk_char_span_ratio": position.get("chunk_char_span_ratio", 0.0),
                    "previous_chunk_id": position.get("previous_chunk_id"),
                    "next_chunk_id": position.get("next_chunk_id"),
                    "is_first_chunk": record["chunk_index"] == 0,
                    "is_last_chunk": chunk_count_for_doc > 0 and record["chunk_index"] == (chunk_count_for_doc - 1),
                    "is_multi_chunk_doc": chunk_count_for_doc > 1,
                    "chunk_primary_section": chunk_sections["chunk_primary_section"],
                    "chunk_section_labels": chunk_sections["chunk_section_labels_csv"],
                    "chunk_section_count": chunk_sections["chunk_section_count"],
                    "chunk_has_identity_context": chunk_sections["chunk_has_identity_context"],
                    "chunk_has_workflow_context": chunk_sections["chunk_has_workflow_context"],
                    "chunk_has_project_context": chunk_sections["chunk_has_project_context"],
                    "chunk_has_action_context": chunk_sections["chunk_has_action_context"],
                    "chunk_has_issue_context": chunk_sections["chunk_has_issue_context"],
                    "chunk_has_activity_context": chunk_sections["chunk_has_activity_context"],
                    "chunk_has_resolution_context": chunk_sections["chunk_has_resolution_context"],
                    "chunk_has_attachment_context": chunk_sections["chunk_has_attachment_context"],
                    "chunk_section_line_counts": chunk_sections["chunk_section_line_counts"],
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
                    "class_name": record["class_name"],
                    "submission_category": record["submission_category"],
                    "resolution_category": record["resolution_category"],
                    "closed_at": record["closed_at"],
                    "attachments_count": record["attachments_count"],
                    "attachment_extensions": record["attachment_extensions_csv"],
                    "attachment_kinds": record["attachment_kinds_csv"],
                    "attachment_kind_primary": record["attachment_kind_primary"],
                    "attachment_total_size_bytes": record["attachment_total_size_bytes"],
                    "attachment_size_known_count": record["attachment_size_known_count"],
                    "attachment_image_count": record["attachment_image_count"],
                    "attachment_document_count": record["attachment_document_count"],
                    "attachment_spreadsheet_count": record["attachment_spreadsheet_count"],
                    "attachment_archive_count": record["attachment_archive_count"],
                    "attachment_log_count": record["attachment_log_count"],
                    "attachment_data_count": record["attachment_data_count"],
                    "attachment_audio_count": record["attachment_audio_count"],
                    "attachment_video_count": record["attachment_video_count"],
                    "attachment_message_count": record["attachment_message_count"],
                    "attachment_other_count": record["attachment_other_count"],
                    "attachment_unknown_count": record["attachment_unknown_count"],
                    "has_attachments": bool(record["has_attachments"]),
                    "ticketlogs_count": record["ticketlogs_count"],
                    "public_log_count": record["public_log_count"],
                    "internal_log_count": record["internal_log_count"],
                    "waiting_log_count": record["waiting_log_count"],
                    "response_log_count": record["response_log_count"],
                    "resolution_log_count": record["resolution_log_count"],
                    "latest_log_date": record["latest_log_date"],
                    "latest_public_log_date": record["latest_public_log_date"],
                    "latest_internal_log_date": record["latest_internal_log_date"],
                    "latest_waiting_log_date": record["latest_waiting_log_date"],
                    "latest_resolution_log_date": record["latest_resolution_log_date"],
                    "timelogs_count": record["timelogs_count"],
                    "cleaned_subject": record["cleaned_subject"],
                    "cleaned_initial_post": record["cleaned_initial_post"],
                    "cleaned_detail_note": record["cleaned_detail_note"],
                    "cleaned_workpad": record["cleaned_workpad"],
                    "cleaned_followup_note": record["cleaned_followup_note"],
                    "cleaned_explicit_followup_note": record["cleaned_explicit_followup_note"],
                    "cleaned_waiting_log_note": record["cleaned_waiting_log_note"],
                    "followup_note_source": record["followup_note_source"],
                    "cleaned_request_completion_note": record["cleaned_request_completion_note"],
                    "cleaned_next_step": record["cleaned_next_step"],
                    "cleaned_action_cue": record["cleaned_action_cue"],
                    "action_cue_source": record["action_cue_source"],
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
                    "department_label": record["department_label"],
                    "department_label_source": record["department_label_source"],
                    "ticket_number": record["ticket_number"],
                    "ticket_key": record["ticket_key"],
                    "technician_email": record["technician_email"],
                    "user_phone": record["user_phone"],
                    "user_created_name": record["user_created_name"],
                    "user_created_email": record["user_created_email"],
                    "technician_type": record["technician_type"],
                    "project_id": record["project_id"],
                    "project_name": record["project_name"],
                    "scheduled_ticket_id": record["scheduled_ticket_id"],
                    "related_tickets_count": record["related_tickets_count"],
                    "estimated_time": record["estimated_time"],
                    "remaining_hours": record["remaining_hours"],
                    "total_hours": record["total_hours"],
                    "total_time_in_minutes": record["total_time_in_minutes"],
                    "labor_cost": record["labor_cost"],
                    "percentage_complete": record["percentage_complete"],
                    "has_project_context": bool(record["has_project_context"]),
                    "has_scheduled_parent": bool(record["has_scheduled_parent"]),
                    "has_related_tickets": bool(record["has_related_tickets"]),
                    "has_effort_tracking": bool(record["has_effort_tracking"]),
                    "has_public_logs": bool(record["has_public_logs"]),
                    "has_internal_logs": bool(record["has_internal_logs"]),
                    "has_waiting_logs": bool(record["has_waiting_logs"]),
                    "has_resolution_logs": bool(record["has_resolution_logs"]),
                    "days_old_in_minutes": record["days_old_in_minutes"],
                    "waiting_minutes": record["waiting_minutes"],
                    "confirmed_by_name": record["confirmed_by_name"],
                    "confirmed_date": record["confirmed_date"],
                    "cleaned_confirmed_note": record["cleaned_confirmed_note"],
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


def _split_csv_values(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    return [part.strip() for part in str(value).split(",") if part and part.strip()]


def _looks_like_identifier(value: Any) -> bool:
    if value is None:
        return False
    candidate = str(value).strip()
    return bool(candidate) and candidate.isdigit()


CHUNK_SECTION_PREFIXES: dict[str, tuple[str, ...]] = {
    "identity": (
        "Ticket #",
        "Status:",
        "Priority:",
        "Category:",
        "Account:",
        "User:",
        "Technician:",
        "Created:",
        "Updated:",
        "Closed:",
        "Ticket number:",
        "Ticket key:",
        "Technician email:",
        "Created by:",
        "Created by email:",
        "User phone:",
        "Account location:",
        "Location:",
        "Department:",
        "Department key:",
        "Support group:",
        "Contract:",
    ),
    "workflow": (
        "Via email parser:",
        "Handled by call centre:",
        "Initial response flag/value:",
        "SLA response date:",
        "SLA completion date:",
        "Ticket log count:",
        "Time log count:",
        "Attachment count:",
        "Waiting on response:",
        "Waiting minutes:",
        "Confirmed:",
        "Resolved flag:",
        "Ticket age minutes:",
        "Technician type:",
    ),
    "project": (
        "Project:",
        "Project ID:",
        "Scheduled ticket ID:",
        "Related ticket count:",
        "Estimated time:",
        "Remaining hours:",
        "Total hours:",
        "Total time minutes:",
        "Labor cost:",
        "Percent complete:",
    ),
    "action": (
        "Next step:",
        "Next step date:",
        "Follow-up date:",
        "Follow-up note:",
        "Requested completion date:",
        "Requested completion note:",
    ),
    "issue": (
        "Issue summary:",
        "Internal note:",
        "Workpad summary:",
    ),
    "activity": (
        "Latest response date:",
        "Latest response note:",
        "Recent log types:",
        "Recent log summary:",
    ),
    "resolution": (
        "Confirmed by:",
        "Confirmed date:",
        "Confirmed note:",
        "Resolution log date:",
        "Resolution log note:",
        "Resolution/activity highlight:",
        "Resolution category:",
    ),
    "attachments": (
        "Attachments (metadata only):",
        "Attachment kinds:",
        "Attachment extensions:",
        "Attachment total size bytes:",
    ),
}


def _infer_chunk_sections(text: Any) -> dict[str, Any]:
    ordered_sections: list[str] = []
    section_counts: dict[str, int] = {}
    first_seen_index: dict[str, int] = {}

    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    for line in lines:
        matched_section = None
        for section, prefixes in CHUNK_SECTION_PREFIXES.items():
            if any(line.startswith(prefix) for prefix in prefixes):
                matched_section = section
                break
        if matched_section is None:
            continue
        if matched_section not in first_seen_index:
            first_seen_index[matched_section] = len(ordered_sections)
            ordered_sections.append(matched_section)
        section_counts[matched_section] = section_counts.get(matched_section, 0) + 1

    if not ordered_sections:
        ordered_sections = ["general"]
        section_counts = {"general": len(lines) if lines else 1}
        first_seen_index = {"general": 0}

    primary_section = min(
        ordered_sections,
        key=lambda section: (-section_counts.get(section, 0), first_seen_index.get(section, 0), section),
    )
    section_labels_csv = ", ".join(ordered_sections)

    return {
        "chunk_primary_section": primary_section,
        "chunk_section_labels": ordered_sections,
        "chunk_section_labels_csv": section_labels_csv,
        "chunk_section_count": len(ordered_sections),
        "chunk_has_identity_context": "identity" in ordered_sections,
        "chunk_has_workflow_context": "workflow" in ordered_sections,
        "chunk_has_project_context": "project" in ordered_sections,
        "chunk_has_action_context": "action" in ordered_sections,
        "chunk_has_issue_context": "issue" in ordered_sections,
        "chunk_has_activity_context": "activity" in ordered_sections,
        "chunk_has_resolution_context": "resolution" in ordered_sections,
        "chunk_has_attachment_context": "attachments" in ordered_sections,
        "chunk_section_line_counts": dict(section_counts),
    }


def _entity_label_quality_summary(
    rows: list[dict[str, Any]],
    *,
    label_field: str,
    label_source_field: str,
    readable_sources: set[str],
    fallback_sources: set[str],
) -> dict[str, Any]:
    total_chunks = len(rows)
    present_rows = [row for row in rows if _present(row.get(label_field))]
    readable_rows = [row for row in present_rows if not _looks_like_identifier(row.get(label_field))]
    identifier_like_rows = [row for row in present_rows if _looks_like_identifier(row.get(label_field))]

    distinct_values = sorted({str(row.get(label_field)).strip() for row in present_rows if _present(row.get(label_field))})
    identifier_like_values = sorted({str(row.get(label_field)).strip() for row in identifier_like_rows if _present(row.get(label_field))})

    source_counts: dict[str, int] = {}
    for row in present_rows:
        source = str(row.get(label_source_field) or "missing")
        source_counts[source] = source_counts.get(source, 0) + 1

    readable_source_chunks = sum(source_counts.get(source, 0) for source in readable_sources)
    fallback_source_chunks = sum(source_counts.get(source, 0) for source in fallback_sources)
    other_source_chunks = max(len(present_rows) - readable_source_chunks - fallback_source_chunks, 0)

    return {
        "present_chunks": len(present_rows),
        "present_ratio": round((len(present_rows) / total_chunks), 4) if total_chunks else 0.0,
        "missing_chunks": total_chunks - len(present_rows),
        "missing_ratio": round(((total_chunks - len(present_rows)) / total_chunks), 4) if total_chunks else 0.0,
        "readable_chunks": len(readable_rows),
        "readable_ratio": round((len(readable_rows) / total_chunks), 4) if total_chunks else 0.0,
        "identifier_like_chunks": len(identifier_like_rows),
        "identifier_like_ratio": round((len(identifier_like_rows) / total_chunks), 4) if total_chunks else 0.0,
        "readable_source_chunks": readable_source_chunks,
        "readable_source_ratio": round((readable_source_chunks / total_chunks), 4) if total_chunks else 0.0,
        "fallback_source_chunks": fallback_source_chunks,
        "fallback_source_ratio": round((fallback_source_chunks / total_chunks), 4) if total_chunks else 0.0,
        "other_source_chunks": other_source_chunks,
        "other_source_ratio": round((other_source_chunks / total_chunks), 4) if total_chunks else 0.0,
        "distinct_value_count": len(distinct_values),
        "identifier_like_distinct_value_count": len(identifier_like_values),
        "identifier_like_distinct_value_sample": identifier_like_values[:10],
        "source_counts": {
            key: {
                "chunks": value,
                "ratio": round((value / total_chunks), 4) if total_chunks else 0.0,
            }
            for key, value in sorted(source_counts.items())
        },
    }


def _json_presence_clause(paths: list[str], *, kind: str = "text") -> str:
    clauses = []
    for path in paths:
        if kind == "value":
            clauses.append(f"json_type(raw_json, '{path}') IS NOT NULL")
        else:
            clauses.append(
                "(json_type(raw_json, '{path}') IS NOT NULL AND TRIM(CAST(json_extract(raw_json, '{path}') AS TEXT)) != '')".format(path=path)
            )
    return " OR ".join(clauses) or "0"


SOURCE_METADATA_FIELDS: dict[str, dict[str, Any]] = {
    "support_group_name": {
        "tickets": {"paths": ["$.support_group_name"], "kind": "text"},
        "ticket_details": {"paths": ["$.support_group_name"], "kind": "text"},
    },
    "default_contract_name": {
        "tickets": {"paths": ["$.default_contract_name"], "kind": "text"},
        "ticket_details": {"paths": ["$.default_contract_name"], "kind": "text"},
    },
    "location_name": {
        "tickets": {"paths": ["$.location_name"], "kind": "text"},
        "ticket_details": {"paths": ["$.location_name"], "kind": "text"},
    },
    "account_location_name": {
        "tickets": {"paths": ["$.account_location_name"], "kind": "text"},
        "ticket_details": {"paths": ["$.account_location_name"], "kind": "text"},
    },
    "department_key": {
        "tickets": {"paths": ["$.department_key"], "kind": "value"},
        "ticket_details": {"paths": ["$.department_key"], "kind": "value"},
    },
    "ticket_number": {
        "tickets": {"paths": ["$.number"], "kind": "text"},
    },
    "ticket_key": {
        "tickets": {"paths": ["$.key"], "kind": "text"},
    },
    "technician_email": {
        "tickets": {"paths": ["$.technician_email", "$.tech_email"], "kind": "text"},
    },
    "user_phone": {
        "tickets": {"paths": ["$.user_phone"], "kind": "text"},
        "ticket_details": {"paths": ["$.user_phone"], "kind": "text"},
    },
    "user_created_email": {
        "tickets": {"paths": ["$.user_created_email"], "kind": "text"},
        "ticket_details": {"paths": ["$.user_created_email"], "kind": "text"},
    },
    "technician_type": {
        "tickets": {"paths": ["$.tech_type"], "kind": "text"},
        "ticket_details": {"paths": ["$.tech_type"], "kind": "text"},
    },
    "project_id": {
        "tickets": {"paths": ["$.project_id"], "kind": "value"},
        "ticket_details": {"paths": ["$.project_id"], "kind": "value"},
    },
    "project_name": {
        "tickets": {"paths": ["$.project_name"], "kind": "text"},
        "ticket_details": {"paths": ["$.project_name"], "kind": "text"},
    },
    "scheduled_ticket_id": {
        "tickets": {"paths": ["$.scheduled_ticket_id"], "kind": "value"},
        "ticket_details": {"paths": ["$.scheduled_ticket_id"], "kind": "value"},
    },
    "related_tickets_count": {
        "tickets": {"paths": ["$.related_tickets_count"], "kind": "value"},
        "ticket_details": {"paths": ["$.related_tickets_count"], "kind": "value"},
    },
    "estimated_time": {
        "tickets": {"paths": ["$.estimated_time"], "kind": "value"},
        "ticket_details": {"paths": ["$.estimated_time"], "kind": "value"},
    },
    "remaining_hours": {
        "tickets": {"paths": ["$.remaining_hours"], "kind": "value"},
        "ticket_details": {"paths": ["$.remaining_hours"], "kind": "value"},
    },
    "total_hours": {
        "tickets": {"paths": ["$.total_hours"], "kind": "value"},
        "ticket_details": {"paths": ["$.total_hours"], "kind": "value"},
    },
    "total_time_in_minutes": {
        "tickets": {"paths": ["$.total_time_in_minutes"], "kind": "value"},
        "ticket_details": {"paths": ["$.total_time_in_minutes"], "kind": "value"},
    },
    "labor_cost": {
        "tickets": {"paths": ["$.labor_cost"], "kind": "value"},
        "ticket_details": {"paths": ["$.labor_cost"], "kind": "value"},
    },
    "percentage_complete": {
        "tickets": {"paths": ["$.percentage_complete"], "kind": "value"},
        "ticket_details": {"paths": ["$.percentage_complete"], "kind": "value"},
    },
    "days_old_in_minutes": {
        "tickets": {"paths": ["$.days_old_in_minutes"], "kind": "value"},
        "ticket_details": {"paths": ["$.days_old_in_minutes"], "kind": "value"},
    },
    "waiting_minutes": {
        "tickets": {"paths": ["$.waiting_minutes"], "kind": "value"},
        "ticket_details": {"paths": ["$.waiting_minutes"], "kind": "value"},
    },
    "confirmed_by_name": {
        "tickets": {"paths": ["$.confirmed_by_name"], "kind": "text"},
        "ticket_details": {"paths": ["$.confirmed_by_name"], "kind": "text"},
    },
    "confirmed_date": {
        "tickets": {"paths": ["$.confirmed_date"], "kind": "text"},
        "ticket_details": {"paths": ["$.confirmed_date"], "kind": "text"},
    },
    "is_via_email_parser": {
        "tickets": {"paths": ["$.is_via_email_parser"], "kind": "value"},
        "ticket_details": {"paths": ["$.is_via_email_parser"], "kind": "value"},
    },
    "is_handle_by_callcentre": {
        "tickets": {"paths": ["$.is_handle_by_callcentre"], "kind": "value"},
        "ticket_details": {"paths": ["$.is_handle_by_callcentre"], "kind": "value"},
    },
    "is_waiting_on_response": {
        "ticket_details": {"paths": ["$.is_waiting_on_response"], "kind": "value"},
    },
    "is_resolved": {
        "ticket_details": {"paths": ["$.is_resolved"], "kind": "value"},
    },
    "is_confirmed": {
        "ticket_details": {"paths": ["$.is_confirmed"], "kind": "value"},
    },
}


def _get_source_metadata_coverage(
    db_path: Path,
    document_metadata_coverage: dict[str, dict[str, float]],
    metadata_coverage: dict[str, dict[str, float]],
    total_documents: int,
    total_chunks: int,
) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    with connect(db_path) as conn:
        for field, config in SOURCE_METADATA_FIELDS.items():
            ticket_clause = _json_presence_clause(
                config.get("tickets", {}).get("paths", []),
                kind=config.get("tickets", {}).get("kind", "text"),
            ) if config.get("tickets") else "0"
            detail_clause = _json_presence_clause(
                config.get("ticket_details", {}).get("paths", []),
                kind=config.get("ticket_details", {}).get("kind", "text"),
            ) if config.get("ticket_details") else "0"
            counts = conn.execute(
                f"""
                SELECT
                    (SELECT COUNT(DISTINCT id) FROM tickets WHERE {ticket_clause}) AS ticket_rows,
                    (SELECT COUNT(DISTINCT ticket_id) FROM ticket_details WHERE {detail_clause}) AS detail_rows,
                    (
                        SELECT COUNT(*)
                        FROM (
                            SELECT id AS ticket_id FROM tickets WHERE {ticket_clause}
                            UNION
                            SELECT ticket_id FROM ticket_details WHERE {detail_clause}
                        )
                    ) AS source_documents
                """
            ).fetchone()
            source_documents = int(counts["source_documents"] or 0)
            materialized_documents = int(document_metadata_coverage.get(field, {}).get("documents", 0) or 0)
            materialized_chunks = int(metadata_coverage.get(field, {}).get("chunks", 0) or 0)
            if source_documents == 0:
                status = "upstream_absent"
            elif materialized_documents == 0:
                status = "missing_materialization"
            elif materialized_documents < source_documents:
                status = "partial_materialization"
            else:
                status = "materialized"
            summary[field] = {
                "ticket_rows": int(counts["ticket_rows"] or 0),
                "detail_rows": int(counts["detail_rows"] or 0),
                "source_documents": source_documents,
                "source_document_ratio": round((source_documents / total_documents), 4) if total_documents else 0.0,
                "materialized_documents": materialized_documents,
                "materialized_document_ratio": round((materialized_documents / total_documents), 4) if total_documents else 0.0,
                "materialized_chunks": materialized_chunks,
                "materialized_chunk_ratio": round((materialized_chunks / total_chunks), 4) if total_chunks else 0.0,
                "promotion_gap_documents": max(source_documents - materialized_documents, 0),
                "status": status,
            }
    return summary


FRESHNESS_LAG_BUCKETS: list[tuple[str, int | None]] = [
    ("lag_le_15m", 15),
    ("lag_le_1h", 60),
    ("lag_le_6h", 360),
    ("lag_le_24h", 1440),
    ("lag_gt_24h", None),
]


FRESHNESS_SAMPLE_LIMIT = 10


def _freshness_bucket_name(lag_minutes: float) -> str:
    for name, upper_bound_minutes in FRESHNESS_LAG_BUCKETS:
        if upper_bound_minutes is None or lag_minutes <= upper_bound_minutes:
            return name
    return "lag_gt_24h"


def _build_freshness_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "earliest_updated_at": min((row.get("updated_at") for row in rows if row.get("updated_at")), default=None),
        "latest_updated_at": max((row.get("updated_at") for row in rows if row.get("updated_at")), default=None),
        "earliest_chunk_synced_at": min((row.get("chunk_synced_at") for row in rows if row.get("chunk_synced_at")), default=None),
        "latest_chunk_synced_at": max((row.get("chunk_synced_at") for row in rows if row.get("chunk_synced_at")), default=None),
        "document_count_with_timestamps": 0,
        "documents_materialized_current_or_ahead": 0,
        "documents_materialized_behind": 0,
        "documents_missing_updated_at": 0,
        "documents_missing_chunk_synced_at": 0,
        "documents_missing_any_timestamp": 0,
        "lagging_document_ratio": 0.0,
        "max_lag_minutes": 0.0,
        "avg_lag_minutes": 0.0,
        "status_breakdown": {
            "Open": {"documents": 0, "lagging_documents": 0, "lagging_ratio": 0.0},
            "Closed": {"documents": 0, "lagging_documents": 0, "lagging_ratio": 0.0},
            "Other": {"documents": 0, "lagging_documents": 0, "lagging_ratio": 0.0},
        },
        "lag_buckets": {
            name: {"documents": 0, "ratio": 0.0}
            for name, _upper_bound_minutes in FRESHNESS_LAG_BUCKETS
        },
        "top_lagging_documents": [],
    }
    if not rows:
        return summary

    docs_by_id: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        docs_by_id.setdefault(str(row["doc_id"]), []).append(row)

    lagging_samples: list[dict[str, Any]] = []
    lag_minutes_values: list[float] = []

    for doc_id, doc_rows in docs_by_id.items():
        first = doc_rows[0]
        updated_at = parse_sherpadesk_timestamp(first.get("updated_at"))
        chunk_sync_times = [
            parse_sherpadesk_timestamp(row.get("chunk_synced_at"))
            for row in doc_rows
            if row.get("chunk_synced_at")
        ]
        latest_chunk_synced_at = max((value for value in chunk_sync_times if value is not None), default=None)

        if updated_at is None:
            summary["documents_missing_updated_at"] += 1
        if latest_chunk_synced_at is None:
            summary["documents_missing_chunk_synced_at"] += 1
        if updated_at is None or latest_chunk_synced_at is None:
            summary["documents_missing_any_timestamp"] += 1
            continue

        summary["document_count_with_timestamps"] += 1
        status_value = str(first.get("status") or "").strip().lower()
        if status_value == "open":
            status_key = "Open"
        elif status_value == "closed":
            status_key = "Closed"
        else:
            status_key = "Other"
        summary["status_breakdown"][status_key]["documents"] += 1

        lag_minutes = round((updated_at - latest_chunk_synced_at).total_seconds() / 60.0, 2)
        if lag_minutes > 0:
            summary["documents_materialized_behind"] += 1
            summary["status_breakdown"][status_key]["lagging_documents"] += 1
            lag_minutes_values.append(lag_minutes)
            bucket_name = _freshness_bucket_name(lag_minutes)
            summary["lag_buckets"][bucket_name]["documents"] += 1
            raw_ticket_id = first.get("ticket_id")
            if raw_ticket_id is None:
                ticket_id: Any = None
            else:
                candidate_ticket_id = str(raw_ticket_id).strip()
                ticket_id = int(candidate_ticket_id) if candidate_ticket_id.isdigit() else raw_ticket_id
            lagging_samples.append(
                {
                    "doc_id": doc_id,
                    "ticket_id": ticket_id,
                    "status": first.get("status"),
                    "account": first.get("account"),
                    "technician": first.get("technician"),
                    "updated_at": first.get("updated_at"),
                    "latest_chunk_synced_at": latest_chunk_synced_at.isoformat(),
                    "lag_minutes": lag_minutes,
                }
            )
        else:
            summary["documents_materialized_current_or_ahead"] += 1

    valid_documents = summary["document_count_with_timestamps"]
    summary["lagging_document_ratio"] = round((summary["documents_materialized_behind"] / valid_documents), 4) if valid_documents else 0.0
    if lag_minutes_values:
        summary["max_lag_minutes"] = round(max(lag_minutes_values), 2)
        summary["avg_lag_minutes"] = round(sum(lag_minutes_values) / len(lag_minutes_values), 2)

    for bucket in summary["lag_buckets"].values():
        bucket["ratio"] = round((bucket["documents"] / valid_documents), 4) if valid_documents else 0.0
    for status_stats in summary["status_breakdown"].values():
        documents = int(status_stats["documents"] or 0)
        lagging_documents = int(status_stats["lagging_documents"] or 0)
        status_stats["lagging_ratio"] = round((lagging_documents / documents), 4) if documents else 0.0

    lagging_samples.sort(key=lambda item: (-float(item["lag_minutes"]), str(item.get("ticket_id") or "")))
    summary["top_lagging_documents"] = lagging_samples[:FRESHNESS_SAMPLE_LIMIT]
    return summary


def _build_source_backed_metadata_summary(source_metadata_coverage: dict[str, Any]) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    fields_with_promotion_gap: list[dict[str, Any]] = []
    upstream_absent_fields: list[dict[str, Any]] = []
    for field, stats in source_metadata_coverage.items():
        status = str(stats.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        promotion_gap = int(stats.get("promotion_gap_documents") or 0)
        if promotion_gap > 0:
            fields_with_promotion_gap.append(
                {
                    "field": field,
                    "status": status,
                    "source_documents": int(stats.get("source_documents") or 0),
                    "materialized_documents": int(stats.get("materialized_documents") or 0),
                    "promotion_gap_documents": promotion_gap,
                    "materialized_document_ratio": float(stats.get("materialized_document_ratio") or 0.0),
                }
            )
        if status == "upstream_absent":
            upstream_absent_fields.append(
                {
                    "field": field,
                    "ticket_rows": int(stats.get("ticket_rows") or 0),
                    "detail_rows": int(stats.get("detail_rows") or 0),
                    "source_documents": int(stats.get("source_documents") or 0),
                    "materialized_documents": int(stats.get("materialized_documents") or 0),
                }
            )

    fields_with_promotion_gap.sort(key=lambda item: (-item["promotion_gap_documents"], item["field"]))
    upstream_absent_fields.sort(key=lambda item: item["field"])
    return {
        "fields": source_metadata_coverage,
        "status_counts": status_counts,
        "fields_with_promotion_gap": fields_with_promotion_gap,
        "upstream_absent_fields": upstream_absent_fields,
    }


def _build_materialization_freshness_lag_summary(freshness_summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "windows": {
            "earliest_updated_at": freshness_summary.get("earliest_updated_at"),
            "latest_updated_at": freshness_summary.get("latest_updated_at"),
            "earliest_chunk_synced_at": freshness_summary.get("earliest_chunk_synced_at"),
            "latest_chunk_synced_at": freshness_summary.get("latest_chunk_synced_at"),
        },
        "lag_summary": {
            "document_count_with_timestamps": int(freshness_summary.get("document_count_with_timestamps") or 0),
            "documents_materialized_current_or_ahead": int(freshness_summary.get("documents_materialized_current_or_ahead") or 0),
            "documents_materialized_behind": int(freshness_summary.get("documents_materialized_behind") or 0),
            "documents_missing_updated_at": int(freshness_summary.get("documents_missing_updated_at") or 0),
            "documents_missing_chunk_synced_at": int(freshness_summary.get("documents_missing_chunk_synced_at") or 0),
            "documents_missing_any_timestamp": int(freshness_summary.get("documents_missing_any_timestamp") or 0),
            "lagging_document_ratio": float(freshness_summary.get("lagging_document_ratio") or 0.0),
            "avg_lag_minutes": float(freshness_summary.get("avg_lag_minutes") or 0.0),
            "max_lag_minutes": float(freshness_summary.get("max_lag_minutes") or 0.0),
        },
        "status_breakdown": freshness_summary.get("status_breakdown", {}),
        "lag_buckets": freshness_summary.get("lag_buckets", {}),
        "top_lagging_documents": freshness_summary.get("top_lagging_documents", []),
    }


def get_retrieval_readiness_summary(db_path: Path, limit: int | None = None) -> dict[str, Any]:
    initialize_db(db_path)
    rows = _load_rows(db_path, limit=limit)
    vector = get_vector_index_status(db_path)
    materialization = get_ticket_document_materialization_status(db_path)

    for row in rows:
        row.update(_infer_chunk_sections(row.get("text")))

    chunk_count = len(rows)
    document_ids = {row["doc_id"] for row in rows}
    accounts = sorted({row.get("account") for row in rows if row.get("account")})
    technicians = sorted({row.get("technician") for row in rows if row.get("technician")})
    statuses = sorted({row.get("status") for row in rows if row.get("status")})
    priorities = sorted({row.get("priority") for row in rows if row.get("priority")})
    categories = sorted({row.get("category") for row in rows if row.get("category")})
    class_names = sorted({row.get("class_name") for row in rows if row.get("class_name")})
    submission_categories = sorted({row.get("submission_category") for row in rows if row.get("submission_category")})
    resolution_categories = sorted({row.get("resolution_category") for row in rows if row.get("resolution_category")})
    departments = sorted({row.get("department_label") for row in rows if row.get("department_label")})
    attachment_extensions = sorted({value for row in rows for value in _split_csv_values(row.get("attachment_extensions_csv"))})
    attachment_kinds = sorted({value for row in rows for value in _split_csv_values(row.get("attachment_kinds_csv"))})
    chunk_primary_sections = sorted({row.get("chunk_primary_section") for row in rows if row.get("chunk_primary_section")})
    chunk_section_labels = sorted({value for row in rows for value in (row.get("chunk_section_labels") or []) if value})

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
        "chunk_primary_section": lambda row: _present(row.get("chunk_primary_section")),
        "chunk_section_labels": lambda row: bool(row.get("chunk_section_labels")),
        "chunk_has_identity_context": lambda row: bool(row.get("chunk_has_identity_context")),
        "chunk_has_workflow_context": lambda row: bool(row.get("chunk_has_workflow_context")),
        "chunk_has_project_context": lambda row: bool(row.get("chunk_has_project_context")),
        "chunk_has_action_context": lambda row: bool(row.get("chunk_has_action_context")),
        "chunk_has_issue_context": lambda row: bool(row.get("chunk_has_issue_context")),
        "chunk_has_activity_context": lambda row: bool(row.get("chunk_has_activity_context")),
        "chunk_has_resolution_context": lambda row: bool(row.get("chunk_has_resolution_context")),
        "chunk_has_attachment_context": lambda row: bool(row.get("chunk_has_attachment_context")),
        "class_name": lambda row: _present(row.get("class_name")),
        "submission_category": lambda row: _present(row.get("submission_category")),
        "resolution_category": lambda row: _present(row.get("resolution_category")),
        "user_email": lambda row: _present(row.get("user_email")),
        "cleaned_subject": lambda row: _present(row.get("cleaned_subject")),
        "public_log_count": lambda row: _present(row.get("public_log_count")),
        "internal_log_count": lambda row: _present(row.get("internal_log_count")),
        "waiting_log_count": lambda row: _present(row.get("waiting_log_count")),
        "response_log_count": lambda row: _present(row.get("response_log_count")),
        "resolution_log_count": lambda row: _present(row.get("resolution_log_count")),
        "latest_log_date": lambda row: _present(row.get("latest_log_date")),
        "latest_public_log_date": lambda row: _present(row.get("latest_public_log_date")),
        "latest_internal_log_date": lambda row: _present(row.get("latest_internal_log_date")),
        "latest_waiting_log_date": lambda row: _present(row.get("latest_waiting_log_date")),
        "latest_resolution_log_date": lambda row: _present(row.get("latest_resolution_log_date")),
        "cleaned_initial_post": lambda row: _present(row.get("cleaned_initial_post")),
        "cleaned_detail_note": lambda row: _present(row.get("cleaned_detail_note")),
        "cleaned_workpad": lambda row: _present(row.get("cleaned_workpad")),
        "cleaned_followup_note": lambda row: _present(row.get("cleaned_followup_note")),
        "cleaned_explicit_followup_note": lambda row: _present(row.get("cleaned_explicit_followup_note")),
        "cleaned_waiting_log_note": lambda row: _present(row.get("cleaned_waiting_log_note")),
        "cleaned_request_completion_note": lambda row: _present(row.get("cleaned_request_completion_note")),
        "cleaned_next_step": lambda row: _present(row.get("cleaned_next_step")),
        "cleaned_action_cue": lambda row: _present(row.get("cleaned_action_cue")),
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
        "department_label": lambda row: _present(row.get("department_label")),
        "ticket_number": lambda row: _present(row.get("ticket_number")),
        "ticket_key": lambda row: _present(row.get("ticket_key")),
        "attachment_extensions": lambda row: _present(row.get("attachment_extensions_csv")),
        "attachment_kinds": lambda row: _present(row.get("attachment_kinds_csv")),
        "attachment_kind_primary": lambda row: _present(row.get("attachment_kind_primary")),
        "attachment_total_size_bytes": lambda row: _present(row.get("attachment_total_size_bytes")),
        "attachment_size_known_count": lambda row: int(row.get("attachment_size_known_count") or 0) > 0,
        "attachment_image_count": lambda row: int(row.get("attachment_image_count") or 0) > 0,
        "attachment_document_count": lambda row: int(row.get("attachment_document_count") or 0) > 0,
        "attachment_spreadsheet_count": lambda row: int(row.get("attachment_spreadsheet_count") or 0) > 0,
        "attachment_archive_count": lambda row: int(row.get("attachment_archive_count") or 0) > 0,
        "attachment_log_count": lambda row: int(row.get("attachment_log_count") or 0) > 0,
        "attachment_data_count": lambda row: int(row.get("attachment_data_count") or 0) > 0,
        "attachment_audio_count": lambda row: int(row.get("attachment_audio_count") or 0) > 0,
        "attachment_video_count": lambda row: int(row.get("attachment_video_count") or 0) > 0,
        "attachment_message_count": lambda row: int(row.get("attachment_message_count") or 0) > 0,
        "attachment_other_count": lambda row: int(row.get("attachment_other_count") or 0) > 0,
        "attachment_unknown_count": lambda row: int(row.get("attachment_unknown_count") or 0) > 0,
        "technician_email": lambda row: _present(row.get("technician_email")),
        "user_phone": lambda row: _present(row.get("user_phone")),
        "user_created_name": lambda row: _present(row.get("user_created_name")),
        "user_created_email": lambda row: _present(row.get("user_created_email")),
        "technician_type": lambda row: _present(row.get("technician_type")),
        "project_id": lambda row: _present(row.get("project_id")),
        "project_name": lambda row: _present(row.get("project_name")),
        "scheduled_ticket_id": lambda row: _present(row.get("scheduled_ticket_id")),
        "related_tickets_count": lambda row: _present(row.get("related_tickets_count")),
        "estimated_time": lambda row: _present(row.get("estimated_time")),
        "remaining_hours": lambda row: _present(row.get("remaining_hours")),
        "total_hours": lambda row: _present(row.get("total_hours")),
        "total_time_in_minutes": lambda row: _present(row.get("total_time_in_minutes")),
        "labor_cost": lambda row: _present(row.get("labor_cost")),
        "percentage_complete": lambda row: _present(row.get("percentage_complete")),
        "has_project_context": lambda row: bool(row.get("has_project_context")),
        "has_scheduled_parent": lambda row: bool(row.get("has_scheduled_parent")),
        "has_related_tickets": lambda row: bool(row.get("has_related_tickets")),
        "has_effort_tracking": lambda row: bool(row.get("has_effort_tracking")),
        "has_public_logs": lambda row: bool(row.get("has_public_logs")),
        "has_internal_logs": lambda row: bool(row.get("has_internal_logs")),
        "has_waiting_logs": lambda row: bool(row.get("has_waiting_logs")),
        "has_resolution_logs": lambda row: bool(row.get("has_resolution_logs")),
        "days_old_in_minutes": lambda row: _present(row.get("days_old_in_minutes")),
        "waiting_minutes": lambda row: _present(row.get("waiting_minutes")),
        "confirmed_by_name": lambda row: _present(row.get("confirmed_by_name")),
        "confirmed_date": lambda row: _present(row.get("confirmed_date")),
        "cleaned_confirmed_note": lambda row: _present(row.get("cleaned_confirmed_note")),
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
    document_metadata_coverage = {}
    for key, predicate in metadata_fields.items():
        covered = sum(1 for row in rows if predicate(row))
        covered_docs = {str(row["doc_id"]) for row in rows if predicate(row)}
        metadata_coverage[key] = {
            "chunks": covered,
            "ratio": round(covered / chunk_count, 4) if chunk_count else 0.0,
        }
        document_metadata_coverage[key] = {
            "documents": len(covered_docs),
            "ratio": round(len(covered_docs) / len(document_ids), 4) if document_ids else 0.0,
        }

    label_source_summary = {}
    for field in ("account_label_source", "user_label_source", "technician_label_source", "department_label_source", "followup_note_source", "action_cue_source"):
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

    source_metadata_coverage = _get_source_metadata_coverage(
        db_path,
        document_metadata_coverage=document_metadata_coverage,
        metadata_coverage=metadata_coverage,
        total_documents=len(document_ids),
        total_chunks=chunk_count,
    )
    source_backed_metadata = _build_source_backed_metadata_summary(source_metadata_coverage)
    freshness_summary = _build_freshness_summary(rows)
    materialization_freshness_lag = _build_materialization_freshness_lag_summary(freshness_summary)

    entity_label_quality = {
        "account": _entity_label_quality_summary(
            rows,
            label_field="account",
            label_source_field="account_label_source",
            readable_sources={"joined", "raw"},
            fallback_sources={"id"},
        ),
        "user": _entity_label_quality_summary(
            rows,
            label_field="user_name",
            label_source_field="user_label_source",
            readable_sources={"joined", "raw", "email"},
            fallback_sources={"id"},
        ),
        "technician": _entity_label_quality_summary(
            rows,
            label_field="technician",
            label_source_field="technician_label_source",
            readable_sources={"joined", "raw"},
            fallback_sources={"id"},
        ),
        "department": _entity_label_quality_summary(
            rows,
            label_field="department_label",
            label_source_field="department_label_source",
            readable_sources={"support_group_name", "class_name", "submission_category", "department_key"},
            fallback_sources=set(),
        ),
    }

    return {
        "chunk_count": chunk_count,
        "document_count": len(document_ids),
        "limit_applied": limit,
        "freshness": freshness_summary,
        "chunk_quality": {
            "avg_chunk_chars": round(sum(chunk_lengths) / chunk_count, 1) if chunk_count else 0.0,
            "min_chunk_chars": min(chunk_lengths, default=0),
            "max_chunk_chars": max(chunk_lengths, default=0),
            "tiny_chunk_count": sum(1 for length in chunk_lengths if length < 200),
            "target_or_smaller_count": sum(1 for length in chunk_lengths if length <= 1800),
            "over_target_chunk_count": sum(1 for length in chunk_lengths if length > 1800),
            "multi_chunk_document_count": sum(1 for count in chunk_counts_by_doc.values() if count > 1),
        },
        "document_chunk_topology": {
            "avg_chunks_per_document": round((chunk_count / len(document_ids)), 4) if document_ids else 0.0,
            "min_chunks_per_document": min(chunk_counts_by_doc.values(), default=0),
            "max_chunks_per_document": max(chunk_counts_by_doc.values(), default=0),
            "single_chunk_document_count": sum(1 for count in chunk_counts_by_doc.values() if count == 1),
            "multi_chunk_document_count": sum(1 for count in chunk_counts_by_doc.values() if count > 1),
            "multi_chunk_document_ratio": round(
                (
                    sum(1 for count in chunk_counts_by_doc.values() if count > 1)
                    / len(document_ids)
                ),
                4,
            ) if document_ids else 0.0,
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
            "class_names": class_names,
            "class_name_count": len(class_names),
            "submission_categories": submission_categories,
            "submission_category_count": len(submission_categories),
            "resolution_categories": resolution_categories,
            "resolution_category_count": len(resolution_categories),
            "departments": departments,
            "department_count": len(departments),
            "attachment_extensions": attachment_extensions,
            "attachment_extension_count": len(attachment_extensions),
            "attachment_kinds": attachment_kinds,
            "attachment_kind_count": len(attachment_kinds),
            "chunk_primary_sections": chunk_primary_sections,
            "chunk_primary_section_count": len(chunk_primary_sections),
            "chunk_section_labels": chunk_section_labels,
            "chunk_section_label_count": len(chunk_section_labels),
        },
        "metadata_coverage": metadata_coverage,
        "document_metadata_coverage": document_metadata_coverage,
        "source_metadata_coverage": source_metadata_coverage,
        "source_backed_metadata": source_backed_metadata,
        "label_source_summary": label_source_summary,
        "entity_label_quality": entity_label_quality,
        "materialization_freshness_lag": materialization_freshness_lag,
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
