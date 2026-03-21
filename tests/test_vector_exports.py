import json
from pathlib import Path

from sherpamind.db import (
    initialize_db,
    replace_ticket_document_chunks,
    replace_ticket_documents,
    upsert_ticket_details,
    upsert_tickets,
)
from sherpamind.documents import DOCUMENT_MATERIALIZATION_VERSION
from sherpamind.vector_exports import export_embedding_manifest, export_embedding_ready_chunks, get_retrieval_readiness_summary


def seed(db: Path) -> None:
    initialize_db(db)
    upsert_tickets(
        db,
        [
            {
                "id": 101,
                "account_id": 1,
                "user_id": 2,
                "tech_id": 3,
                "subject": "hello",
                "status": "Open",
                "priority_name": "High",
                "creation_category_name": "Hardware",
                "created_time": "2026-03-19T01:00:00Z",
                "updated_time": "2026-03-19T03:00:00Z",
                "closed_time": None,
                "number": "T-101",
                "key": "abc-101",
                "technician_email": "tech@example.com",
                "user_phone": "520-555-0101",
                "account_location_name": "HQ Campus",
                "is_via_email_parser": True,
                "is_handle_by_callcentre": False,
            },
            {
                "id": 102,
                "account_id": 44,
                "user_id": 55,
                "tech_id": 66,
                "subject": "numeric fallback labels",
                "status": "Closed",
                "priority_name": "Low",
                "creation_category_name": "Software",
                "created_time": "2026-03-18T01:00:00Z",
                "updated_time": "2026-03-18T03:00:00Z",
                "closed_time": "2026-03-18T04:00:00Z",
                "number": "T-102",
                "key": "abc-102",
            },
        ],
        synced_at="2026-03-19T01:00:00Z",
    )
    upsert_ticket_details(
        db,
        [{
            "id": 101,
            "default_contract_name": "Gold",
            "location_name": "HQ",
            "department_key": "managed-services",
            "user_created_email": "dispatcher@example.com",
            "tech_type": "dispatcher",
            "days_old_in_minutes": 1440,
            "waiting_minutes": 30,
            "confirmed_by_name": "Tech Lead",
            "confirmed_date": "2026-03-19T05:00:00Z",
            "is_waiting_on_response": True,
            "is_resolved": False,
            "is_confirmed": True,
            "attachments": [{"id": 9, "name": "photo.png"}],
            "ticketlogs": [{"id": 1, "log_type": "Response", "note": "closed"}],
            "timelogs": [],
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    replace_ticket_documents(
        db,
        [
            {
                "doc_id": "ticket:101",
                "ticket_id": 101,
                "status": "Open",
                "account": "Acme",
                "user_name": "Alice",
                "technician": "Tech One",
                "updated_at": "2026-03-19T03:00:00Z",
                "text": "hello",
                "metadata": {
                    "priority": "High",
                    "category": "Hardware",
                    "class_name": "Service Request",
                    "submission_category": "Portal",
                    "resolution_category": "Completed",
                    "attachments_count": 1,
                    "has_attachments": True,
                    "ticketlogs_count": 5,
                    "timelogs_count": 0,
                    "cleaned_subject": "hello",
                    "cleaned_initial_post": "Help me",
                    "cleaned_followup_note": "Waiting on customer reply",
                    "cleaned_request_completion_note": "Complete during maintenance window",
                    "cleaned_next_step": "Call back tomorrow",
                    "cleaned_action_cue": "Call back tomorrow",
                    "action_cue_source": "next_step",
                    "cleaned_latest_response_note": "We are checking now",
                    "latest_response_date": "2026-03-19T09:15:00Z",
                    "cleaned_resolution_log_note": "Closed after maintenance",
                    "resolution_log_date": "2026-03-19T10:00:00Z",
                    "followup_date": "2026-03-20T10:00:00Z",
                    "request_completion_date": "2026-03-21T17:00:00Z",
                    "has_next_step": True,
                    "recent_log_types_csv": "Initial Post, Response",
                    "initial_response_present": True,
                    "user_email": "alice@example.com",
                    "support_group_name": "Managed Services",
                    "default_contract_name": "Gold",
                    "location_name": "HQ",
                    "account_location_name": "HQ Campus",
                    "department_key": "managed-services",
                    "department_label": "Managed Services",
                    "department_label_source": "support_group_name",
                    "ticket_number": "T-101",
                    "ticket_key": "abc-101",
                    "technician_email": "tech@example.com",
                    "user_phone": "520-555-0101",
                    "user_created_name": "Casey Dispatcher",
                    "user_created_email": "dispatcher@example.com",
                    "technician_type": "dispatcher",
                    "days_old_in_minutes": 1440,
                    "waiting_minutes": 30,
                    "confirmed_by_name": "Tech Lead",
                    "confirmed_date": "2026-03-19T05:00:00Z",
                    "cleaned_confirmed_note": "User confirmed the printer was fixed.",
                    "is_via_email_parser": True,
                    "is_handle_by_callcentre": False,
                    "is_waiting_on_response": True,
                    "is_resolved": False,
                    "is_confirmed": True,
                    "account_label_source": "raw",
                    "user_label_source": "email",
                    "technician_label_source": "joined",
                    "resolution_summary": "Closed successfully",
                    "has_resolution_summary": True,
                    "materialization_version": DOCUMENT_MATERIALIZATION_VERSION,
                },
                "content_hash": "abc",
            },
            {
                "doc_id": "ticket:102",
                "ticket_id": 102,
                "status": "Closed",
                "account": "44",
                "user_name": "55",
                "technician": "66",
                "updated_at": "2026-03-18T03:00:00Z",
                "text": "numeric fallback labels",
                "metadata": {
                    "priority": "Low",
                    "category": "Software",
                    "department_label": "Dispatch",
                    "department_label_source": "class_name",
                    "ticket_number": "T-102",
                    "ticket_key": "abc-102",
                    "account_label_source": "id",
                    "user_label_source": "id",
                    "technician_label_source": "id",
                    "materialization_version": DOCUMENT_MATERIALIZATION_VERSION,
                },
                "content_hash": "ghi",
            },
        ],
        synced_at="2026-03-19T01:00:00Z",
    )
    replace_ticket_document_chunks(
        db,
        [
            {
                "chunk_id": "ticket:101:chunk:0",
                "doc_id": "ticket:101",
                "ticket_id": 101,
                "chunk_index": 0,
                "text": "chunk text",
                "content_hash": "def",
            },
            {
                "chunk_id": "ticket:102:chunk:0",
                "doc_id": "ticket:102",
                "ticket_id": 102,
                "chunk_index": 0,
                "text": "numeric fallback labels",
                "content_hash": "jkl",
            },
        ],
        synced_at="2026-03-19T01:00:00Z",
    )


def test_export_embedding_ready_chunks(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed(db)
    output = tmp_path / "embedding.jsonl"
    result = export_embedding_ready_chunks(db, output)
    assert result["status"] == "ok"
    rows = [json.loads(line) for line in output.read_text().splitlines()]
    row = next(item for item in rows if item["id"] == "ticket:101:chunk:0")
    assert len(rows) == 2
    assert row["metadata"]["account"] == "Acme"
    assert row["metadata"]["chunk_chars"] == len("chunk text")
    assert row["metadata"]["chunk_count_for_doc"] == 1
    assert row["metadata"]["doc_total_chunk_chars"] == len("chunk text")
    assert row["metadata"]["is_first_chunk"] is True
    assert row["metadata"]["is_last_chunk"] is True
    assert row["metadata"]["is_multi_chunk_doc"] is False
    assert row["metadata"]["priority"] == "High"
    assert row["metadata"]["class_name"] == "Service Request"
    assert row["metadata"]["submission_category"] == "Portal"
    assert row["metadata"]["resolution_category"] == "Completed"
    assert row["metadata"]["ticketlogs_count"] == 5
    assert row["metadata"]["has_attachments"] is True
    assert row["metadata"]["cleaned_followup_note"] == "Waiting on customer reply"
    assert row["metadata"]["cleaned_request_completion_note"] == "Complete during maintenance window"
    assert row["metadata"]["cleaned_next_step"] == "Call back tomorrow"
    assert row["metadata"]["cleaned_action_cue"] == "Call back tomorrow"
    assert row["metadata"]["action_cue_source"] == "next_step"
    assert row["metadata"]["cleaned_latest_response_note"] == "We are checking now"
    assert row["metadata"]["latest_response_date"] == "2026-03-19T09:15:00Z"
    assert row["metadata"]["cleaned_resolution_log_note"] == "Closed after maintenance"
    assert row["metadata"]["resolution_log_date"] == "2026-03-19T10:00:00Z"
    assert row["metadata"]["followup_date"] == "2026-03-20T10:00:00Z"
    assert row["metadata"]["request_completion_date"] == "2026-03-21T17:00:00Z"
    assert row["metadata"]["support_group_name"] == "Managed Services"
    assert row["metadata"]["default_contract_name"] == "Gold"
    assert row["metadata"]["location_name"] == "HQ"
    assert row["metadata"]["account_location_name"] == "HQ Campus"
    assert row["metadata"]["department_key"] == "managed-services"
    assert row["metadata"]["department_label"] == "Managed Services"
    assert row["metadata"]["department_label_source"] == "support_group_name"
    assert row["metadata"]["ticket_number"] == "T-101"
    assert row["metadata"]["ticket_key"] == "abc-101"
    assert row["metadata"]["technician_email"] == "tech@example.com"
    assert row["metadata"]["user_phone"] == "520-555-0101"
    assert row["metadata"]["user_created_name"] == "Casey Dispatcher"
    assert row["metadata"]["user_created_email"] == "dispatcher@example.com"
    assert row["metadata"]["technician_type"] == "dispatcher"
    assert row["metadata"]["days_old_in_minutes"] == 1440
    assert row["metadata"]["waiting_minutes"] == 30
    assert row["metadata"]["confirmed_by_name"] == "Tech Lead"
    assert row["metadata"]["confirmed_date"] == "2026-03-19T05:00:00Z"
    assert row["metadata"]["cleaned_confirmed_note"] == "User confirmed the printer was fixed."
    assert row["metadata"]["is_via_email_parser"] is True
    assert row["metadata"]["is_handle_by_callcentre"] is False
    assert row["metadata"]["is_waiting_on_response"] is True
    assert row["metadata"]["is_resolved"] is False
    assert row["metadata"]["is_confirmed"] is True
    assert row["metadata"]["has_next_step"] is True
    assert row["metadata"]["recent_log_types"] == "Initial Post, Response"
    assert row["metadata"]["user_email"] == "alice@example.com"
    assert row["metadata"]["account_label_source"] == "raw"
    assert row["metadata"]["user_label_source"] == "email"
    assert row["metadata"]["technician_label_source"] == "joined"
    assert row["metadata"]["resolution_summary"] == "Closed successfully"
    assert row["metadata"]["has_resolution_summary"] is True


def test_get_retrieval_readiness_summary(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed(db)
    summary = get_retrieval_readiness_summary(db)
    assert summary["chunk_count"] == 2
    assert summary["document_count"] == 2
    assert summary["chunk_quality"]["max_chunk_chars"] == len("numeric fallback labels")
    assert summary["document_chunk_topology"]["avg_chunks_per_document"] == 1.0
    assert summary["document_chunk_topology"]["single_chunk_document_count"] == 2
    assert summary["document_chunk_topology"]["multi_chunk_document_count"] == 0
    assert summary["filter_facets"]["accounts"] == ["44", "Acme"]
    assert summary["filter_facets"]["priorities"] == ["High", "Low"]
    assert summary["filter_facets"]["class_names"] == ["Service Request"]
    assert summary["filter_facets"]["submission_categories"] == ["Portal"]
    assert summary["filter_facets"]["resolution_categories"] == ["Completed"]
    assert summary["filter_facets"]["departments"] == ["Dispatch", "Managed Services"]
    assert summary["metadata_coverage"]["cleaned_subject"]["chunks"] == 1
    assert summary["metadata_coverage"]["cleaned_followup_note"]["chunks"] == 1
    assert summary["metadata_coverage"]["cleaned_action_cue"]["chunks"] == 1
    assert summary["metadata_coverage"]["cleaned_latest_response_note"]["chunks"] == 1
    assert summary["metadata_coverage"]["cleaned_resolution_log_note"]["chunks"] == 1
    assert summary["metadata_coverage"]["class_name"]["chunks"] == 1
    assert summary["metadata_coverage"]["submission_category"]["chunks"] == 1
    assert summary["metadata_coverage"]["resolution_category"]["chunks"] == 1
    assert summary["metadata_coverage"]["support_group_name"]["chunks"] == 1
    assert summary["metadata_coverage"]["account_location_name"]["chunks"] == 1
    assert summary["metadata_coverage"]["department_key"]["chunks"] == 1
    assert summary["metadata_coverage"]["department_label"]["chunks"] == 2
    assert summary["metadata_coverage"]["ticket_number"]["chunks"] == 2
    assert summary["metadata_coverage"]["ticket_key"]["chunks"] == 2
    assert summary["metadata_coverage"]["technician_email"]["chunks"] == 1
    assert summary["metadata_coverage"]["user_phone"]["chunks"] == 1
    assert summary["metadata_coverage"]["user_created_name"]["chunks"] == 1
    assert summary["metadata_coverage"]["user_created_email"]["chunks"] == 1
    assert summary["metadata_coverage"]["technician_type"]["chunks"] == 1
    assert summary["metadata_coverage"]["days_old_in_minutes"]["chunks"] == 1
    assert summary["metadata_coverage"]["waiting_minutes"]["chunks"] == 1
    assert summary["document_metadata_coverage"]["department_label"]["documents"] == 2
    assert summary["metadata_coverage"]["confirmed_date"]["chunks"] == 1
    assert summary["metadata_coverage"]["cleaned_confirmed_note"]["chunks"] == 1
    assert summary["metadata_coverage"]["is_via_email_parser"]["chunks"] == 1
    assert summary["metadata_coverage"]["is_handle_by_callcentre"]["chunks"] == 1
    assert summary["metadata_coverage"]["is_waiting_on_response"]["chunks"] == 1
    assert summary["source_metadata_coverage"]["support_group_name"]["status"] == "upstream_absent"
    assert summary["source_metadata_coverage"]["support_group_name"]["source_documents"] == 0
    assert summary["source_metadata_coverage"]["support_group_name"]["materialized_documents"] == 1
    assert summary["source_metadata_coverage"]["ticket_number"]["ticket_rows"] == 2
    assert summary["source_metadata_coverage"]["ticket_number"]["detail_rows"] == 0
    assert summary["source_metadata_coverage"]["default_contract_name"]["detail_rows"] == 1
    assert summary["source_metadata_coverage"]["default_contract_name"]["status"] == "materialized"
    assert summary["label_source_summary"]["account_label_source"]["id"]["chunks"] == 1
    assert summary["label_source_summary"]["account_label_source"]["raw"]["chunks"] == 1
    assert summary["label_source_summary"]["user_label_source"]["email"]["chunks"] == 1
    assert summary["label_source_summary"]["user_label_source"]["id"]["chunks"] == 1
    assert summary["label_source_summary"]["technician_label_source"]["joined"]["chunks"] == 1
    assert summary["label_source_summary"]["technician_label_source"]["id"]["chunks"] == 1
    assert summary["label_source_summary"]["department_label_source"]["class_name"]["chunks"] == 1
    assert summary["label_source_summary"]["department_label_source"]["support_group_name"]["chunks"] == 1
    assert summary["label_source_summary"]["action_cue_source"]["missing"]["chunks"] == 1
    assert summary["label_source_summary"]["action_cue_source"]["next_step"]["chunks"] == 1
    assert summary["entity_label_quality"]["account"]["readable_chunks"] == 1
    assert summary["entity_label_quality"]["account"]["identifier_like_chunks"] == 1
    assert summary["entity_label_quality"]["account"]["fallback_source_chunks"] == 1
    assert summary["entity_label_quality"]["account"]["identifier_like_distinct_value_sample"] == ["44"]
    assert summary["entity_label_quality"]["user"]["readable_source_chunks"] == 1
    assert summary["entity_label_quality"]["user"]["fallback_source_chunks"] == 1
    assert summary["entity_label_quality"]["technician"]["identifier_like_chunks"] == 1
    assert summary["entity_label_quality"]["department"]["readable_chunks"] == 2
    assert summary["metadata_coverage"]["has_attachments"]["ratio"] == 0.5
    assert summary["materialization"]["current_version"] >= 1
    assert summary["materialization"]["current_version_docs"] == 2
    assert summary["materialization"]["stale_docs"] == 0
    assert summary["materialization"]["chunk_rows_at_current_version"] == 2
    assert summary["vector_index"]["total_chunk_rows"] == 2
    assert summary["content_hash_summary"]["present_count"] == 2


def test_export_embedding_manifest(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed(db)
    output = tmp_path / "manifest.json"
    result = export_embedding_manifest(db, output)
    assert result["status"] == "ok"
    manifest = json.loads(output.read_text())
    assert manifest["chunk_count"] == 2
    assert manifest["filter_facets"]["accounts"] == ["44", "Acme"]
    assert manifest["filter_facets"]["departments"] == ["Dispatch", "Managed Services"]
    assert manifest["document_chunk_topology"]["avg_chunks_per_document"] == 1.0
    assert manifest["metadata_coverage"]["resolution_summary"]["chunks"] == 1
    assert manifest["document_metadata_coverage"]["resolution_summary"]["documents"] == 1
    assert manifest["metadata_coverage"]["account_location_name"]["chunks"] == 1
    assert manifest["source_metadata_coverage"]["default_contract_name"]["detail_rows"] == 1
    assert manifest["source_metadata_coverage"]["support_group_name"]["status"] == "upstream_absent"
    assert manifest["label_source_summary"]["account_label_source"]["raw"]["chunks"] == 1
    assert manifest["entity_label_quality"]["account"]["identifier_like_chunks"] == 1
