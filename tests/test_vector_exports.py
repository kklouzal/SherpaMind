import json
from pathlib import Path

from sherpamind.db import initialize_db, replace_ticket_document_chunks, replace_ticket_documents
from sherpamind.documents import DOCUMENT_MATERIALIZATION_VERSION
from sherpamind.vector_exports import export_embedding_manifest, export_embedding_ready_chunks, get_retrieval_readiness_summary


def seed(db: Path) -> None:
    initialize_db(db)
    replace_ticket_documents(
        db,
        [{
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
                "confirmed_by_name": "Tech Lead",
                "confirmed_date": "2026-03-19T05:00:00Z",
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
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    replace_ticket_document_chunks(
        db,
        [{
            "chunk_id": "ticket:101:chunk:0",
            "doc_id": "ticket:101",
            "ticket_id": 101,
            "chunk_index": 0,
            "text": "chunk text",
            "content_hash": "def",
        }],
        synced_at="2026-03-19T01:00:00Z",
    )


def test_export_embedding_ready_chunks(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed(db)
    output = tmp_path / "embedding.jsonl"
    result = export_embedding_ready_chunks(db, output)
    assert result["status"] == "ok"
    row = json.loads(output.read_text().splitlines()[0])
    assert row["id"] == "ticket:101:chunk:0"
    assert row["metadata"]["account"] == "Acme"
    assert row["metadata"]["priority"] == "High"
    assert row["metadata"]["class_name"] == "Service Request"
    assert row["metadata"]["submission_category"] == "Portal"
    assert row["metadata"]["resolution_category"] == "Completed"
    assert row["metadata"]["ticketlogs_count"] == 5
    assert row["metadata"]["has_attachments"] is True
    assert row["metadata"]["cleaned_followup_note"] == "Waiting on customer reply"
    assert row["metadata"]["cleaned_request_completion_note"] == "Complete during maintenance window"
    assert row["metadata"]["cleaned_next_step"] == "Call back tomorrow"
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
    assert row["metadata"]["confirmed_by_name"] == "Tech Lead"
    assert row["metadata"]["confirmed_date"] == "2026-03-19T05:00:00Z"
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
    assert summary["chunk_count"] == 1
    assert summary["document_count"] == 1
    assert summary["chunk_quality"]["max_chunk_chars"] == len("chunk text")
    assert summary["filter_facets"]["accounts"] == ["Acme"]
    assert summary["filter_facets"]["priorities"] == ["High"]
    assert summary["filter_facets"]["class_names"] == ["Service Request"]
    assert summary["filter_facets"]["submission_categories"] == ["Portal"]
    assert summary["filter_facets"]["resolution_categories"] == ["Completed"]
    assert summary["filter_facets"]["departments"] == ["Managed Services"]
    assert summary["metadata_coverage"]["cleaned_subject"]["chunks"] == 1
    assert summary["metadata_coverage"]["cleaned_followup_note"]["chunks"] == 1
    assert summary["metadata_coverage"]["cleaned_latest_response_note"]["chunks"] == 1
    assert summary["metadata_coverage"]["cleaned_resolution_log_note"]["chunks"] == 1
    assert summary["metadata_coverage"]["class_name"]["chunks"] == 1
    assert summary["metadata_coverage"]["submission_category"]["chunks"] == 1
    assert summary["metadata_coverage"]["resolution_category"]["chunks"] == 1
    assert summary["metadata_coverage"]["support_group_name"]["chunks"] == 1
    assert summary["metadata_coverage"]["account_location_name"]["chunks"] == 1
    assert summary["metadata_coverage"]["department_key"]["chunks"] == 1
    assert summary["metadata_coverage"]["department_label"]["chunks"] == 1
    assert summary["metadata_coverage"]["confirmed_date"]["chunks"] == 1
    assert summary["metadata_coverage"]["is_via_email_parser"]["chunks"] == 1
    assert summary["metadata_coverage"]["is_handle_by_callcentre"]["chunks"] == 1
    assert summary["metadata_coverage"]["is_waiting_on_response"]["chunks"] == 1
    assert summary["label_source_summary"]["account_label_source"]["raw"]["chunks"] == 1
    assert summary["label_source_summary"]["user_label_source"]["email"]["chunks"] == 1
    assert summary["label_source_summary"]["technician_label_source"]["joined"]["chunks"] == 1
    assert summary["label_source_summary"]["department_label_source"]["support_group_name"]["chunks"] == 1
    assert summary["metadata_coverage"]["has_attachments"]["ratio"] == 1.0
    assert summary["materialization"]["current_version"] >= 1
    assert summary["materialization"]["current_version_docs"] == 1
    assert summary["materialization"]["stale_docs"] == 0
    assert summary["materialization"]["chunk_rows_at_current_version"] == 1
    assert summary["vector_index"]["total_chunk_rows"] == 1
    assert summary["content_hash_summary"]["present_count"] == 1


def test_export_embedding_manifest(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed(db)
    output = tmp_path / "manifest.json"
    result = export_embedding_manifest(db, output)
    assert result["status"] == "ok"
    manifest = json.loads(output.read_text())
    assert manifest["chunk_count"] == 1
    assert manifest["filter_facets"]["accounts"] == ["Acme"]
    assert manifest["filter_facets"]["departments"] == ["Managed Services"]
    assert manifest["metadata_coverage"]["resolution_summary"]["chunks"] == 1
    assert manifest["metadata_coverage"]["account_location_name"]["chunks"] == 1
    assert manifest["label_source_summary"]["account_label_source"]["raw"]["chunks"] == 1
