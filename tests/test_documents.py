import json
from pathlib import Path

from sherpamind.db import initialize_db, upsert_accounts, upsert_ticket_details, upsert_tickets, upsert_technicians, upsert_users
from sherpamind.documents import (
    DOCUMENT_MATERIALIZATION_VERSION,
    build_ticket_document_chunks,
    build_ticket_documents,
    ensure_current_ticket_materialization,
    export_ticket_chunks,
    export_ticket_documents,
    materialize_ticket_documents,
)


def seed_fixture(db: Path) -> None:
    initialize_db(db)
    upsert_accounts(db, [{"id": 1, "name": "Acme"}], synced_at="2026-03-19T01:00:00Z")
    upsert_users(db, [{"id": 11, "account_id": 1, "FullName": "Alice User", "email": "alice@example.com"}], synced_at="2026-03-19T01:00:00Z")
    upsert_technicians(db, [{"id": 21, "FullName": "Tech One", "email": "tech@example.com"}], synced_at="2026-03-19T01:00:00Z")
    upsert_tickets(
        db,
        [{
            "id": 101,
            "account_id": 1,
            "user_id": 11,
            "tech_id": 21,
            "subject": "Issue A",
            "status": "Open",
            "priority_name": "High",
            "class_name": "Hardware / Printer",
            "created_time": "2026-03-18T01:00:00Z",
            "updated_time": "2026-03-19T03:00:00Z",
            "initial_post": "",
            "account_location_name": "HQ",
            "support_group_name": "Dispatch",
            "location_name": "Front Desk",
            "department_key": "service",
            "is_via_email_parser": 1,
            "is_handle_by_callcentre": 0,
            "confirmed_by_name": "Dispatcher",
            "confirmed_date": "2026-03-19T04:30:00Z",
        }, {
            "id": 102,
            "account_id": 2,
            "user_id": 12,
            "tech_id": 999,
            "account_name": "Raw Account",
            "account_location_name": "Warehouse",
            "user_firstname": "Bob",
            "user_lastname": "Jones",
            "technician_firstname": "Queue",
            "technician_lastname": "Owner",
            "subject": "Issue B",
            "status": "Open",
            "priority_name": "Low",
            "created_time": "2026-03-18T02:00:00Z",
            "updated_time": "2026-03-19T04:00:00Z"
        }],
        synced_at="2026-03-19T01:00:00Z",
    )
    upsert_ticket_details(
        db,
        [{
            "id": 101,
            "plain_initial_post": "Can you help with issue A?\n\nThis ticket was created via the email parser.",
            "next_step": "Call back",
            "workpad": "Internal note",
            "initial_response": True,
            "followup_date": "2026-03-20T10:00:00Z",
            "followup_note": "Waiting on user approval",
            "request_completion_date": "2026-03-21T17:00:00Z",
            "request_completion_note": "Finish after-hours maintenance window",
            "support_group_name": "Managed Services",
            "default_contract_name": "Gold",
            "location_name": "HQ",
            "account_location_name": "HQ Campus",
            "department_key": "managed-services",
            "confirmed_by_name": "Tech Lead",
            "confirmed_date": "2026-03-19T05:00:00Z",
            "is_via_email_parser": 1,
            "is_handle_by_callcentre": 0,
            "is_waiting_on_response": True,
            "is_resolved": False,
            "is_confirmed": True,
            "ticketlogs": [
                {"id": 503, "log_type": "Closed", "record_date": "2026-03-19T06:30:00Z", "plain_note": "Closed after printer service restored"},
                {"id": 502, "log_type": "Waiting on Response", "record_date": "2026-03-19T06:00:00Z", "plain_note": "Waiting on user approval"},
                {"id": 501, "log_type": "Initial Post", "record_date": "2026-03-18T01:00:00Z", "plain_note": "printer broken"}
            ],
            "timelogs": [],
            "attachments": [{"id": "a1", "name": "shot.png", "url": "https://example/shot.png", "size": 1234, "date": "2026-03-18T01:00:00Z"}],
        }],
        synced_at="2026-03-19T01:00:00Z",
    )


def test_build_materialize_and_export_ticket_documents(tmp_path: Path) -> None:
    db = tmp_path / "sherpamind.sqlite3"
    seed_fixture(db)
    docs = build_ticket_documents(db)
    docs_by_id = {doc["ticket_id"]: doc for doc in docs}

    primary = docs_by_id["101"]
    assert primary["doc_id"] == "ticket:101"
    assert "Issue A" in primary["text"]
    assert "Issue summary:" in primary["text"]
    assert "This ticket was created via the email parser." not in primary["text"]
    assert "Internal note" in primary["text"]
    assert "printer broken" in primary["text"]
    assert "Support group: Managed Services" in primary["text"]
    assert "Contract: Gold" in primary["text"]
    assert "Location: HQ" in primary["text"]
    assert "Account location: HQ Campus" in primary["text"]
    assert "Department: Managed Services" in primary["text"]
    assert "Department key: managed-services" in primary["text"]
    assert "Via email parser: True" in primary["text"]
    assert "Handled by call centre: False" in primary["text"]
    assert "Confirmed date: 2026-03-19T05:00:00Z" in primary["text"]
    assert "Follow-up note: Waiting on user approval" in primary["text"]
    assert "Latest response date: 2026-03-18T01:00:00Z" in primary["text"]
    assert "Latest response note: printer broken" in primary["text"]
    assert "Resolution log date: 2026-03-19T06:30:00Z" in primary["text"]
    assert "Resolution log note: Closed after printer service restored" in primary["text"]
    assert "Requested completion note: Finish after-hours maintenance window" in primary["text"]
    assert "Attachments (metadata only)" in primary["text"]
    assert primary["metadata"]["attachments"][0]["name"] == "shot.png"
    assert primary["metadata"]["attachment_names"] == ["shot.png"]
    assert primary["metadata"]["has_attachments"] is True
    assert primary["metadata"]["category"] == "Hardware / Printer"
    assert primary["metadata"]["cleaned_subject"] == "Issue A"
    assert primary["metadata"]["cleaned_initial_post"] == "Can you help with issue A?"
    assert primary["metadata"]["cleaned_workpad"] == "Internal note"
    assert primary["metadata"]["cleaned_followup_note"] == "Waiting on user approval"
    assert primary["metadata"]["cleaned_request_completion_note"] == "Finish after-hours maintenance window"
    assert primary["metadata"]["cleaned_next_step"] == "Call back"
    assert primary["metadata"]["cleaned_latest_response_note"] == "printer broken"
    assert primary["metadata"]["latest_response_date"] == "2026-03-18T01:00:00Z"
    assert primary["metadata"]["cleaned_resolution_log_note"] == "Closed after printer service restored"
    assert primary["metadata"]["resolution_log_date"] == "2026-03-19T06:30:00Z"
    assert primary["metadata"]["followup_date"] == "2026-03-20T10:00:00Z"
    assert primary["metadata"]["request_completion_date"] == "2026-03-21T17:00:00Z"
    assert primary["metadata"]["support_group_name"] == "Managed Services"
    assert primary["metadata"]["default_contract_name"] == "Gold"
    assert primary["metadata"]["location_name"] == "HQ"
    assert primary["metadata"]["account_location_name"] == "HQ Campus"
    assert primary["metadata"]["department_key"] == "managed-services"
    assert primary["metadata"]["department_label"] == "Managed Services"
    assert primary["metadata"]["department_label_source"] == "support_group_name"
    assert primary["metadata"]["confirmed_by_name"] == "Tech Lead"
    assert primary["metadata"]["confirmed_date"] == "2026-03-19T05:00:00Z"
    assert primary["metadata"]["is_via_email_parser"] is True
    assert primary["metadata"]["is_handle_by_callcentre"] is False
    assert primary["metadata"]["is_waiting_on_response"] is True
    assert primary["metadata"]["is_resolved"] is False
    assert primary["metadata"]["is_confirmed"] is True
    assert primary["metadata"]["has_next_step"] is True
    assert primary["metadata"]["recent_log_types"] == ["Closed", "Waiting on Response", "Initial Post"]
    assert primary["metadata"]["recent_log_types_csv"] == "Closed, Waiting on Response, Initial Post"
    assert primary["metadata"]["initial_response_present"] is True
    assert primary["metadata"]["user_email"] == "alice@example.com"
    assert primary["metadata"]["detail_available"] is True
    assert primary["metadata"]["account_label_source"] == "joined"
    assert primary["metadata"]["user_label_source"] == "joined"
    assert primary["metadata"]["technician_label_source"] == "joined"
    assert primary["materialization_version"] == DOCUMENT_MATERIALIZATION_VERSION
    assert primary["metadata"]["materialization_version"] == DOCUMENT_MATERIALIZATION_VERSION

    fallback = docs_by_id["102"]
    assert fallback["account"] == "Raw Account"
    assert fallback["user_name"] == "Bob Jones"
    assert fallback["technician"] == "Queue Owner"
    assert "Account: Raw Account" in fallback["text"]
    assert "User: Bob Jones" in fallback["text"]
    assert "Technician: Queue Owner" in fallback["text"]
    assert "Account location: Warehouse" in fallback["text"]
    assert fallback["metadata"]["account_location_name"] == "Warehouse"
    assert fallback["metadata"]["account_label_source"] == "raw"
    assert fallback["metadata"]["user_label_source"] == "raw"
    assert fallback["metadata"]["technician_label_source"] == "joined"

    chunks = build_ticket_document_chunks(docs)
    chunks_by_ticket = {chunk["ticket_id"]: chunk for chunk in chunks}
    assert chunks_by_ticket["101"]["chunk_id"].startswith("ticket:101:chunk:")
    assert chunks_by_ticket["101"]["account"] == "Acme"

    materialized = materialize_ticket_documents(db)
    assert materialized["status"] == "ok"
    assert materialized["chunk_count"] >= 1
    assert materialized["materialization_version"] == DOCUMENT_MATERIALIZATION_VERSION

    output = tmp_path / "ticket-docs.jsonl"
    result = export_ticket_documents(db, output)
    assert result["status"] == "ok"
    assert result["document_count"] == 2

    chunk_output = tmp_path / "ticket-chunks.jsonl"
    chunk_result = export_ticket_chunks(db, chunk_output)
    assert chunk_result["status"] == "ok"
    assert chunk_result["chunk_count"] >= 1

    rematerialization_check = ensure_current_ticket_materialization(db)
    assert rematerialization_check["status"] == "ok"
    assert rematerialization_check["refreshed"] is False
    assert rematerialization_check["materialization"]["current_version"] == DOCUMENT_MATERIALIZATION_VERSION

    lines = output.read_text().splitlines()
    assert len(lines) == 2
    exported_ids = {json.loads(line)["doc_id"] for line in lines}
    assert exported_ids == {"ticket:101", "ticket:102"}
