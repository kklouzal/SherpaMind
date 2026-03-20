from __future__ import annotations

import json
from pathlib import Path

import typer
from rich import print

from .analysis import (
    get_dataset_summary,
    get_insight_snapshot,
    list_open_ticket_ages,
    list_recent_account_activity,
    list_recent_tickets,
    list_technician_recent_load,
    list_ticket_attachment_summary,
    list_ticket_counts_by_account,
    list_ticket_counts_by_priority,
    list_ticket_counts_by_status,
    list_ticket_counts_by_technician,
    list_ticket_log_types,
    search_ticket_document_chunks,
    search_ticket_documents,
)
from .client import SherpaDeskClient
from .documents import export_ticket_documents, materialize_ticket_documents
from .enrichment import enrich_priority_ticket_details
from .migrate import migrate_legacy_state
from .paths import ensure_path_layout
from .public_artifacts import generate_public_snapshot
from .ingest import (
    seed_all,
    sync_cold_closed_audit,
    sync_delta,
    sync_hot_open_tickets,
    sync_warm_closed_tickets,
)
from .settings import load_settings, write_config_env
from .watch import watch_new_tickets
from .db import initialize_db

app = typer.Typer(help="SherpaMind CLI")


def _build_client() -> SherpaDeskClient:
    settings = load_settings()
    if not settings.api_key:
        raise typer.BadParameter("SHERPADESK_API_KEY is required for live API commands")
    return SherpaDeskClient(
        api_base_url=settings.api_base_url,
        api_key=settings.api_key,
        api_user=settings.api_user,
        org_key=settings.org_key,
        instance_key=settings.instance_key,
        timeout_seconds=settings.request_timeout_seconds,
        min_interval_seconds=settings.request_min_interval_seconds,
    )


@app.command("init-db")
def init_db() -> None:
    settings = load_settings()
    initialize_db(settings.db_path)
    print({"status": "ok", "db": str(settings.db_path)})


@app.command("workspace-layout")
def workspace_layout() -> None:
    paths = ensure_path_layout()
    print(json.dumps({
        "workspace_root": str(paths.workspace_root),
        "root": str(paths.root),
        "private_root": str(paths.private_root),
        "public_root": str(paths.public_root),
        "runtime_venv": str(paths.runtime_venv),
        "db_path": str(paths.db_path),
        "watch_state_path": str(paths.watch_state_path),
        "exports_root": str(paths.exports_root),
        "docs_root": str(paths.docs_root),
        "env_file": str(paths.env_file),
    }, indent=2))


@app.command("configure")
def configure(
    api_key: str | None = None,
    org_key: str | None = None,
    instance_key: str | None = None,
    api_user: str | None = None,
    api_base_url: str | None = None,
    notify_channel: str | None = None,
) -> None:
    env_file = write_config_env(
        api_base_url=api_base_url,
        api_key=api_key,
        api_user=api_user,
        org_key=org_key,
        instance_key=instance_key,
        notify_channel=notify_channel,
    )
    print(json.dumps({
        "status": "ok",
        "env_file": str(env_file),
        "updated_keys": [
            key for key, value in {
                "SHERPADESK_API_BASE_URL": api_base_url,
                "SHERPADESK_API_KEY": api_key,
                "SHERPADESK_API_USER": api_user,
                "SHERPADESK_ORG_KEY": org_key,
                "SHERPADESK_INSTANCE_KEY": instance_key,
                "SHERPAMIND_NOTIFY_CHANNEL": notify_channel,
            }.items() if value is not None
        ],
    }, indent=2))


@app.command("doctor")
def doctor() -> None:
    settings = load_settings()
    paths = ensure_path_layout()
    legacy_db = paths.workspace_root / "state" / "sherpamind.sqlite3"
    legacy_watch = paths.workspace_root / "state" / "watch_state.json"
    checks = {
        "env_file_exists": paths.env_file.exists(),
        "runtime_venv_exists": paths.runtime_venv.exists(),
        "db_exists": settings.db_path.exists(),
        "watch_state_exists": settings.watch_state_path.exists(),
        "legacy_db_exists": legacy_db.exists(),
        "legacy_watch_state_exists": legacy_watch.exists(),
        "api_key_present": bool(settings.api_key),
        "org_key_present": bool(settings.org_key),
        "instance_key_present": bool(settings.instance_key),
    }
    print(json.dumps({
        "status": "ok",
        "paths": {
            "root": str(paths.root),
            "private_root": str(paths.private_root),
            "public_root": str(paths.public_root),
            "env_file": str(paths.env_file),
            "runtime_venv": str(paths.runtime_venv),
            "db_path": str(settings.db_path),
        },
        "checks": checks,
    }, indent=2))


@app.command("migrate-legacy-state")
def migrate_state() -> None:
    paths = ensure_path_layout()
    result = migrate_legacy_state(paths.workspace_root)
    print(json.dumps(result.__dict__, indent=2))


@app.command("discover-orgs")
def discover_orgs() -> None:
    client = _build_client()
    result = client.discover_organizations()
    print(json.dumps(result, indent=2))


@app.command("seed")
def seed() -> None:
    settings = load_settings()
    result = seed_all(settings)
    print(json.dumps(result.__dict__, indent=2))


@app.command("sync")
def sync() -> None:
    settings = load_settings()
    result = sync_delta(settings)
    print(json.dumps(result.__dict__, indent=2))


@app.command("watch")
def watch() -> None:
    settings = load_settings()
    result = watch_new_tickets(settings)
    print(json.dumps(result.__dict__, indent=2))


@app.command("sync-hot-open")
def sync_hot_open() -> None:
    settings = load_settings()
    result = sync_hot_open_tickets(settings)
    print(json.dumps(result.__dict__, indent=2))


@app.command("sync-warm-closed")
def sync_warm_closed() -> None:
    settings = load_settings()
    result = sync_warm_closed_tickets(settings)
    print(json.dumps(result.__dict__, indent=2))


@app.command("sync-cold-closed-audit")
def sync_cold_closed() -> None:
    settings = load_settings()
    result = sync_cold_closed_audit(settings)
    print(json.dumps(result.__dict__, indent=2))


@app.command("enrich-priority-ticket-details")
def enrich_priority_details(limit: int = 50, materialize_docs: bool = True) -> None:
    settings = load_settings()
    result = enrich_priority_ticket_details(settings, limit=limit, materialize_docs=materialize_docs)
    print(json.dumps(result.__dict__, indent=2))


@app.command("materialize-ticket-docs")
def materialize_docs(limit: int = 0) -> None:
    settings = load_settings()
    initialize_db(settings.db_path)
    effective_limit = None if limit <= 0 else limit
    result = materialize_ticket_documents(settings.db_path, limit=effective_limit)
    print(json.dumps(result, indent=2))


@app.command("dataset-summary")
def dataset_summary() -> None:
    settings = load_settings()
    print(json.dumps(get_dataset_summary(settings.db_path), indent=2))


@app.command("insight-snapshot")
def insight_snapshot() -> None:
    settings = load_settings()
    print(json.dumps(get_insight_snapshot(settings.db_path), indent=2))


@app.command("report-ticket-counts")
def report_ticket_counts(limit: int = 20) -> None:
    settings = load_settings()
    rows = list_ticket_counts_by_account(settings.db_path, limit=limit)
    print(json.dumps(rows, indent=2))


@app.command("report-status-counts")
def report_status_counts() -> None:
    settings = load_settings()
    rows = list_ticket_counts_by_status(settings.db_path)
    print(json.dumps(rows, indent=2))


@app.command("report-priority-counts")
def report_priority_counts() -> None:
    settings = load_settings()
    rows = list_ticket_counts_by_priority(settings.db_path)
    print(json.dumps(rows, indent=2))


@app.command("report-technician-counts")
def report_technician_counts(limit: int = 20) -> None:
    settings = load_settings()
    rows = list_ticket_counts_by_technician(settings.db_path, limit=limit)
    print(json.dumps(rows, indent=2))


@app.command("report-ticket-log-types")
def report_ticket_log_types(limit: int = 20) -> None:
    settings = load_settings()
    rows = list_ticket_log_types(settings.db_path, limit=limit)
    print(json.dumps(rows, indent=2))


@app.command("report-attachment-summary")
def report_attachment_summary(limit: int = 20) -> None:
    settings = load_settings()
    rows = list_ticket_attachment_summary(settings.db_path, limit=limit)
    print(json.dumps(rows, indent=2))


@app.command("recent-tickets")
def recent_tickets(limit: int = 20) -> None:
    settings = load_settings()
    rows = list_recent_tickets(settings.db_path, limit=limit)
    print(json.dumps(rows, indent=2))


@app.command("open-ticket-ages")
def open_ticket_ages(limit: int = 20) -> None:
    settings = load_settings()
    rows = list_open_ticket_ages(settings.db_path, limit=limit)
    print(json.dumps(rows, indent=2))


@app.command("recent-account-activity")
def recent_account_activity(days: int = 7, limit: int = 20) -> None:
    settings = load_settings()
    rows = list_recent_account_activity(settings.db_path, days=days, limit=limit)
    print(json.dumps(rows, indent=2))


@app.command("recent-technician-load")
def recent_technician_load(days: int = 7, limit: int = 20) -> None:
    settings = load_settings()
    rows = list_technician_recent_load(settings.db_path, days=days, limit=limit)
    print(json.dumps(rows, indent=2))


@app.command("search-ticket-docs")
def search_docs(query: str, limit: int = 20) -> None:
    settings = load_settings()
    rows = search_ticket_documents(settings.db_path, query=query, limit=limit)
    print(json.dumps(rows, indent=2))


@app.command("search-ticket-chunks")
def search_chunks(query: str, limit: int = 20) -> None:
    settings = load_settings()
    rows = search_ticket_document_chunks(settings.db_path, query=query, limit=limit)
    print(json.dumps(rows, indent=2))


@app.command("export-ticket-docs")
def export_ticket_docs(output_path: str = "", limit: int = 0) -> None:
    settings = load_settings()
    paths = ensure_path_layout()
    effective_limit = None if limit <= 0 else limit
    resolved_output = Path(output_path) if output_path else (paths.exports_root / "ticket-docs.jsonl")
    result = export_ticket_documents(settings.db_path, resolved_output, limit=effective_limit)
    print(json.dumps(result, indent=2))


@app.command("generate-public-snapshot")
def generate_snapshot() -> None:
    settings = load_settings()
    result = generate_public_snapshot(settings.db_path)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    app()
