from __future__ import annotations

import json

import typer
from rich import print

from .analysis import (
    get_dataset_summary,
    list_recent_tickets,
    list_ticket_counts_by_account,
    list_ticket_counts_by_priority,
    list_ticket_counts_by_status,
    list_ticket_counts_by_technician,
)
from .client import SherpaDeskClient
from .ingest import seed_all, sync_delta
from .settings import load_settings
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


@app.command("dataset-summary")
def dataset_summary() -> None:
    settings = load_settings()
    print(json.dumps(get_dataset_summary(settings.db_path), indent=2))


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


@app.command("recent-tickets")
def recent_tickets(limit: int = 20) -> None:
    settings = load_settings()
    rows = list_recent_tickets(settings.db_path, limit=limit)
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    app()
