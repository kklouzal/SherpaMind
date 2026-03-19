from __future__ import annotations

import json
from pathlib import Path

import typer
from rich import print

from .analysis import list_ticket_counts_by_account
from .ingest import seed_all, sync_delta
from .settings import load_settings
from .watch import watch_new_tickets
from .db import initialize_db

app = typer.Typer(help="SherpaMind CLI")


@app.command("init-db")
def init_db() -> None:
    settings = load_settings()
    initialize_db(settings.db_path)
    print({"status": "ok", "db": str(settings.db_path)})


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


@app.command("report-ticket-counts")
def report_ticket_counts() -> None:
    settings = load_settings()
    rows = list_ticket_counts_by_account(settings.db_path)
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    app()
