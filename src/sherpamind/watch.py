from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .classification import enqueue_final_ticket_classification, enqueue_initial_ticket_classification
from .client import SherpaDeskClient
from .db import (
    enqueue_alert,
    enqueue_derived_refresh,
    get_ticket_alert_state,
    initialize_db,
    mark_new_ticket_alert_sent,
    mark_ticket_closed_confirmed,
    mark_ticket_open_missing,
    mark_ticket_open_seen,
    now_iso,
    upsert_tickets,
)
from .paths import ensure_path_layout
from .settings import Settings
from .sync_state import set_json_state


@dataclass
class WatchResult:
    status: str
    message: str
    stats: dict[str, Any] | None = None


def _load_watch_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "known_open_ticket_ids": [],
            "last_watch_at": None,
            "open_ticket_snapshot": {},
        }
    return json.loads(path.read_text())


def _save_watch_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True))


def _build_client(settings: Settings) -> SherpaDeskClient:
    assert settings.api_key is not None
    return SherpaDeskClient(
        api_base_url=settings.api_base_url,
        api_key=settings.api_key,
        api_user=settings.api_user,
        org_key=settings.org_key,
        instance_key=settings.instance_key,
        timeout_seconds=settings.request_timeout_seconds,
        min_interval_seconds=settings.request_min_interval_seconds,
        request_tracking_db_path=settings.db_path,
    )


def _snapshot_ticket(ticket: dict[str, Any]) -> dict[str, Any]:
    return {
        "updated_time": ticket.get("updated_time"),
        "status": ticket.get("status"),
        "is_new_user_post": ticket.get("is_new_user_post"),
        "is_new_tech_post": ticket.get("is_new_tech_post"),
        "next_step_date": ticket.get("next_step_date"),
        "subject": ticket.get("subject"),
        "account_name": ticket.get("account_name"),
    }


def _ticket_event_snapshot(ticket: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": ticket.get("id"),
        "ticket_id": str(ticket.get("id")),
        "subject": ticket.get("subject"),
        "account_name": ticket.get("account_name"),
        "updated_time": ticket.get("updated_time"),
        "created_time": ticket.get("created_time"),
        "status": ticket.get("status"),
        "priority_name": ticket.get("priority_name") or ticket.get("priority"),
        "is_new_user_post": ticket.get("is_new_user_post"),
        "is_new_tech_post": ticket.get("is_new_tech_post"),
        "next_step_date": ticket.get("next_step_date"),
    }


def _enqueue_detected_alerts(settings: Settings, new_tickets: list[dict[str, Any]], changed_tickets: list[dict[str, Any]], *, allow_new_ticket_alerts: bool = True, baseline_only: bool = False) -> dict[str, list[dict[str, Any]]]:
    new_results: list[dict[str, Any]] = []
    update_results: list[dict[str, Any]] = []
    if baseline_only:
        return {
            "new_ticket_alert_enqueues": new_results,
            "ticket_update_alert_enqueues": update_results,
        }

    if allow_new_ticket_alerts and settings.new_ticket_alerts_enabled:
        for ticket in new_tickets:
            state = get_ticket_alert_state(settings.db_path, str(ticket.get("id"))) or {}
            if state.get("open_alert_sent_at") and state.get("is_currently_monitored_open"):
                continue
            dedupe_key = f"new_ticket:{ticket.get('id')}:{int(state.get('open_cycle_id') or 1)}"
            result = enqueue_alert(
                settings.db_path,
                alert_type="new_ticket",
                ticket_id=str(ticket.get("id")),
                dedupe_key=dedupe_key,
                payload=_ticket_event_snapshot(ticket),
                priority=10,
            )
            new_results.append(result)
            if result.get("status") == "enqueued":
                mark_new_ticket_alert_sent(settings.db_path, str(ticket.get("id")))

    if settings.ticket_update_alerts_enabled:
        for ticket in changed_tickets:
            if ticket.get("is_new_user_post") and not ticket.get("is_new_tech_post"):
                state = get_ticket_alert_state(settings.db_path, str(ticket.get("id"))) or {}
                event_key = str(ticket.get("updated_time") or "unknown")
                if state.get("last_non_tech_alerted_key") == event_key:
                    continue
                dedupe_key = f"ticket_update:{ticket.get('id')}:{event_key}"
                update_results.append(
                    enqueue_alert(
                        settings.db_path,
                        alert_type="ticket_update",
                        ticket_id=str(ticket.get("id")),
                        dedupe_key=dedupe_key,
                        payload={**_ticket_event_snapshot(ticket), "event_key": event_key},
                        priority=20,
                    )
                )
    return {
        "new_ticket_alert_enqueues": new_results,
        "ticket_update_alert_enqueues": update_results,
    }


def _detect_ticket_changes(tickets: list[dict[str, Any]], prior_state: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[int], list[int], dict[str, Any]]:
    prior_ids = {int(ticket_id) for ticket_id in prior_state.get("known_open_ticket_ids", [])}
    prior_snapshot = {str(k): v for k, v in prior_state.get("open_ticket_snapshot", {}).items()}
    current_ids = {int(ticket["id"]) for ticket in tickets}
    new_ids = sorted(current_ids - prior_ids)
    closed_or_missing_ids = sorted(prior_ids - current_ids)
    new_id_set = set(new_ids)
    changed_tickets: list[dict[str, Any]] = []
    current_snapshot: dict[str, Any] = {}
    for ticket in tickets:
        ticket_id = str(ticket["id"])
        snap = _snapshot_ticket(ticket)
        current_snapshot[ticket_id] = snap
        if int(ticket["id"]) in new_id_set:
            continue
        if prior_snapshot.get(ticket_id) != snap:
            changed_tickets.append(_ticket_event_snapshot(ticket))
    new_tickets = [ticket for ticket in tickets if int(ticket["id"]) in new_id_set]
    return new_tickets, changed_tickets, new_ids, closed_or_missing_ids, current_snapshot


def _watch_ticket_set(settings: Settings, *, path: str, extra_params: dict[str, Any], max_pages: int, state_path: Path, state_key: str, count_key: str) -> WatchResult:
    initialize_db(settings.db_path)
    if not settings.api_key:
        return WatchResult(status="needs_config", message="Watcher is blocked until the OpenClaw `sherpamind` skill provides SHERPADESK_API_KEY.")
    if not settings.org_key or not settings.instance_key:
        return WatchResult(status="needs_org_context", message="Watcher is blocked until staged org/instance settings exist under .SherpaMind/private/config/settings.env.")

    client = _build_client(settings)
    tickets = client.list_paginated(path, page_size=settings.seed_page_size, max_pages=max_pages, extra_params=extra_params)
    synced_at = now_iso()
    upsert_tickets(settings.db_path, tickets, synced_at=synced_at)

    if state_key == "watch.last_state":
        for ticket in tickets:
            mark_ticket_open_seen(settings.db_path, ticket)

    prior_state = _load_watch_state(state_path)
    new_tickets, changed_tickets, new_ids, removed_ids, current_snapshot = _detect_ticket_changes(tickets, prior_state)
    if state_key == "watch.last_state":
        for removed_id in removed_ids:
            mark_ticket_open_missing(settings.db_path, str(removed_id))
    baseline_only = not bool(prior_state.get("known_open_ticket_ids"))
    enqueue_results = _enqueue_detected_alerts(
        settings,
        new_tickets,
        changed_tickets,
        allow_new_ticket_alerts=(state_key == "watch.last_state"),
        baseline_only=baseline_only,
    )
    classification_enqueues: list[dict[str, Any]] = []
    if not baseline_only:
        if state_key == "watch.last_state":
            classification_enqueues.extend(enqueue_initial_ticket_classification(settings, ticket, trigger_source=state_key) for ticket in new_tickets)
        elif state_key == "watch.warm_state":
            classification_enqueues.extend(enqueue_final_ticket_classification(settings, ticket, trigger_source=state_key) for ticket in new_tickets)

    touched_tickets = [str(ticket.get("id")) for ticket in new_tickets + changed_tickets if ticket.get("id") is not None]
    derived_refresh_enqueues = [
        enqueue_derived_refresh(
            settings.db_path,
            ticket_id=ticket_id,
            source=state_key,
            priority=10 if state_key == "watch.last_state" else 40,
        )
        for ticket_id in sorted(set(touched_tickets))
    ]

    if state_key == "watch.warm_state":
        for ticket in tickets:
            mark_ticket_closed_confirmed(settings.db_path, str(ticket.get("id")), status=ticket.get("status"), updated_time=ticket.get("updated_time"))

    next_state = {
        "known_open_ticket_ids": sorted(int(ticket["id"]) for ticket in tickets),
        "last_watch_at": synced_at,
        count_key: len(tickets),
        "new_ticket_ids_last_run": new_ids,
        "removed_open_ticket_ids_last_run": removed_ids,
        "open_ticket_snapshot": current_snapshot,
        "derived_refresh_enqueues": derived_refresh_enqueues,
        "classification_enqueues": classification_enqueues,
        **enqueue_results,
    }
    _save_watch_state(state_path, next_state)
    set_json_state(settings.db_path, state_key, next_state)

    return WatchResult(
        status="ok",
        message="Ticket watcher poll completed.",
        stats={
            "watched_pages": max_pages,
            count_key: len(tickets),
            "new_ticket_count": len(new_ids),
            "changed_ticket_count": len(changed_tickets),
            "changed_open_ticket_count": len(changed_tickets),
            "removed_ticket_count": len(removed_ids),
            "removed_open_ticket_count": len(removed_ids),
            "new_tickets": [_ticket_event_snapshot(ticket) for ticket in new_tickets],
            "changed_tickets": changed_tickets,
            "classification_enqueues": classification_enqueues,
            **enqueue_results,
        },
    )


def watch_new_tickets(settings: Settings) -> WatchResult:
    return _watch_ticket_set(
        settings,
        path="tickets",
        extra_params={"status": "open"},
        max_pages=settings.hot_open_pages,
        state_path=settings.watch_state_path,
        state_key="watch.last_state",
        count_key="observed_open_ticket_count",
    )


def watch_warm_tickets(settings: Settings) -> WatchResult:
    return _watch_ticket_set(
        settings,
        path="tickets",
        extra_params={"status": "closed"},
        max_pages=settings.warm_closed_pages,
        state_path=ensure_path_layout().warm_watch_state_path,
        state_key="watch.warm_state",
        count_key="observed_warm_ticket_count",
    )
