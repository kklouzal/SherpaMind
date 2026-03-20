from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import time
from typing import Any, Callable

from .analysis import get_api_usage_summary
from .db import prune_api_request_events
from .enrichment import enrich_priority_ticket_details
from .ingest import sync_cold_closed_audit, sync_hot_open_tickets, sync_warm_closed_tickets
from .paths import ensure_path_layout
from .public_artifacts import generate_public_snapshot
from .settings import Settings, load_settings
from .watch import watch_new_tickets


@dataclass
class TaskSpec:
    name: str
    every_seconds: int
    runner: Callable[[Settings], Any]
    budget_class: str = "core"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_state() -> dict[str, Any]:
    paths = ensure_path_layout()
    if not paths.service_state_file.exists():
        return {"started_at": _now_iso(), "tasks": {}}
    return json.loads(paths.service_state_file.read_text())


def _save_state(state: dict[str, Any]) -> None:
    paths = ensure_path_layout()
    paths.service_state_file.write_text(json.dumps(state, indent=2, sort_keys=True))


def _append_log(message: str) -> None:
    paths = ensure_path_layout()
    with paths.service_log.open("a", encoding="utf-8") as f:
        f.write(f"[{_now_iso()}] {message}\n")


def _task_specs(settings: Settings) -> list[TaskSpec]:
    return [
        TaskSpec("hot_open", settings.service_hot_open_every_seconds, lambda s: (watch_new_tickets(s), sync_hot_open_tickets(s)), budget_class="core"),
        TaskSpec("warm_closed", settings.service_warm_closed_every_seconds, sync_warm_closed_tickets, budget_class="important"),
        TaskSpec("cold_closed", settings.service_cold_closed_every_seconds, sync_cold_closed_audit, budget_class="deferrable"),
        TaskSpec("enrichment", settings.service_enrichment_every_seconds, lambda s: enrich_priority_ticket_details(s, limit=s.service_enrichment_limit, materialize_docs=True), budget_class="deferrable"),
        TaskSpec("public_snapshot", settings.service_public_snapshot_every_seconds, lambda s: generate_public_snapshot(s.db_path), budget_class="lightweight"),
        TaskSpec("vector_refresh", settings.service_vector_refresh_every_seconds, lambda s: build_vector_index(s.db_path), budget_class="lightweight"),
        TaskSpec("runtime_status", settings.service_doctor_every_seconds, lambda s: generate_runtime_status_artifacts(s.db_path), budget_class="lightweight"),
        TaskSpec("doctor_marker", settings.service_doctor_every_seconds, lambda s: {"status": "ok", "checked_at": _now_iso()}, budget_class="lightweight"),
    ]


def _budget_gate(settings: Settings, usage: dict[str, Any], spec: TaskSpec) -> tuple[bool, str | None]:
    ratio = float(usage.get("budget_utilization_ratio", 0.0))
    if ratio >= settings.api_budget_critical_ratio:
        if spec.budget_class in {"important", "deferrable"}:
            return False, f"budget_critical ratio={ratio}"
    elif ratio >= settings.api_budget_warn_ratio:
        if spec.budget_class == "deferrable":
            return False, f"budget_warn ratio={ratio}"
    return True, None


def run_pending_tasks(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    state = _load_state()
    tasks_state = state.setdefault("tasks", {})
    now = time.time()
    results = []

    pruned = prune_api_request_events(settings.db_path, settings.api_request_log_retention_days)
    if pruned:
        _append_log(f"api_request_events pruned={pruned}")
    usage = get_api_usage_summary(settings.db_path)
    state["api_usage_last_seen"] = usage

    for spec in _task_specs(settings):
        task_state = tasks_state.setdefault(spec.name, {})
        last_run = float(task_state.get("last_run_epoch", 0))
        if now - last_run < spec.every_seconds:
            continue
        allowed, reason = _budget_gate(settings, usage, spec)
        if not allowed:
            task_state.update({
                "last_skipped_at": _now_iso(),
                "last_skip_reason": reason,
            })
            results.append({"task": spec.name, "status": "skipped", "reason": reason})
            _append_log(f"task={spec.name} status=skipped reason={reason}")
            continue
        try:
            result = spec.runner(settings)
            task_state.update({
                "last_run_epoch": now,
                "last_status": "ok",
                "last_run_at": _now_iso(),
            })
            task_state.pop("last_error", None)
            task_state.pop("last_skip_reason", None)
            results.append({"task": spec.name, "status": "ok", "result": getattr(result, '__dict__', result)})
            _append_log(f"task={spec.name} status=ok")
            usage = get_api_usage_summary(settings.db_path)
            state["api_usage_last_seen"] = usage
        except Exception as exc:
            task_state.update({
                "last_run_epoch": now,
                "last_status": "error",
                "last_run_at": _now_iso(),
                "last_error": f"{type(exc).__name__}: {exc}",
            })
            results.append({"task": spec.name, "status": "error", "error": f"{type(exc).__name__}: {exc}"})
            _append_log(f"task={spec.name} status=error error={type(exc).__name__}: {exc}")
            usage = get_api_usage_summary(settings.db_path)
            state["api_usage_last_seen"] = usage
    state["last_loop_at"] = _now_iso()
    _save_state(state)
    return {
        "status": "ok",
        "results": results,
        "state_file": str(ensure_path_layout().service_state_file),
        "api_usage": usage,
        "retention_days": settings.api_request_log_retention_days,
        "pruned_request_events": pruned,
    }


def run_service_loop() -> int:
    settings = load_settings()
    _append_log("service loop starting")
    while True:
        run_pending_tasks(settings)
        time.sleep(30)
