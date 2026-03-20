from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import time
from typing import Any, Callable

from .documents import materialize_ticket_documents
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
        TaskSpec("hot_open", settings.service_hot_open_every_seconds, lambda s: (watch_new_tickets(s), sync_hot_open_tickets(s))),
        TaskSpec("warm_closed", settings.service_warm_closed_every_seconds, sync_warm_closed_tickets),
        TaskSpec("cold_closed", settings.service_cold_closed_every_seconds, sync_cold_closed_audit),
        TaskSpec("enrichment", settings.service_enrichment_every_seconds, lambda s: enrich_priority_ticket_details(s, limit=s.service_enrichment_limit, materialize_docs=True)),
        TaskSpec("public_snapshot", settings.service_public_snapshot_every_seconds, lambda s: generate_public_snapshot(s.db_path)),
        TaskSpec("doctor_marker", settings.service_doctor_every_seconds, lambda s: {"status": "ok", "checked_at": _now_iso()}),
    ]


def run_pending_tasks(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    state = _load_state()
    tasks_state = state.setdefault("tasks", {})
    now = time.time()
    results = []
    for spec in _task_specs(settings):
        task_state = tasks_state.setdefault(spec.name, {})
        last_run = float(task_state.get("last_run_epoch", 0))
        if now - last_run < spec.every_seconds:
            continue
        try:
            result = spec.runner(settings)
            task_state.update({
                "last_run_epoch": now,
                "last_status": "ok",
                "last_run_at": _now_iso(),
            })
            results.append({"task": spec.name, "status": "ok", "result": getattr(result, '__dict__', result)})
            _append_log(f"task={spec.name} status=ok")
        except Exception as exc:
            task_state.update({
                "last_run_epoch": now,
                "last_status": "error",
                "last_run_at": _now_iso(),
                "last_error": f"{type(exc).__name__}: {exc}",
            })
            results.append({"task": spec.name, "status": "error", "error": f"{type(exc).__name__}: {exc}"})
            _append_log(f"task={spec.name} status=error error={type(exc).__name__}: {exc}")
    state["last_loop_at"] = _now_iso()
    _save_state(state)
    return {"status": "ok", "results": results, "state_file": str(ensure_path_layout().service_state_file)}


def run_service_loop() -> int:
    settings = load_settings()
    _append_log("service loop starting")
    while True:
        run_pending_tasks(settings)
        time.sleep(30)
