from __future__ import annotations

import os
import time
from typing import Any, Callable

from .analysis import get_api_usage_summary
from .db import cleanup_stale_ingest_runs, cleanup_stale_worker_runs, prune_api_request_events, release_worker_lease, start_worker_run, finish_worker_run, try_acquire_worker_lease
from .documents import ensure_current_ticket_materialization
from .enrichment import enrich_priority_ticket_details
from .freshness import get_sync_freshness
from .ingest import sync_cold_closed_audit, sync_warm_closed_tickets
from .observability import generate_runtime_status_artifacts
from .paths import ensure_path_layout
from .public_artifacts import generate_public_snapshot
from .settings import Settings, load_settings
from .vector_index import build_vector_index
from .worker_common import aggregate_service_state, append_log, file_lock, load_state, now_iso, save_state, worker_loop_sleep

WORKER_NAME = "maintenance"


def _task_specs(settings: Settings) -> list[tuple[str, int, Callable[[Settings], Any]]]:
    return [
        ("warm_closed", settings.service_warm_closed_every_seconds, sync_warm_closed_tickets),
        ("cold_closed", settings.service_cold_closed_every_seconds, lambda s: sync_cold_closed_audit(s, pages_per_run=s.cold_closed_pages_per_run)),
        ("enrichment", settings.service_enrichment_every_seconds, lambda s: enrich_priority_ticket_details(s, limit=s.service_enrichment_limit, materialize_docs=True)),
        ("retrieval_artifacts", settings.service_public_snapshot_every_seconds, lambda s: ensure_current_ticket_materialization(s.db_path)),
        ("public_snapshot", settings.service_public_snapshot_every_seconds, lambda s: generate_public_snapshot(s.db_path)),
        ("vector_refresh", settings.service_vector_refresh_every_seconds, lambda s: build_vector_index(s.db_path)),
        ("runtime_status", settings.service_doctor_every_seconds, lambda s: generate_runtime_status_artifacts(s.db_path)),
        ("doctor_marker", settings.service_doctor_every_seconds, lambda s: {"status": "ok", "checked_at": now_iso(), "freshness": get_sync_freshness(s.db_path)}),
    ]


def run_maintenance_once(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    paths = ensure_path_layout()
    owner_id = f"{WORKER_NAME}:{os.getpid()}"
    state = load_state(paths.maintenance_state_file)
    state["last_loop_started_at"] = now_iso()
    state["loop_status"] = "running"
    save_state(paths.maintenance_state_file, state)
    aggregate_service_state()

    if not try_acquire_worker_lease(settings.db_path, WORKER_NAME, owner_id, lease_seconds=max(settings.service_cold_closed_every_seconds, 900), notes="maintenance loop"):
        append_log(paths.maintenance_log, "run skipped: worker lease already active")
        state["loop_status"] = "busy"
        state["last_loop_progress_at"] = now_iso()
        save_state(paths.maintenance_state_file, state)
        aggregate_service_state()
        return {"status": "skipped", "reason": "worker_lease_already_active"}

    try:
        with file_lock(paths.maintenance_lock_file, wait=False):
            cleanup_stale_ingest_runs(settings.db_path)
            cleanup_stale_worker_runs(settings.db_path)
            prune_api_request_events(settings.db_path, settings.api_request_log_retention_days)
            run_id = start_worker_run(settings.db_path, WORKER_NAME, "maintenance_loop")
            results: list[dict[str, Any]] = []
            try:
                now_epoch = time.time()
                tasks = state.setdefault("tasks", {})
                for name, every_seconds, runner in _task_specs(settings):
                    task_state = tasks.setdefault(name, {})
                    last_run = float(task_state.get("last_run_epoch", 0))
                    if now_epoch - last_run < every_seconds:
                        continue
                    result = runner(settings)
                    task_state.update({
                        "last_run_epoch": now_epoch,
                        "last_run_at": now_iso(),
                        "last_status": getattr(result, 'status', None) or (result.get('status') if isinstance(result, dict) else 'ok'),
                    })
                    state["last_loop_progress_at"] = now_iso()
                    append_log(paths.maintenance_log, f"task={name} status={task_state['last_status']}")
                    results.append({"task": name, "status": task_state['last_status']})
                state["api_usage_last_seen"] = get_api_usage_summary(settings.db_path)
                state["last_loop_at"] = now_iso()
                state["loop_status"] = "idle"
                save_state(paths.maintenance_state_file, state)
                aggregate_service_state()
                finish_worker_run(settings.db_path, run_id, "success", notes=f"results={len(results)}")
                return {"status": "ok", "results": results}
            except Exception as exc:
                state["loop_status"] = "error"
                state["last_error"] = f"{type(exc).__name__}: {exc}"
                state["last_loop_progress_at"] = now_iso()
                save_state(paths.maintenance_state_file, state)
                aggregate_service_state()
                finish_worker_run(settings.db_path, run_id, "failed", notes=f"{type(exc).__name__}: {exc}")
                append_log(paths.maintenance_log, f"worker status=error error={type(exc).__name__}: {exc}")
                raise
    finally:
        release_worker_lease(settings.db_path, WORKER_NAME, owner_id)


def run_maintenance_loop() -> int:
    settings = load_settings()
    paths = ensure_path_layout()
    append_log(paths.maintenance_log, "worker loop starting")
    while True:
        try:
            run_maintenance_once(settings)
        except Exception as exc:
            append_log(paths.maintenance_log, f"loop status=error error={type(exc).__name__}: {exc}")
        worker_loop_sleep(settings.service_maintenance_tick_seconds)
