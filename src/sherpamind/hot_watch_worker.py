from __future__ import annotations

import os
import time
from typing import Any

from .db import cleanup_stale_worker_runs, finish_worker_run, get_worker_lease, release_worker_lease, renew_worker_lease, start_worker_run, try_acquire_worker_lease
from .ingest import sync_hot_open_tickets
from .settings import Settings, load_settings
from .watch import watch_new_tickets, watch_warm_tickets
from .worker_common import aggregate_service_state, append_log, file_lock, load_state, now_iso, save_state, worker_loop_sleep
from .paths import ensure_path_layout

WORKER_NAME = "hot_watch"
DEFAULT_LEASE_SECONDS = 300


def _lease_seconds(settings: Settings) -> int:
    return min(max(settings.service_hot_open_every_seconds * 2, 180), DEFAULT_LEASE_SECONDS)


def run_hot_watch_once(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    paths = ensure_path_layout()
    owner_id = f"{WORKER_NAME}:{os.getpid()}:{int(time.time())}"
    state = load_state(paths.hot_watch_state_file)

    if not try_acquire_worker_lease(settings.db_path, WORKER_NAME, owner_id, lease_seconds=_lease_seconds(settings), notes="hot watch loop"):
        lease = get_worker_lease(settings.db_path, WORKER_NAME)
        append_log(paths.hot_watch_log, "run skipped: worker lease already active")
        state["loop_status"] = "busy"
        state["last_loop_progress_at"] = now_iso()
        state["lease"] = lease
        save_state(paths.hot_watch_state_file, state)
        aggregate_service_state()
        return {"status": "skipped", "reason": "worker_lease_already_active", "lease": lease}

    state["last_loop_started_at"] = now_iso()
    state["loop_status"] = "running"
    state["lease"] = get_worker_lease(settings.db_path, WORKER_NAME)
    save_state(paths.hot_watch_state_file, state)
    aggregate_service_state()

    try:
        with file_lock(paths.hot_watch_lock_file, wait=False):
            cleanup_stale_worker_runs(settings.db_path)
            run_id = start_worker_run(settings.db_path, WORKER_NAME, "hot_watch_loop")
            results: list[dict[str, Any]] = []
            try:
                now_epoch = time.time()
                tasks = state.setdefault("tasks", {})

                hot_task = tasks.get("hot_open", {})
                last_hot = float(hot_task.get("last_run_epoch", 0))
                if now_epoch - last_hot >= settings.service_hot_open_every_seconds:
                    state["current_task"] = "hot_open"
                    watch_result = watch_new_tickets(settings)
                    renew_worker_lease(settings.db_path, WORKER_NAME, owner_id, lease_seconds=_lease_seconds(settings), notes="hot_open_sync")
                    sync_result = sync_hot_open_tickets(settings)
                    tasks["hot_open"] = {
                        "last_run_epoch": now_epoch,
                        "last_run_at": now_iso(),
                        "last_status": sync_result.status if sync_result.status != "ok" else watch_result.status,
                        "watch_result": watch_result.stats,
                        "sync_result": sync_result.stats,
                    }
                    state["last_loop_progress_at"] = now_iso()
                    append_log(paths.hot_watch_log, f"task=hot_open status={tasks['hot_open']['last_status']}")
                    results.append({"task": "hot_open", "status": tasks["hot_open"]["last_status"], "watch_result": watch_result.stats, "sync_result": sync_result.stats})

                warm_task = tasks.get("warm_watch", {})
                last_warm = float(warm_task.get("last_run_epoch", 0))
                if now_epoch - last_warm >= settings.service_warm_watch_every_seconds:
                    state["current_task"] = "warm_watch"
                    result = watch_warm_tickets(settings)
                    tasks["warm_watch"] = {
                        "last_run_epoch": now_epoch,
                        "last_run_at": now_iso(),
                        "last_status": result.status,
                        "last_result": result.stats,
                    }
                    state["last_loop_progress_at"] = now_iso()
                    append_log(paths.hot_watch_log, f"task=warm_watch status={result.status}")
                    results.append({"task": "warm_watch", "status": result.status, "result": result.stats})

                state["last_loop_at"] = now_iso()
                state["loop_status"] = "idle"
                state["current_task"] = None
                state["lease"] = get_worker_lease(settings.db_path, WORKER_NAME)
                save_state(paths.hot_watch_state_file, state)
                aggregate_service_state()
                finish_worker_run(settings.db_path, run_id, "success", notes=f"results={len(results)}")
                return {"status": "ok", "results": results}
            except Exception as exc:
                state["last_loop_progress_at"] = now_iso()
                state["loop_status"] = "error"
                state["last_error"] = f"{type(exc).__name__}: {exc}"
                state["lease"] = get_worker_lease(settings.db_path, WORKER_NAME)
                save_state(paths.hot_watch_state_file, state)
                aggregate_service_state()
                finish_worker_run(settings.db_path, run_id, "failed", notes=f"{type(exc).__name__}: {exc}")
                append_log(paths.hot_watch_log, f"worker status=error error={type(exc).__name__}: {exc}")
                raise
    finally:
        release_worker_lease(settings.db_path, WORKER_NAME, owner_id)


def run_hot_watch_loop() -> int:
    settings = load_settings()
    paths = ensure_path_layout()
    append_log(paths.hot_watch_log, "worker loop starting")
    while True:
        try:
            run_hot_watch_once(settings)
        except Exception as exc:
            append_log(paths.hot_watch_log, f"loop status=error error={type(exc).__name__}: {exc}")
        worker_loop_sleep(min(settings.service_hot_open_every_seconds, settings.service_alert_dispatch_every_seconds, 30))
