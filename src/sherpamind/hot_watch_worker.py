from __future__ import annotations

import os
from typing import Any

from .db import cleanup_stale_worker_runs, finish_worker_run, start_worker_run, try_acquire_worker_lease, release_worker_lease
from .settings import Settings, load_settings
from .watch import watch_new_tickets, watch_warm_tickets
from .worker_common import aggregate_service_state, append_log, file_lock, load_state, now_iso, save_state, worker_loop_sleep
from .paths import ensure_path_layout

WORKER_NAME = "hot_watch"


def run_hot_watch_once(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    paths = ensure_path_layout()
    owner_id = f"{WORKER_NAME}:{os.getpid()}"
    state = load_state(paths.hot_watch_state_file)
    state["last_loop_started_at"] = now_iso()
    state["loop_status"] = "running"
    save_state(paths.hot_watch_state_file, state)
    aggregate_service_state()

    if not try_acquire_worker_lease(settings.db_path, WORKER_NAME, owner_id, lease_seconds=max(settings.service_hot_open_every_seconds * 2, 300), notes="hot watch loop"):
        append_log(paths.hot_watch_log, "run skipped: worker lease already active")
        state["loop_status"] = "busy"
        state["last_loop_progress_at"] = now_iso()
        save_state(paths.hot_watch_state_file, state)
        aggregate_service_state()
        return {"status": "skipped", "reason": "worker_lease_already_active"}

    try:
        with file_lock(paths.hot_watch_lock_file, wait=False):
            cleanup_stale_worker_runs(settings.db_path)
            run_id = start_worker_run(settings.db_path, WORKER_NAME, "hot_watch_loop")
            results: list[dict[str, Any]] = []
            try:
                hot_task = (state.get("tasks") or {}).get("hot_open", {})
                last_hot = float(hot_task.get("last_run_epoch", 0))
                now_epoch = __import__("time").time()
                if now_epoch - last_hot >= settings.service_hot_open_every_seconds:
                    result = watch_new_tickets(settings)
                    state.setdefault("tasks", {})["hot_open"] = {
                        "last_run_epoch": now_epoch,
                        "last_run_at": now_iso(),
                        "last_status": result.status,
                        "last_result": result.stats,
                    }
                    state["last_loop_progress_at"] = now_iso()
                    append_log(paths.hot_watch_log, f"task=hot_open status={result.status}")
                    results.append({"task": "hot_open", "status": result.status, "result": result.stats})

                warm_task = (state.get("tasks") or {}).get("warm_watch", {})
                last_warm = float(warm_task.get("last_run_epoch", 0))
                if now_epoch - last_warm >= settings.service_warm_watch_every_seconds:
                    result = watch_warm_tickets(settings)
                    state.setdefault("tasks", {})["warm_watch"] = {
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
                save_state(paths.hot_watch_state_file, state)
                aggregate_service_state()
                finish_worker_run(settings.db_path, run_id, "success", notes=f"results={len(results)}")
                return {"status": "ok", "results": results}
            except Exception as exc:
                state["last_loop_progress_at"] = now_iso()
                state["loop_status"] = "error"
                state["last_error"] = f"{type(exc).__name__}: {exc}"
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
