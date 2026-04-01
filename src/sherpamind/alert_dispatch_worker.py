from __future__ import annotations

import os
from typing import Any

from .alerts import dispatch_queued_alert, finalize_queued_alert
from .db import cleanup_stale_worker_runs, finish_worker_run, get_alert_queue_summary, lease_alert_batch, release_worker_lease, start_worker_run, try_acquire_worker_lease
from .paths import ensure_path_layout
from .settings import Settings, load_settings
from .worker_common import aggregate_service_state, append_log, file_lock, load_state, now_iso, save_state, worker_loop_sleep

WORKER_NAME = "alert_dispatch"


def run_alert_dispatch_once(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    paths = ensure_path_layout()
    owner_id = f"{WORKER_NAME}:{os.getpid()}"
    state = load_state(paths.alert_dispatch_state_file)
    state["last_loop_started_at"] = now_iso()
    state["loop_status"] = "running"
    save_state(paths.alert_dispatch_state_file, state)
    aggregate_service_state()

    if not try_acquire_worker_lease(settings.db_path, WORKER_NAME, owner_id, lease_seconds=max(settings.service_alert_lease_seconds, 120), notes="alert dispatch loop"):
        append_log(paths.alert_dispatch_log, "run skipped: worker lease already active")
        state["loop_status"] = "busy"
        state["last_loop_progress_at"] = now_iso()
        save_state(paths.alert_dispatch_state_file, state)
        aggregate_service_state()
        return {"status": "skipped", "reason": "worker_lease_already_active"}

    try:
        with file_lock(paths.alert_dispatch_lock_file, wait=False):
            cleanup_stale_worker_runs(settings.db_path)
            run_id = start_worker_run(settings.db_path, WORKER_NAME, "alert_dispatch_loop")
            try:
                batch = lease_alert_batch(settings.db_path, batch_size=settings.service_alert_dispatch_batch_size, lease_seconds=settings.service_alert_lease_seconds)
                results: list[dict[str, Any]] = []
                for row in batch:
                    result = dispatch_queued_alert(settings, row)
                    finalize_queued_alert(settings, row, result, retry_after_seconds=settings.service_alert_retry_base_seconds)
                    results.append({"alert_id": row["id"], "ticket_id": row["ticket_id"], "alert_type": row["alert_type"], "status": result.status, "message": result.message})
                    append_log(paths.alert_dispatch_log, f"alert_id={row['id']} type={row['alert_type']} ticket_id={row['ticket_id']} status={result.status}")
                state.setdefault("tasks", {})["alert_dispatch"] = {
                    "last_run_at": now_iso(),
                    "last_status": "ok",
                    "last_batch_size": len(batch),
                    "queue": get_alert_queue_summary(settings.db_path),
                }
                state["last_loop_progress_at"] = now_iso()
                state["last_loop_at"] = now_iso()
                state["loop_status"] = "idle"
                save_state(paths.alert_dispatch_state_file, state)
                aggregate_service_state()
                finish_worker_run(settings.db_path, run_id, "success", notes=f"batch={len(batch)}")
                return {"status": "ok", "results": results, "queue": get_alert_queue_summary(settings.db_path)}
            except Exception as exc:
                state["loop_status"] = "error"
                state["last_error"] = f"{type(exc).__name__}: {exc}"
                state["last_loop_progress_at"] = now_iso()
                save_state(paths.alert_dispatch_state_file, state)
                aggregate_service_state()
                finish_worker_run(settings.db_path, run_id, "failed", notes=f"{type(exc).__name__}: {exc}")
                append_log(paths.alert_dispatch_log, f"worker status=error error={type(exc).__name__}: {exc}")
                raise
    finally:
        release_worker_lease(settings.db_path, WORKER_NAME, owner_id)


def run_alert_dispatch_loop() -> int:
    settings = load_settings()
    paths = ensure_path_layout()
    append_log(paths.alert_dispatch_log, "worker loop starting")
    while True:
        try:
            run_alert_dispatch_once(settings)
        except Exception as exc:
            append_log(paths.alert_dispatch_log, f"loop status=error error={type(exc).__name__}: {exc}")
        worker_loop_sleep(settings.service_alert_dispatch_every_seconds)
