from __future__ import annotations

import os
import time
from dataclasses import asdict
from typing import Any, Callable

from .analysis import get_api_usage_summary
from .classification import dispatch_ticket_classification_events, refresh_ticket_class_taxonomy, write_back_completed_ticket_classifications
from .db import (
    cleanup_stale_ingest_runs,
    cleanup_stale_worker_runs,
    complete_derived_refresh_batch,
    get_derived_refresh_summary,
    get_worker_lease,
    lease_derived_refresh_batch,
    prune_api_request_events,
    release_worker_lease,
    renew_worker_lease,
    start_worker_run,
    finish_worker_run,
    try_acquire_worker_lease,
)
from .documents import ensure_current_ticket_materialization, materialize_ticket_documents
from .enrichment import enrich_priority_ticket_details
from .freshness import get_sync_freshness
from .ingest import sync_cold_closed_audit, sync_warm_closed_tickets
from .observability import generate_runtime_status_artifacts
from .paths import ensure_path_layout
from .public_artifacts import generate_public_snapshot
from .service_runtime import _build_budget_plan, _detect_immediate_local_repair_needs, _effective_settings, _update_cold_bootstrap_status
from .settings import Settings, load_settings
from .vector_index import build_vector_index, ensure_current_vector_index
from .worker_common import aggregate_service_state, append_log, file_lock, load_state, now_iso, save_state, worker_loop_sleep

WORKER_NAME = "maintenance"
DEFAULT_LEASE_SECONDS = 600
DERIVED_REFRESH_BATCH_SIZE = 50


def _task_specs(settings: Settings) -> list[tuple[str, int, Callable[[Settings], Any], str]]:
    return [
        ("warm_closed", settings.service_warm_closed_every_seconds, sync_warm_closed_tickets, "important"),
        ("cold_closed", settings.service_cold_closed_every_seconds, lambda s: sync_cold_closed_audit(s, pages_per_run=s.cold_closed_pages_per_run), "deferrable"),
        ("enrichment", settings.service_enrichment_every_seconds, lambda s: enrich_priority_ticket_details(s, limit=s.service_enrichment_limit, materialize_docs=True), "deferrable"),
        ("ticket_class_taxonomy_refresh", settings.service_ticket_class_taxonomy_refresh_every_seconds, lambda s: refresh_ticket_class_taxonomy(s), "important"),
        ("classification_dispatch", settings.service_classification_dispatch_every_seconds, lambda s: dispatch_ticket_classification_events(s, limit=2), "lightweight"),
        ("classification_writeback", settings.service_classification_writeback_every_seconds, lambda s: write_back_completed_ticket_classifications(s, limit=1, apply=True), "important"),
        ("retrieval_artifacts", settings.service_public_snapshot_every_seconds, lambda s: ensure_current_ticket_materialization(s.db_path), "lightweight"),
        ("public_snapshot", settings.service_public_snapshot_every_seconds, lambda s: generate_public_snapshot(s.db_path), "lightweight"),
        ("vector_refresh", settings.service_vector_refresh_every_seconds, lambda s: ensure_current_vector_index(s.db_path), "lightweight"),
        ("runtime_status", settings.service_doctor_every_seconds, lambda s: generate_runtime_status_artifacts(s.db_path), "lightweight"),
        ("doctor_marker", settings.service_doctor_every_seconds, lambda s: {"status": "ok", "checked_at": now_iso(), "freshness": get_sync_freshness(s.db_path)}, "lightweight"),
    ]


def _record_task_state(state: dict[str, Any], name: str, *, status: str, started_at: str, result: Any = None, error: str | None = None, skip_reason: str | None = None, forced: bool = False, force_reason: str | None = None, items_processed: int | None = None) -> None:
    finished_at = now_iso()
    task_state = state.setdefault("tasks", {}).setdefault(name, {})
    task_state.update(
        {
            "last_run_at": finished_at,
            "last_run_epoch": time.time(),
            "last_status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "duration_seconds": round(max(time.time() - time.mktime(time.strptime(started_at[:19], "%Y-%m-%dT%H:%M:%S")) if False else 0.0, 0.0), 4),
            "forced": forced,
            "force_reason": force_reason,
        }
    )
    if result is not None:
        task_state["last_result"] = result
    if error is not None:
        task_state["last_error"] = error
    else:
        task_state.pop("last_error", None)
    if skip_reason is not None:
        task_state["last_skip_reason"] = skip_reason
    else:
        task_state.pop("last_skip_reason", None)
    if items_processed is not None:
        task_state["items_processed"] = items_processed


def _maintenance_lease_seconds(settings: Settings) -> int:
    return min(max(settings.service_maintenance_tick_seconds * 4, 300), DEFAULT_LEASE_SECONDS)


def _run_derived_refresh_queue(settings: Settings, owner_id: str, state: dict[str, Any]) -> dict[str, Any]:
    batch = lease_derived_refresh_batch(settings.db_path, batch_size=DERIVED_REFRESH_BATCH_SIZE, lease_seconds=_maintenance_lease_seconds(settings))
    if not batch:
        return {"status": "ok", "ticket_count": 0, "materialized_documents": 0, "materialized_chunks": 0, "vector": None}

    ticket_ids = [str(row["ticket_id"]) for row in batch]
    try:
        materialization = materialize_ticket_documents(settings.db_path, ticket_ids=ticket_ids)
        renew_worker_lease(settings.db_path, WORKER_NAME, owner_id, lease_seconds=_maintenance_lease_seconds(settings), notes=f"derived_refresh:{len(ticket_ids)}")
        vector = build_vector_index(settings.db_path, ticket_ids=ticket_ids)
        complete_derived_refresh_batch(settings.db_path, ticket_ids)
        state["last_loop_progress_at"] = now_iso()
        return {
            "status": "ok",
            "ticket_count": len(ticket_ids),
            "ticket_ids": ticket_ids[:10],
            "materialized_documents": materialization.get("document_count", 0),
            "materialized_chunks": materialization.get("chunk_count", 0),
            "vector": vector,
        }
    except Exception as exc:
        complete_derived_refresh_batch(settings.db_path, ticket_ids, error_message=f"{type(exc).__name__}: {exc}")
        raise


def run_maintenance_once(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or load_settings()
    paths = ensure_path_layout()
    owner_id = f"{WORKER_NAME}:{os.getpid()}:{int(time.time())}"
    state = load_state(paths.maintenance_state_file)

    lease_seconds = _maintenance_lease_seconds(settings)
    if not try_acquire_worker_lease(settings.db_path, WORKER_NAME, owner_id, lease_seconds=lease_seconds, notes="maintenance loop"):
        lease = get_worker_lease(settings.db_path, WORKER_NAME)
        append_log(paths.maintenance_log, "run skipped: worker lease already active")
        state["loop_status"] = "busy"
        state["last_loop_progress_at"] = now_iso()
        state["lease"] = lease
        save_state(paths.maintenance_state_file, state)
        aggregate_service_state()
        return {"status": "skipped", "reason": "worker_lease_already_active", "lease": lease}

    state["last_loop_started_at"] = now_iso()
    state["loop_status"] = "running"
    state["lease"] = get_worker_lease(settings.db_path, WORKER_NAME)
    save_state(paths.maintenance_state_file, state)
    aggregate_service_state()

    try:
        with file_lock(paths.maintenance_lock_file, wait=False):
            cleanup_stale_ingest_runs(settings.db_path)
            cleanup_stale_worker_runs(settings.db_path)
            prune_api_request_events(settings.db_path, settings.api_request_log_retention_days)
            run_id = start_worker_run(settings.db_path, WORKER_NAME, "maintenance_loop")
            results: list[dict[str, Any]] = []
            try:
                usage = get_api_usage_summary(settings.db_path)
                bootstrap = _update_cold_bootstrap_status(settings)
                plan = _build_budget_plan(settings, usage, bootstrap)
                effective_settings = _effective_settings(settings, plan)
                forced_tasks = _detect_immediate_local_repair_needs(settings)

                derived_started_at = now_iso()
                state["current_task"] = "derived_refresh_queue"
                save_state(paths.maintenance_state_file, state)
                derived_result = _run_derived_refresh_queue(effective_settings, owner_id, state)
                _record_task_state(
                    state,
                    "derived_refresh_queue",
                    status=derived_result.get("status", "ok"),
                    started_at=derived_started_at,
                    result=derived_result,
                    items_processed=derived_result.get("ticket_count"),
                )
                append_log(paths.maintenance_log, f"task=derived_refresh_queue status={derived_result.get('status')} count={derived_result.get('ticket_count')}")
                results.append({"task": "derived_refresh_queue", "status": derived_result.get("status"), "result": derived_result})
                renew_worker_lease(settings.db_path, WORKER_NAME, owner_id, lease_seconds=lease_seconds, notes="maintenance after derived refresh")

                now_epoch = time.time()
                for name, every_seconds, runner, budget_class in _task_specs(effective_settings):
                    task_state = state.setdefault("tasks", {}).setdefault(name, {})
                    last_run = float(task_state.get("last_run_epoch", 0))
                    force_reason = forced_tasks.get(name)
                    if force_reason is None and now_epoch - last_run < every_seconds:
                        continue
                    if budget_class == "important" and not plan.allow_important:
                        _record_task_state(state, name, status="skipped", started_at=now_iso(), skip_reason=f"budget_protected mode={plan.mode}", forced=bool(force_reason), force_reason=force_reason)
                        results.append({"task": name, "status": "skipped", "reason": f"budget_protected mode={plan.mode}", "forced": bool(force_reason), "force_reason": force_reason})
                        continue
                    if budget_class == "deferrable" and not plan.allow_deferrable:
                        _record_task_state(state, name, status="skipped", started_at=now_iso(), skip_reason=f"budget_protected mode={plan.mode}", forced=bool(force_reason), force_reason=force_reason)
                        results.append({"task": name, "status": "skipped", "reason": f"budget_protected mode={plan.mode}", "forced": bool(force_reason), "force_reason": force_reason})
                        continue

                    started_at = now_iso()
                    state["current_task"] = name
                    save_state(paths.maintenance_state_file, state)
                    renew_worker_lease(settings.db_path, WORKER_NAME, owner_id, lease_seconds=lease_seconds, notes=f"task:{name}")
                    try:
                        result = runner(effective_settings)
                        state["last_loop_progress_at"] = now_iso()
                        _record_task_state(state, name, status=getattr(result, "status", None) or (result.get("status") if isinstance(result, dict) else "ok"), started_at=started_at, result=getattr(result, "__dict__", result), forced=bool(force_reason), force_reason=force_reason)
                        append_log(paths.maintenance_log, f"task={name} status={state['tasks'][name]['last_status']}")
                        results.append({"task": name, "status": state["tasks"][name]["last_status"], "result": getattr(result, "__dict__", result), "forced": bool(force_reason), "force_reason": force_reason})
                    except Exception as exc:
                        _record_task_state(state, name, status="error", started_at=started_at, error=f"{type(exc).__name__}: {exc}", forced=bool(force_reason), force_reason=force_reason)
                        append_log(paths.maintenance_log, f"task={name} status=error error={type(exc).__name__}: {exc}")
                        raise

                if get_derived_refresh_summary(settings.db_path).get("pending_count", 0) > 0:
                    followup_started_at = now_iso()
                    state["current_task"] = "derived_refresh_queue"
                    save_state(paths.maintenance_state_file, state)
                    renew_worker_lease(settings.db_path, WORKER_NAME, owner_id, lease_seconds=lease_seconds, notes="derived_refresh_followup")
                    followup_result = _run_derived_refresh_queue(effective_settings, owner_id, state)
                    _record_task_state(
                        state,
                        "derived_refresh_queue_followup",
                        status=followup_result.get("status", "ok"),
                        started_at=followup_started_at,
                        result=followup_result,
                        items_processed=followup_result.get("ticket_count"),
                    )
                    append_log(paths.maintenance_log, f"task=derived_refresh_queue_followup status={followup_result.get('status')} count={followup_result.get('ticket_count')}")
                    results.append({"task": "derived_refresh_queue_followup", "status": followup_result.get("status"), "result": followup_result})

                state["api_usage_last_seen"] = usage
                state["budget_plan_last_seen"] = asdict(plan)
                state["cold_bootstrap_last_seen"] = asdict(bootstrap)
                state["derived_refresh_summary"] = get_derived_refresh_summary(settings.db_path)
                state["last_loop_at"] = now_iso()
                state["loop_status"] = "idle"
                state["current_task"] = None
                state["lease"] = get_worker_lease(settings.db_path, WORKER_NAME)
                save_state(paths.maintenance_state_file, state)
                aggregate_service_state()
                finish_worker_run(settings.db_path, run_id, "success", notes=f"results={len(results)}")
                return {"status": "ok", "results": results, "budget_plan": asdict(plan), "cold_bootstrap": asdict(bootstrap), "derived_refresh_summary": state["derived_refresh_summary"]}
            except Exception as exc:
                state["loop_status"] = "error"
                state["last_error"] = f"{type(exc).__name__}: {exc}"
                state["last_loop_progress_at"] = now_iso()
                state["lease"] = get_worker_lease(settings.db_path, WORKER_NAME)
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
