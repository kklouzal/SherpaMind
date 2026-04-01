from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
from typing import Any

from .db import connect
from .paths import ensure_path_layout
from .time_utils import parse_sherpadesk_timestamp


_SERVICE_TASK_BY_MODE = {
    "seed": None,
    "sync_hot_open": "hot_open",
    "sync_warm_closed": "warm_closed",
    "sync_cold_closed_audit": "cold_closed",
    "enrich_priority_ticket_details": "enrichment",
}


_SYNC_LANES: tuple[tuple[str, float], ...] = (
    ("seed", 24.0 * 30.0),
    ("sync_hot_open", 3.0),
    ("sync_warm_closed", 24.0),
    ("sync_cold_closed_audit", 24.0 * 14.0),
    ("enrich_priority_ticket_details", 24.0 * 7.0),
)


def _safe_json_loads(raw: str | None) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _hours_since(now: datetime, value: str | None) -> float | None:
    dt = parse_sherpadesk_timestamp(value)
    if dt is None:
        return None
    return round(max((now - dt).total_seconds(), 0.0) / 3600.0, 4)


def _classify_lane(*, latest_status: str | None, latest_finished_age_hours: float | None, expected_max_age_hours: float) -> str:
    normalized = str(latest_status or "").strip().lower()
    if not normalized:
        return "missing"
    if normalized == "running":
        return "running"
    if normalized != "success":
        return "error"
    if latest_finished_age_hours is None:
        return "unknown"
    if latest_finished_age_hours <= expected_max_age_hours:
        return "healthy"
    if latest_finished_age_hours <= expected_max_age_hours * 2:
        return "stale"
    return "critical"


def _load_service_state() -> dict[str, Any]:
    path = ensure_path_layout().service_state_file
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}


def _service_lane_snapshot(service_state: dict[str, Any], mode: str) -> dict[str, Any] | None:
    task_name = _SERVICE_TASK_BY_MODE.get(mode)
    if not task_name:
        return None
    task_state = ((service_state.get("tasks") or {}).get(task_name) or {})
    if not task_state:
        return None
    return {
        "task_name": task_name,
        "last_status": task_state.get("last_status"),
        "last_run_at": task_state.get("last_run_at"),
        "last_error": task_state.get("last_error"),
    }


def get_sync_freshness(db_path: Path) -> dict:
    now = datetime.now(timezone.utc)
    service_state = _load_service_state()
    with connect(db_path) as conn:
        lanes: dict[str, dict[str, Any]] = {}
        for mode, expected_max_age_hours in _SYNC_LANES:
            latest_run = conn.execute(
                "SELECT id, mode, started_at, finished_at, status, notes FROM ingest_runs WHERE mode = ? ORDER BY id DESC LIMIT 1",
                (mode,),
            ).fetchone()
            latest_success = conn.execute(
                "SELECT id, started_at, finished_at, status FROM ingest_runs WHERE mode = ? AND status = 'success' ORDER BY id DESC LIMIT 1",
                (mode,),
            ).fetchone()
            stats = conn.execute(
                """
                SELECT
                    COUNT(*) AS run_count,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) AS running_count,
                    SUM(CASE WHEN status != 'success' AND status != 'running' THEN 1 ELSE 0 END) AS error_count,
                    MIN(started_at) AS first_started_at,
                    MAX(started_at) AS latest_started_at,
                    MAX(finished_at) AS latest_finished_at
                FROM ingest_runs
                WHERE mode = ?
                """,
                (mode,),
            ).fetchone()
            consecutive_failures = conn.execute(
                """
                WITH ordered AS (
                    SELECT status,
                           ROW_NUMBER() OVER (ORDER BY id DESC) AS rn
                    FROM ingest_runs
                    WHERE mode = ?
                )
                SELECT COUNT(*) AS consecutive_failures
                FROM ordered
                WHERE rn < COALESCE((SELECT MIN(rn) FROM ordered WHERE status = 'success'), (SELECT COUNT(*) + 1 FROM ordered))
                  AND status != 'success'
                """,
                (mode,),
            ).fetchone()["consecutive_failures"]

            latest_dict = dict(latest_run) if latest_run else None
            latest_success_dict = dict(latest_success) if latest_success else None
            latest_started_at = latest_dict.get("started_at") if latest_dict else None
            latest_finished_at = latest_dict.get("finished_at") if latest_dict else None
            latest_status = latest_dict.get("status") if latest_dict else None
            latest_notes = latest_dict.get("notes") if latest_dict else None
            note_json = _safe_json_loads(latest_notes)
            latest_started_age_hours = _hours_since(now, latest_started_at)
            latest_finished_age_hours = _hours_since(now, latest_finished_at)
            last_success_finished_at = latest_success_dict.get("finished_at") if latest_success_dict else None
            last_success_age_hours = _hours_since(now, last_success_finished_at)

            service_lane = _service_lane_snapshot(service_state, mode)
            service_last_status = (service_lane or {}).get("last_status")
            service_last_run_at = (service_lane or {}).get("last_run_at")
            service_last_run_age_hours = _hours_since(now, service_last_run_at)
            service_evidence_used = False

            freshness_status = _classify_lane(
                latest_status=latest_status,
                latest_finished_age_hours=latest_finished_age_hours,
                expected_max_age_hours=expected_max_age_hours,
            )

            if service_last_status == "ok" and service_last_run_at:
                db_missing_or_stale = latest_finished_age_hours is None or latest_finished_age_hours > expected_max_age_hours * 2
                if db_missing_or_stale:
                    latest_status = "success"
                    latest_finished_at = service_last_run_at
                    latest_finished_age_hours = service_last_run_age_hours
                    if last_success_finished_at is None or (last_success_age_hours is None or service_last_run_age_hours is not None and service_last_run_age_hours < last_success_age_hours):
                        last_success_finished_at = service_last_run_at
                        last_success_age_hours = service_last_run_age_hours
                    freshness_status = _classify_lane(
                        latest_status=latest_status,
                        latest_finished_age_hours=latest_finished_age_hours,
                        expected_max_age_hours=expected_max_age_hours,
                    )
                    service_evidence_used = True
            elif service_last_status in {"error", "partial"} and freshness_status == "healthy":
                freshness_status = "error"

            lane_summary = {
                "mode": mode,
                "expected_max_age_hours": expected_max_age_hours,
                "freshness_status": freshness_status,
                "latest_run": latest_dict,
                "latest_run_notes_json": note_json,
                "latest_started_at": latest_started_at,
                "latest_started_age_hours": latest_started_age_hours,
                "latest_finished_at": latest_finished_at,
                "latest_finished_age_hours": latest_finished_age_hours,
                "last_success_finished_at": last_success_finished_at,
                "last_success_age_hours": last_success_age_hours,
                "run_count": int(stats["run_count"] or 0),
                "success_count": int(stats["success_count"] or 0),
                "running_count": int(stats["running_count"] or 0),
                "error_count": int(stats["error_count"] or 0),
                "first_started_at": stats["first_started_at"],
                "latest_recorded_started_at": stats["latest_started_at"],
                "latest_recorded_finished_at": stats["latest_finished_at"],
                "consecutive_non_success_runs": int(consecutive_failures or 0),
                "service_state": service_lane,
                "service_state_used_for_freshness": service_evidence_used,
            }
            lanes[mode] = lane_summary

    total_lanes = len(_SYNC_LANES)
    status_counts = {
        "healthy": 0,
        "stale": 0,
        "critical": 0,
        "missing": 0,
        "running": 0,
        "error": 0,
        "unknown": 0,
    }
    latest_success_ages = [
        float(lane["last_success_age_hours"])
        for lane in lanes.values()
        if lane.get("last_success_age_hours") is not None
    ]
    for lane in lanes.values():
        status = str(lane.get("freshness_status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

    if status_counts["critical"] or status_counts["error"]:
        overall_status = "critical"
    elif status_counts["missing"]:
        overall_status = "degraded"
    elif status_counts["stale"]:
        overall_status = "stale"
    elif status_counts["running"]:
        overall_status = "active"
    else:
        overall_status = "healthy"

    summary = {
        "generated_at": now.isoformat(),
        "overall_status": overall_status,
        "lane_count": total_lanes,
        "healthy_lanes": status_counts.get("healthy", 0),
        "stale_lanes": status_counts.get("stale", 0),
        "critical_lanes": status_counts.get("critical", 0),
        "missing_lanes": status_counts.get("missing", 0),
        "running_lanes": status_counts.get("running", 0),
        "error_lanes": status_counts.get("error", 0),
        "unknown_lanes": status_counts.get("unknown", 0),
        "lanes_with_recent_success": len(latest_success_ages),
        "freshest_success_age_hours": round(min(latest_success_ages), 4) if latest_success_ages else None,
        "stalest_success_age_hours": round(max(latest_success_ages), 4) if latest_success_ages else None,
    }

    result: dict[str, Any] = {
        "summary": summary,
        "lanes": lanes,
    }
    result.update(lanes)
    return result
