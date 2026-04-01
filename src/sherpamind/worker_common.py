from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fcntl

from .paths import ensure_path_layout


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(f"[{now_iso()}] {message}\n")


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"started_at": now_iso(), "tasks": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {"started_at": now_iso(), "tasks": {}}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def aggregate_service_state() -> dict[str, Any]:
    paths = ensure_path_layout()
    hot = load_state(paths.hot_watch_state_file)
    dispatch = load_state(paths.alert_dispatch_state_file)
    maintenance = load_state(paths.maintenance_state_file)
    aggregate = load_state(paths.service_state_file)
    aggregate["workers"] = {
        "hot_watch": hot,
        "alert_dispatch": dispatch,
        "maintenance": maintenance,
    }
    aggregate["last_aggregate_at"] = now_iso()
    aggregate.setdefault("tasks", {})
    maintenance_tasks = maintenance.get("tasks") or {}
    hot_tasks = hot.get("tasks") or {}
    aggregate["tasks"]["hot_open"] = hot_tasks.get("hot_open", aggregate["tasks"].get("hot_open", {}))
    aggregate["tasks"]["warm_watch"] = hot_tasks.get("warm_watch", aggregate["tasks"].get("warm_watch", {}))
    for key in ("warm_closed", "cold_closed", "enrichment", "retrieval_artifacts", "public_snapshot", "vector_refresh", "runtime_status", "doctor_marker"):
        if key in maintenance_tasks:
            aggregate["tasks"][key] = maintenance_tasks[key]
    aggregate["loop_status"] = {
        "hot_watch": hot.get("loop_status"),
        "alert_dispatch": dispatch.get("loop_status"),
        "maintenance": maintenance.get("loop_status"),
    }
    save_state(paths.service_state_file, aggregate)
    return aggregate


@contextmanager
def file_lock(path: Path, *, wait: bool = False):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as fh:
        flags = fcntl.LOCK_EX
        if not wait:
            flags |= fcntl.LOCK_NB
        try:
            fcntl.flock(fh.fileno(), flags)
        except BlockingIOError:
            raise RuntimeError(f"lock already active: {path.name}")
        fh.seek(0)
        fh.truncate()
        fh.write(f"pid={os.getpid()} acquired_at={now_iso()}\n")
        fh.flush()
        try:
            yield fh
        finally:
            fh.seek(0)
            fh.truncate()
            fh.flush()
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)


def worker_loop_sleep(seconds: int) -> None:
    time.sleep(max(int(seconds), 1))
