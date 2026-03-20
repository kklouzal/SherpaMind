from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
from typing import Any


@dataclass(frozen=True)
class CronSpec:
    name: str
    schedule_kind: str
    schedule_value: str
    message: str
    timeout_seconds: int = 300
    tz: str | None = None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def desired_cron_specs() -> list[CronSpec]:
    repo = _repo_root()
    runner = repo / "scripts" / "run.py"
    base = f"In the SherpaMind skill repository at {repo}, use the stable skill runner `{runner}` and do not invent alternate paths or state locations."
    return [
        CronSpec(
            name="sherpamind:hot-open-sync",
            schedule_kind="every",
            schedule_value="5m",
            timeout_seconds=240,
            message=f"{base} Run `python3 scripts/run.py watch` and then `python3 scripts/run.py sync-hot-open`. Keep this read-only and quiet unless something truly breaks.",
        ),
        CronSpec(
            name="sherpamind:warm-closed-sync",
            schedule_kind="every",
            schedule_value="4h",
            timeout_seconds=420,
            message=f"{base} Run `python3 scripts/run.py sync-warm-closed` and keep the warm closed reconciliation lane current.",
        ),
        CronSpec(
            name="sherpamind:cold-closed-audit",
            schedule_kind="cron",
            schedule_value="17 3 * * *",
            tz="America/Phoenix",
            timeout_seconds=420,
            message=f"{base} Run `python3 scripts/run.py sync-cold-closed-audit` to advance the slow rolling cold-history audit lane.",
        ),
        CronSpec(
            name="sherpamind:priority-enrichment",
            schedule_kind="every",
            schedule_value="2h",
            timeout_seconds=900,
            message=f"{base} Run `python3 scripts/run.py enrich-priority-ticket-details --limit 25` to keep high-value tickets enriched without over-hitting the API.",
        ),
        CronSpec(
            name="sherpamind:public-snapshot",
            schedule_kind="every",
            schedule_value="30m",
            timeout_seconds=300,
            message=f"{base} Run `python3 scripts/run.py generate-public-snapshot` so `.SherpaMind/public/docs` stays fresh for OpenClaw-friendly reading.",
        ),
        CronSpec(
            name="sherpamind:doctor",
            schedule_kind="cron",
            schedule_value="11 */12 * * *",
            tz="America/Phoenix",
            timeout_seconds=180,
            message=f"{base} Run `python3 scripts/run.py doctor` and `python3 scripts/run.py workspace-layout`. If something looks structurally wrong, report it clearly.",
        ),
    ]


def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=True)


def list_cron_jobs() -> dict[str, Any]:
    result = _run(["openclaw", "cron", "list", "--json"])
    return json.loads(result.stdout)


def managed_jobs() -> list[dict[str, Any]]:
    jobs = list_cron_jobs().get("jobs", [])
    names = {spec.name for spec in desired_cron_specs()}
    return [job for job in jobs if job.get("name") in names]


def doctor_automation() -> dict[str, Any]:
    jobs = managed_jobs()
    expected_names = [spec.name for spec in desired_cron_specs()]
    counts: dict[str, int] = {}
    for job in jobs:
        counts[job["name"]] = counts.get(job["name"], 0) + 1
    duplicates = {name: count for name, count in counts.items() if count > 1}
    missing = [name for name in expected_names if counts.get(name, 0) == 0]
    return {
        "expected_names": expected_names,
        "found_names": sorted(counts.keys()),
        "missing": missing,
        "duplicates": duplicates,
        "job_count": len(jobs),
    }


def _remove_job(job_id: str) -> None:
    _run(["openclaw", "cron", "remove", job_id, "--json"])


def _add_job(spec: CronSpec) -> dict[str, Any]:
    cmd = [
        "openclaw", "cron", "add",
        "--name", spec.name,
        "--session", "isolated",
        "--message", spec.message,
        "--no-deliver",
        "--light-context",
        "--timeout-seconds", str(spec.timeout_seconds),
        "--json",
    ]
    if spec.schedule_kind == "every":
        cmd.extend(["--every", spec.schedule_value])
    elif spec.schedule_kind == "cron":
        cmd.extend(["--cron", spec.schedule_value])
        if spec.tz:
            cmd.extend(["--tz", spec.tz])
    else:
        raise ValueError(f"Unsupported schedule kind: {spec.schedule_kind}")
    result = _run(cmd)
    return json.loads(result.stdout)


def reconcile_automation() -> dict[str, Any]:
    existing = managed_jobs()
    removed = []
    for job in existing:
        _remove_job(job["id"])
        removed.append({"id": job["id"], "name": job["name"]})
    created = []
    for spec in desired_cron_specs():
        created.append(_add_job(spec))
    return {
        "status": "ok",
        "removed": removed,
        "created": created,
        "expected": [spec.name for spec in desired_cron_specs()],
    }
