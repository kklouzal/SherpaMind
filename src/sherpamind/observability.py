from __future__ import annotations

from pathlib import Path
import json

from .analysis import get_api_usage_summary, get_dataset_summary, get_enrichment_coverage
from .db import get_alert_queue_summary
from .freshness import get_sync_freshness
from .vector_index import get_vector_index_status
from .vector_exports import get_retrieval_readiness_summary
from .paths import ensure_path_layout
from .sync_state import get_json_state


def _markdown_table(rows: list[dict], columns: list[tuple[str, str]]) -> str:
    if not rows:
        return "_No data available._"
    header = "| " + " | ".join(label for _, label in columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    body = [
        "| " + " | ".join(str(row.get(key, "")) for key, _ in columns) + " |"
        for row in rows
    ]
    return "\n".join([header, sep, *body])


def _format_hours(value: float | None) -> str:
    if value is None:
        return ""
    return f"{float(value):.1f}"


def _sync_lane_rows(freshness: dict) -> list[dict]:
    rows: list[dict] = []
    for mode, lane in (freshness.get("lanes") or {}).items():
        rows.append({
            "mode": mode,
            "freshness_status": lane.get("freshness_status", "unknown"),
            "latest_status": ((lane.get("latest_run") or {}).get("status") or ""),
            "latest_finished_age_hours": _format_hours(lane.get("latest_finished_age_hours")),
            "last_success_age_hours": _format_hours(lane.get("last_success_age_hours")),
            "expected_max_age_hours": _format_hours(lane.get("expected_max_age_hours")),
            "consecutive_non_success_runs": lane.get("consecutive_non_success_runs", 0),
        })
    return rows


def _api_usage_summary_rows(usage: dict) -> list[dict]:
    return [{
        "requests_last_hour": usage.get("requests_last_hour", 0),
        "http_success_responses_last_hour": usage.get("http_success_responses_last_hour", 0),
        "http_error_responses_last_hour": usage.get("http_error_responses_last_hour", 0),
        "transport_errors_last_hour": usage.get("transport_errors_last_hour", 0),
        "error_ratio": usage.get("error_ratio", 0.0),
        "remaining_hourly_budget": usage.get("remaining_hourly_budget", 0),
        "budget_utilization_ratio": usage.get("budget_utilization_ratio", 0.0),
    }]


def _api_status_rows(usage: dict) -> list[dict]:
    rows: list[dict] = []
    for row in usage.get("status_breakdown_last_hour") or []:
        rows.append({
            "status_code": row.get("status_code", ""),
            "requests": row.get("request_count", 0),
        })
    return rows


def _api_error_path_rows(usage: dict) -> list[dict]:
    rows: list[dict] = []
    for row in usage.get("top_error_paths_last_hour") or []:
        rows.append({
            "path": row.get("path", ""),
            "error_key": row.get("error_key", ""),
            "requests": row.get("request_count", 0),
        })
    return rows


def generate_runtime_status_artifacts(db_path: Path) -> dict:
    paths = ensure_path_layout()
    runtime_dir = paths.docs_root / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    coverage = get_enrichment_coverage(db_path)
    freshness = get_sync_freshness(db_path)
    usage = get_api_usage_summary(db_path)
    dataset = get_dataset_summary(db_path)
    vector = get_vector_index_status(db_path)
    retrieval = get_retrieval_readiness_summary(db_path)
    cold_bootstrap = get_json_state(db_path, "service.cold_bootstrap", default={}) or {}
    alert_queue = get_alert_queue_summary(db_path)

    status_md = [
        "# SherpaMind Runtime Status",
        "",
        "## Dataset summary",
        "```json",
        json.dumps(dataset, indent=2),
        "```",
        "",
        "## Enrichment coverage",
        "```json",
        json.dumps(coverage, indent=2),
        "```",
        "",
        "## Sync freshness summary",
        _markdown_table([
            {
                "overall_status": (freshness.get("summary") or {}).get("overall_status", "unknown"),
                "healthy_lanes": (freshness.get("summary") or {}).get("healthy_lanes", 0),
                "stale_lanes": (freshness.get("summary") or {}).get("stale_lanes", 0),
                "critical_lanes": (freshness.get("summary") or {}).get("critical_lanes", 0),
                "missing_lanes": (freshness.get("summary") or {}).get("missing_lanes", 0),
                "running_lanes": (freshness.get("summary") or {}).get("running_lanes", 0),
                "stalest_success_age_hours": _format_hours((freshness.get("summary") or {}).get("stalest_success_age_hours")),
            }
        ], [
            ("overall_status", "Overall"),
            ("healthy_lanes", "Healthy"),
            ("stale_lanes", "Stale"),
            ("critical_lanes", "Critical"),
            ("missing_lanes", "Missing"),
            ("running_lanes", "Running"),
            ("stalest_success_age_hours", "Stalest Success Age (h)"),
        ]),
        "",
        "## Sync freshness lanes",
        _markdown_table(_sync_lane_rows(freshness), [
            ("mode", "Lane"),
            ("freshness_status", "Freshness"),
            ("latest_status", "Latest Run"),
            ("latest_finished_age_hours", "Latest Finish Age (h)"),
            ("last_success_age_hours", "Last Success Age (h)"),
            ("expected_max_age_hours", "Expected Max Age (h)"),
            ("consecutive_non_success_runs", "Consecutive Non-Success"),
        ]),
        "",
        "## Sync freshness",
        "```json",
        json.dumps(freshness, indent=2),
        "```",
        "",
        "## API usage summary",
        _markdown_table(_api_usage_summary_rows(usage), [
            ("requests_last_hour", "Requests (1h)"),
            ("http_success_responses_last_hour", "HTTP Success"),
            ("http_error_responses_last_hour", "HTTP Error Responses"),
            ("transport_errors_last_hour", "Transport Errors"),
            ("error_ratio", "Error Ratio"),
            ("remaining_hourly_budget", "Remaining Budget"),
            ("budget_utilization_ratio", "Budget Utilization"),
        ]),
        "",
        "## API status breakdown",
        _markdown_table(_api_status_rows(usage), [
            ("status_code", "Status"),
            ("requests", "Requests"),
        ]),
        "",
        "## API top failing paths",
        _markdown_table(_api_error_path_rows(usage), [
            ("path", "Path"),
            ("error_key", "Error"),
            ("requests", "Requests"),
        ]),
        "",
        "## API usage",
        "```json",
        json.dumps(usage, indent=2),
        "```",
        "",
        "## Cold bootstrap status",
        "```json",
        json.dumps(cold_bootstrap, indent=2),
        "```",
        "",
        "## Alert queue",
        "```json",
        json.dumps(alert_queue, indent=2),
        "```",
        "",
        "## Vector index status",
        "```json",
        json.dumps(vector, indent=2),
        "```",
        "",
        "## Retrieval readiness",
        "```json",
        json.dumps(retrieval, indent=2),
        "```",
    ]
    out = runtime_dir / "status.md"
    out.write_text("\n".join(status_md) + "\n")
    return {"status": "ok", "output_path": str(out)}
