from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import os

from .paths import ensure_path_layout


@dataclass(frozen=True)
class Settings:
    api_base_url: str
    api_key: str | None
    api_user: str | None
    org_key: str | None
    instance_key: str | None
    db_path: Path
    watch_state_path: Path
    new_ticket_alerts_enabled: bool
    ticket_update_alerts_enabled: bool
    openclaw_webhook_url: str | None
    openclaw_webhook_token: str | None
    new_ticket_alert_channel: str | None
    ticket_update_alert_channel: str | None
    request_min_interval_seconds: float
    request_timeout_seconds: float
    seed_page_size: int
    seed_max_pages: int | None
    hot_open_pages: int
    warm_closed_pages: int
    warm_closed_days: int
    cold_closed_pages_per_run: int
    service_hot_open_every_seconds: int = 300
    service_warm_watch_every_seconds: int = 900
    service_alert_dispatch_every_seconds: int = 30
    service_alert_dispatch_batch_size: int = 10
    service_alert_lease_seconds: int = 300
    service_alert_retry_base_seconds: int = 120
    service_alert_max_attempts: int = 8
    service_maintenance_tick_seconds: int = 30
    service_warm_closed_every_seconds: int = 14400
    service_cold_closed_every_seconds: int = 86400
    service_enrichment_every_seconds: int = 7200
    service_public_snapshot_every_seconds: int = 1800
    service_vector_refresh_every_seconds: int = 1800
    service_doctor_every_seconds: int = 43200
    service_enrichment_limit: int = 25
    service_cold_bootstrap_every_seconds: int = 1800
    service_enrichment_bootstrap_every_seconds: int = 900
    service_enrichment_bootstrap_limit: int = 240
    cold_closed_bootstrap_pages_per_run: int = 10
    api_hourly_limit: int = 600
    api_budget_warn_ratio: float = 0.7
    api_budget_critical_ratio: float = 0.85
    api_request_log_retention_days: int = 14


def _read_key_value_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _write_key_value_file(path: Path, values: dict[str, str]) -> None:
    ordered_keys = [
        "SHERPADESK_API_BASE_URL",
        "SHERPADESK_ORG_KEY",
        "SHERPADESK_INSTANCE_KEY",
        "SHERPAMIND_OPENCLAW_WEBHOOK_URL",
        "SHERPAMIND_OPENCLAW_WEBHOOK_TOKEN",
        "SHERPAMIND_SERVICE_HOT_OPEN_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_WARM_WATCH_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_ALERT_DISPATCH_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_ALERT_DISPATCH_BATCH_SIZE",
        "SHERPAMIND_SERVICE_ALERT_LEASE_SECONDS",
        "SHERPAMIND_SERVICE_ALERT_RETRY_BASE_SECONDS",
        "SHERPAMIND_SERVICE_ALERT_MAX_ATTEMPTS",
        "SHERPAMIND_SERVICE_MAINTENANCE_TICK_SECONDS",
    ]
    lines = [
        "# SherpaMind staged non-secret settings",
        "# Runtime state lives under .SherpaMind/private/ outside the skill tree.",
        "# Secrets are stored separately under .SherpaMind/private/secrets/.",
    ]
    for key in ordered_keys:
        if key in values:
            lines.append(f"{key}={values[key]}")
    for key in sorted(values):
        if key not in ordered_keys:
            lines.append(f"{key}={values[key]}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _env_or_file(key: str, file_values: dict[str, str], default: str | None = None) -> str | None:
    return os.getenv(key) or file_values.get(key) or default


def _read_openclaw_config() -> dict[str, Any]:
    config_path = Path.home() / ".openclaw" / "openclaw.json"
    if not config_path.exists():
        return {}
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _read_openclaw_skill_entry() -> dict[str, str]:
    data = _read_openclaw_config()
    entry = (((data.get("skills") or {}).get("entries") or {}).get("sherpamind") or {})
    if not isinstance(entry, dict):
        return {}
    values: dict[str, str] = {}
    api_user = entry.get("apiUser")
    if isinstance(api_user, str) and api_user.strip():
        values["SHERPADESK_API_USER"] = api_user.strip()
    org_key = entry.get("orgKey")
    if isinstance(org_key, str) and org_key.strip():
        values["SHERPADESK_ORG_KEY"] = org_key.strip()
    instance_key = entry.get("instanceKey")
    if isinstance(instance_key, str) and instance_key.strip():
        values["SHERPADESK_INSTANCE_KEY"] = instance_key.strip()
    entry_config = entry.get("config") if isinstance(entry.get("config"), dict) else {}

    def entry_value(key: str) -> Any:
        return entry_config.get(key, entry.get(key))

    api_base_url = entry_value("apiBaseUrl")
    if isinstance(api_base_url, str) and api_base_url.strip():
        values["SHERPADESK_API_BASE_URL"] = api_base_url.strip()
    new_ticket_alert_channel = entry_value("newTicketAlertChannel")
    if isinstance(new_ticket_alert_channel, str) and new_ticket_alert_channel.strip():
        values["SHERPAMIND_NEW_TICKET_ALERT_CHANNEL"] = new_ticket_alert_channel.strip()
    ticket_update_alert_channel = entry_value("ticketUpdateAlertChannel")
    if isinstance(ticket_update_alert_channel, str) and ticket_update_alert_channel.strip():
        values["SHERPAMIND_TICKET_UPDATE_ALERT_CHANNEL"] = ticket_update_alert_channel.strip()
    new_ticket_alerts_enabled = entry_value("newTicketAlertsEnabled")
    if isinstance(new_ticket_alerts_enabled, bool):
        values["SHERPAMIND_NEW_TICKET_ALERTS_ENABLED"] = "true" if new_ticket_alerts_enabled else "false"
    ticket_update_alerts_enabled = entry_value("ticketUpdateAlertsEnabled")
    if isinstance(ticket_update_alerts_enabled, bool):
        values["SHERPAMIND_TICKET_UPDATE_ALERTS_ENABLED"] = "true" if ticket_update_alerts_enabled else "false"
    return values


def _read_openclaw_hooks_defaults() -> dict[str, str]:
    """Return local OpenClaw hook defaults for the alert dispatcher.

    SherpaMind installs are usually local to the same OpenClaw instance that
    should receive alerts. Reading the local hooks config here keeps reinstall /
    rebootstrap flows from silently falling back to an unauthenticated or stale
    webhook setup when `SHERPAMIND_OPENCLAW_*` values were not staged manually.
    """
    data = _read_openclaw_config()
    hooks = data.get("hooks") if isinstance(data.get("hooks"), dict) else {}
    if not hooks or hooks.get("enabled") is not True:
        return {}
    token = hooks.get("token")
    if not isinstance(token, str) or not token.strip():
        return {}
    gateway = data.get("gateway") if isinstance(data.get("gateway"), dict) else {}
    port = gateway.get("port") if isinstance(gateway.get("port"), int) else 18789
    path = hooks.get("path") if isinstance(hooks.get("path"), str) and hooks.get("path", "").strip() else "/hooks"
    path = "/" + path.strip().strip("/")
    return {
        "SHERPAMIND_OPENCLAW_WEBHOOK_URL": f"http://127.0.0.1:{port}{path}/agent",
        "SHERPAMIND_OPENCLAW_WEBHOOK_TOKEN": token.strip(),
    }


def stage_connection_settings(
    *,
    api_base_url: str | None = None,
    org_key: str | None = None,
    instance_key: str | None = None,
    openclaw_webhook_url: str | None = None,
    openclaw_webhook_token: str | None = None,
) -> Path:
    paths = ensure_path_layout()
    current = _read_key_value_file(paths.settings_file)
    updates = {
        "SHERPADESK_API_BASE_URL": api_base_url,
        "SHERPADESK_ORG_KEY": org_key,
        "SHERPADESK_INSTANCE_KEY": instance_key,
        "SHERPAMIND_OPENCLAW_WEBHOOK_URL": openclaw_webhook_url,
        "SHERPAMIND_OPENCLAW_WEBHOOK_TOKEN": openclaw_webhook_token,
    }
    for key, value in updates.items():
        if value is not None:
            current[key] = value
    _write_key_value_file(paths.settings_file, current)
    return paths.settings_file


def load_settings() -> Settings:
    paths = ensure_path_layout()
    file_values = _read_key_value_file(paths.settings_file)
    openclaw_skill_values = _read_openclaw_skill_entry()
    openclaw_hook_values = _read_openclaw_hooks_defaults()
    seed_max_pages_raw = _env_or_file("SHERPAMIND_SEED_MAX_PAGES", file_values)
    return Settings(
        api_base_url=_env_or_file("SHERPADESK_API_BASE_URL", file_values, openclaw_skill_values.get("SHERPADESK_API_BASE_URL") or "https://api.sherpadesk.com") or "https://api.sherpadesk.com",
        api_key=os.getenv("SHERPADESK_API_KEY"),
        api_user=os.getenv("SHERPADESK_API_USER") or openclaw_skill_values.get("SHERPADESK_API_USER"),
        org_key=_env_or_file("SHERPADESK_ORG_KEY", file_values, openclaw_skill_values.get("SHERPADESK_ORG_KEY")),
        instance_key=_env_or_file("SHERPADESK_INSTANCE_KEY", file_values, openclaw_skill_values.get("SHERPADESK_INSTANCE_KEY")),
        db_path=paths.db_path,
        watch_state_path=paths.watch_state_path,
        new_ticket_alerts_enabled=str(openclaw_skill_values.get("SHERPAMIND_NEW_TICKET_ALERTS_ENABLED") or "false").strip().lower() in {"1", "true", "yes", "on"},
        ticket_update_alerts_enabled=str(openclaw_skill_values.get("SHERPAMIND_TICKET_UPDATE_ALERTS_ENABLED") or "false").strip().lower() in {"1", "true", "yes", "on"},
        openclaw_webhook_url=_env_or_file("SHERPAMIND_OPENCLAW_WEBHOOK_URL", file_values, openclaw_hook_values.get("SHERPAMIND_OPENCLAW_WEBHOOK_URL")),
        openclaw_webhook_token=_env_or_file("SHERPAMIND_OPENCLAW_WEBHOOK_TOKEN", file_values, openclaw_hook_values.get("SHERPAMIND_OPENCLAW_WEBHOOK_TOKEN")),
        new_ticket_alert_channel=openclaw_skill_values.get("SHERPAMIND_NEW_TICKET_ALERT_CHANNEL"),
        ticket_update_alert_channel=openclaw_skill_values.get("SHERPAMIND_TICKET_UPDATE_ALERT_CHANNEL") or openclaw_skill_values.get("SHERPAMIND_NEW_TICKET_ALERT_CHANNEL"),
        request_min_interval_seconds=float(_env_or_file("SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS", file_values, "8.0") or "8.0"),
        request_timeout_seconds=float(_env_or_file("SHERPAMIND_REQUEST_TIMEOUT_SECONDS", file_values, "30.0") or "30.0"),
        seed_page_size=int(_env_or_file("SHERPAMIND_SEED_PAGE_SIZE", file_values, "100") or "100"),
        seed_max_pages=int(seed_max_pages_raw) if seed_max_pages_raw else None,
        hot_open_pages=int(_env_or_file("SHERPAMIND_HOT_OPEN_PAGES", file_values, "5") or "5"),
        warm_closed_pages=int(_env_or_file("SHERPAMIND_WARM_CLOSED_PAGES", file_values, "10") or "10"),
        warm_closed_days=int(_env_or_file("SHERPAMIND_WARM_CLOSED_DAYS", file_values, "7") or "7"),
        cold_closed_pages_per_run=int(_env_or_file("SHERPAMIND_COLD_CLOSED_PAGES_PER_RUN", file_values, "2") or "2"),
        service_hot_open_every_seconds=int(_env_or_file("SHERPAMIND_SERVICE_HOT_OPEN_EVERY_SECONDS", file_values, "300") or "300"),
        service_warm_watch_every_seconds=int(_env_or_file("SHERPAMIND_SERVICE_WARM_WATCH_EVERY_SECONDS", file_values, "900") or "900"),
        service_alert_dispatch_every_seconds=int(_env_or_file("SHERPAMIND_SERVICE_ALERT_DISPATCH_EVERY_SECONDS", file_values, "30") or "30"),
        service_alert_dispatch_batch_size=int(_env_or_file("SHERPAMIND_SERVICE_ALERT_DISPATCH_BATCH_SIZE", file_values, "10") or "10"),
        service_alert_lease_seconds=int(_env_or_file("SHERPAMIND_SERVICE_ALERT_LEASE_SECONDS", file_values, "300") or "300"),
        service_alert_retry_base_seconds=int(_env_or_file("SHERPAMIND_SERVICE_ALERT_RETRY_BASE_SECONDS", file_values, "120") or "120"),
        service_alert_max_attempts=int(_env_or_file("SHERPAMIND_SERVICE_ALERT_MAX_ATTEMPTS", file_values, "8") or "8"),
        service_maintenance_tick_seconds=int(_env_or_file("SHERPAMIND_SERVICE_MAINTENANCE_TICK_SECONDS", file_values, "30") or "30"),
        service_warm_closed_every_seconds=int(_env_or_file("SHERPAMIND_SERVICE_WARM_CLOSED_EVERY_SECONDS", file_values, "14400") or "14400"),
        service_cold_closed_every_seconds=int(_env_or_file("SHERPAMIND_SERVICE_COLD_CLOSED_EVERY_SECONDS", file_values, "86400") or "86400"),
        service_enrichment_every_seconds=int(_env_or_file("SHERPAMIND_SERVICE_ENRICHMENT_EVERY_SECONDS", file_values, "7200") or "7200"),
        service_public_snapshot_every_seconds=int(_env_or_file("SHERPAMIND_SERVICE_PUBLIC_SNAPSHOT_EVERY_SECONDS", file_values, "1800") or "1800"),
        service_vector_refresh_every_seconds=int(_env_or_file("SHERPAMIND_SERVICE_VECTOR_REFRESH_EVERY_SECONDS", file_values, "1800") or "1800"),
        service_doctor_every_seconds=int(_env_or_file("SHERPAMIND_SERVICE_DOCTOR_EVERY_SECONDS", file_values, "43200") or "43200"),
        service_enrichment_limit=int(_env_or_file("SHERPAMIND_SERVICE_ENRICHMENT_LIMIT", file_values, "60") or "60"),
        service_cold_bootstrap_every_seconds=int(_env_or_file("SHERPAMIND_SERVICE_COLD_BOOTSTRAP_EVERY_SECONDS", file_values, "1800") or "1800"),
        service_enrichment_bootstrap_every_seconds=int(_env_or_file("SHERPAMIND_SERVICE_ENRICHMENT_BOOTSTRAP_EVERY_SECONDS", file_values, "900") or "900"),
        service_enrichment_bootstrap_limit=int(_env_or_file("SHERPAMIND_SERVICE_ENRICHMENT_BOOTSTRAP_LIMIT", file_values, "240") or "240"),
        cold_closed_bootstrap_pages_per_run=int(_env_or_file("SHERPAMIND_COLD_CLOSED_BOOTSTRAP_PAGES_PER_RUN", file_values, "10") or "10"),
        api_hourly_limit=int(_env_or_file("SHERPAMIND_API_HOURLY_LIMIT", file_values, "600") or "600"),
        api_budget_warn_ratio=float(_env_or_file("SHERPAMIND_API_BUDGET_WARN_RATIO", file_values, "0.7") or "0.7"),
        api_budget_critical_ratio=float(_env_or_file("SHERPAMIND_API_BUDGET_CRITICAL_RATIO", file_values, "0.85") or "0.85"),
        api_request_log_retention_days=int(_env_or_file("SHERPAMIND_API_REQUEST_LOG_RETENTION_DAYS", file_values, "14") or "14"),
    )
