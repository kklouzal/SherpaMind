from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil
import subprocess
from typing import Any

from .paths import ensure_path_layout

SERVICE_NAMES = {
    "hot_watch": "sherpamind-hot-watch.service",
    "alert_dispatch": "sherpamind-alert-dispatch.service",
    "maintenance": "sherpamind-maintenance.service",
}


def _read_openclaw_skill_entry() -> dict[str, str]:
    config_path = Path.home() / ".openclaw" / "openclaw.json"
    if not config_path.exists():
        return {}
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}
    entry = (((data.get("skills") or {}).get("entries") or {}).get("sherpamind") or {})
    return entry if isinstance(entry, dict) else {}


@dataclass
class ServiceCommandResult:
    status: str
    message: str
    details: dict[str, Any] | None = None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _unit_path(name: str) -> Path:
    return Path.home() / ".config" / "systemd" / "user" / name


def _run(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=check)


def _base_env_lines() -> list[str]:
    paths = ensure_path_layout()
    skill_entry = _read_openclaw_skill_entry()
    env_lines = [f"Environment=SHERPAMIND_WORKSPACE_ROOT={paths.workspace_root}"]
    api_key = skill_entry.get("apiKey")
    if isinstance(api_key, str) and api_key.strip():
        env_lines.append(f"Environment=SHERPADESK_API_KEY={api_key.strip()}")
    api_user = skill_entry.get("apiUser")
    if isinstance(api_user, str) and api_user.strip():
        env_lines.append(f"Environment=SHERPADESK_API_USER={api_user.strip()}")
    return env_lines


def unit_contents(service_key: str = "maintenance") -> str:
    paths = ensure_path_layout()
    repo = _repo_root()
    python = paths.runtime_venv / "bin" / "python"
    exec_map = {
        "hot_watch": "hot-watch-run",
        "alert_dispatch": "alert-dispatch-run",
        "maintenance": "maintenance-run",
    }
    desc_map = {
        "hot_watch": "SherpaMind hot watch worker",
        "alert_dispatch": "SherpaMind alert dispatch worker",
        "maintenance": "SherpaMind maintenance worker",
    }
    env_block = "\n".join(_base_env_lines())
    return f"""[Unit]
Description={desc_map[service_key]}
After=default.target

[Service]
Type=simple
WorkingDirectory={repo}
{env_block}
ExecStart={python} -m sherpamind.cli {exec_map[service_key]}
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
"""


def write_unit_files() -> list[Path]:
    written: list[Path] = []
    for service_key, service_name in SERVICE_NAMES.items():
        path = _unit_path(service_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(unit_contents(service_key))
        written.append(path)
    legacy = _unit_path("sherpamind.service")
    if legacy.exists():
        legacy.unlink()
    return written


def daemon_reload() -> None:
    _run(["systemctl", "--user", "daemon-reload"])


def service_status() -> dict[str, Any]:
    units: dict[str, Any] = {}
    all_enabled = True
    all_active = True
    for service_key, service_name in SERVICE_NAMES.items():
        unit = _unit_path(service_name)
        enabled = _run(["systemctl", "--user", "is-enabled", service_name], check=False)
        active = _run(["systemctl", "--user", "is-active", service_name], check=False)
        unit_status = {
            "unit_path": str(unit),
            "unit_exists": unit.exists(),
            "enabled": enabled.returncode == 0 and enabled.stdout.strip() == "enabled",
            "active": active.returncode == 0 and active.stdout.strip() == "active",
            "enabled_raw": enabled.stdout.strip() or enabled.stderr.strip(),
            "active_raw": active.stdout.strip() or active.stderr.strip(),
        }
        all_enabled = all_enabled and unit_status["enabled"]
        all_active = all_active and unit_status["active"]
        units[service_key] = unit_status
    return {
        "units": units,
        "enabled": all_enabled,
        "active": all_active,
    }


def install_service(start_now: bool = True) -> ServiceCommandResult:
    units = write_unit_files()
    daemon_reload()
    for service_name in SERVICE_NAMES.values():
        _run(["systemctl", "--user", "enable", service_name])
    if start_now:
        for service_name in SERVICE_NAMES.values():
            _run(["systemctl", "--user", "restart", service_name])
    return ServiceCommandResult(status="ok", message="SherpaMind services installed.", details={"unit_paths": [str(u) for u in units], **service_status()})


def uninstall_service(stop_now: bool = True) -> ServiceCommandResult:
    for service_name in SERVICE_NAMES.values():
        if stop_now:
            _run(["systemctl", "--user", "stop", service_name], check=False)
        _run(["systemctl", "--user", "disable", service_name], check=False)
        unit = _unit_path(service_name)
        if unit.exists():
            unit.unlink()
    daemon_reload()
    return ServiceCommandResult(status="ok", message="SherpaMind services uninstalled.", details=service_status())


def restart_service() -> ServiceCommandResult:
    for service_name in SERVICE_NAMES.values():
        _run(["systemctl", "--user", "restart", service_name])
    return ServiceCommandResult(status="ok", message="SherpaMind services restarted.", details=service_status())


def stop_service() -> ServiceCommandResult:
    for service_name in SERVICE_NAMES.values():
        _run(["systemctl", "--user", "stop", service_name], check=False)
    return ServiceCommandResult(status="ok", message="SherpaMind services stopped.", details=service_status())


def start_service() -> ServiceCommandResult:
    for service_name in SERVICE_NAMES.values():
        _run(["systemctl", "--user", "start", service_name])
    return ServiceCommandResult(status="ok", message="SherpaMind services started.", details=service_status())


def doctor_service() -> dict[str, Any]:
    status = service_status()
    paths = ensure_path_layout()
    systemctl_available = shutil.which("systemctl") is not None
    return {
        **status,
        "runtime_python_exists": (paths.runtime_venv / "bin" / "python").exists(),
        "settings_file_exists": paths.settings_file.exists(),
        "api_user_file_exists": paths.api_user_file.exists(),
        "service_log_exists": paths.service_log.exists(),
        "hot_watch_log_exists": paths.hot_watch_log.exists(),
        "alert_dispatch_log_exists": paths.alert_dispatch_log.exists(),
        "maintenance_log_exists": paths.maintenance_log.exists(),
        "service_state_exists": paths.service_state_file.exists(),
        "systemctl_user_available": systemctl_available,
    }
