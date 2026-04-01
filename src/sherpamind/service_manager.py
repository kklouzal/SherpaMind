from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil
import subprocess
from typing import Any

from .paths import ensure_path_layout

SERVICE_NAME = "sherpamind.service"


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


def _unit_path() -> Path:
    return Path.home() / ".config" / "systemd" / "user" / SERVICE_NAME


def _run(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=check)


def unit_contents() -> str:
    paths = ensure_path_layout()
    repo = _repo_root()
    python = paths.runtime_venv / "bin" / "python"
    skill_entry = _read_openclaw_skill_entry()
    env_lines = [
        f"Environment=SHERPAMIND_WORKSPACE_ROOT={paths.workspace_root}",
    ]
    api_key = skill_entry.get("apiKey")
    if isinstance(api_key, str) and api_key.strip():
        env_lines.append(f"Environment=SHERPADESK_API_KEY={api_key.strip()}")
    api_user = skill_entry.get("apiUser")
    if isinstance(api_user, str) and api_user.strip():
        env_lines.append(f"Environment=SHERPADESK_API_USER={api_user.strip()}")
    env_block = "\n".join(env_lines)
    return f"""[Unit]
Description=SherpaMind background sync service
After=default.target

[Service]
Type=simple
WorkingDirectory={repo}
{env_block}
ExecStart={python} -m sherpamind.cli service-run
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
"""


def write_unit_file() -> Path:
    path = _unit_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(unit_contents())
    return path


def daemon_reload() -> None:
    _run(["systemctl", "--user", "daemon-reload"])


def service_status() -> dict[str, Any]:
    unit = _unit_path()
    enabled = _run(["systemctl", "--user", "is-enabled", SERVICE_NAME], check=False)
    active = _run(["systemctl", "--user", "is-active", SERVICE_NAME], check=False)
    return {
        "unit_path": str(unit),
        "unit_exists": unit.exists(),
        "enabled": enabled.returncode == 0 and enabled.stdout.strip() == "enabled",
        "active": active.returncode == 0 and active.stdout.strip() == "active",
        "enabled_raw": enabled.stdout.strip() or enabled.stderr.strip(),
        "active_raw": active.stdout.strip() or active.stderr.strip(),
    }


def install_service(start_now: bool = True) -> ServiceCommandResult:
    unit = write_unit_file()
    daemon_reload()
    _run(["systemctl", "--user", "enable", SERVICE_NAME])
    if start_now:
        _run(["systemctl", "--user", "restart", SERVICE_NAME])
    return ServiceCommandResult(status="ok", message="SherpaMind service installed.", details={"unit_path": str(unit), **service_status()})


def uninstall_service(stop_now: bool = True) -> ServiceCommandResult:
    if stop_now:
        _run(["systemctl", "--user", "stop", SERVICE_NAME], check=False)
    _run(["systemctl", "--user", "disable", SERVICE_NAME], check=False)
    unit = _unit_path()
    if unit.exists():
        unit.unlink()
    daemon_reload()
    return ServiceCommandResult(status="ok", message="SherpaMind service uninstalled.", details=service_status())


def restart_service() -> ServiceCommandResult:
    _run(["systemctl", "--user", "restart", SERVICE_NAME])
    return ServiceCommandResult(status="ok", message="SherpaMind service restarted.", details=service_status())


def stop_service() -> ServiceCommandResult:
    _run(["systemctl", "--user", "stop", SERVICE_NAME], check=False)
    return ServiceCommandResult(status="ok", message="SherpaMind service stopped.", details=service_status())


def start_service() -> ServiceCommandResult:
    _run(["systemctl", "--user", "start", SERVICE_NAME])
    return ServiceCommandResult(status="ok", message="SherpaMind service started.", details=service_status())


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
        "service_state_exists": paths.service_state_file.exists(),
        "systemctl_user_available": systemctl_available,
    }
