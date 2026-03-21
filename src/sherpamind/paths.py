from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import shutil


def discover_workspace_root(*, repo_root: Path | None = None, cwd: Path | None = None) -> Path:
    explicit = os.getenv("SHERPAMIND_WORKSPACE_ROOT")
    if explicit:
        return Path(explicit).resolve()
    repo = (repo_root or Path(__file__).resolve().parents[2]).resolve()
    if repo.parent.name == "skills":
        return repo.parent.parent.resolve()
    return (cwd or Path.cwd()).resolve()


@dataclass(frozen=True)
class SherpaMindPaths:
    workspace_root: Path
    root: Path
    config_root: Path
    secrets_root: Path
    data_root: Path
    state_root: Path
    logs_root: Path
    runtime_root: Path
    public_root: Path
    exports_root: Path
    docs_root: Path
    settings_file: Path
    api_key_file: Path
    api_user_file: Path
    db_path: Path
    watch_state_path: Path
    service_state_file: Path
    service_log: Path
    runtime_venv: Path
    legacy_private_root: Path
    legacy_env_file: Path


SECRET_FILE_MODE = 0o600


def resolve_paths() -> SherpaMindPaths:
    workspace_root = discover_workspace_root()
    root = workspace_root / ".SherpaMind"
    config_root = root / "config"
    secrets_root = root / "secrets"
    data_root = root / "data"
    state_root = root / "state"
    logs_root = root / "logs"
    runtime_root = root / "runtime"
    public_root = root / "public"
    exports_root = public_root / "exports"
    docs_root = public_root / "docs"
    legacy_private_root = root / "private"
    return SherpaMindPaths(
        workspace_root=workspace_root,
        root=root,
        config_root=config_root,
        secrets_root=secrets_root,
        data_root=data_root,
        state_root=state_root,
        logs_root=logs_root,
        runtime_root=runtime_root,
        public_root=public_root,
        exports_root=exports_root,
        docs_root=docs_root,
        settings_file=config_root / "settings.env",
        api_key_file=secrets_root / "sherpadesk_api_key.txt",
        api_user_file=secrets_root / "sherpadesk_api_user.txt",
        db_path=data_root / "sherpamind.sqlite3",
        watch_state_path=state_root / "watch_state.json",
        service_state_file=state_root / "service-state.json",
        service_log=logs_root / "service.log",
        runtime_venv=runtime_root / "venv",
        legacy_private_root=legacy_private_root,
        legacy_env_file=legacy_private_root / "config.env",
    )


def _write_secret_file(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value.strip() + "\n", encoding="utf-8")
    try:
        path.chmod(SECRET_FILE_MODE)
    except OSError:
        pass


def _read_env_file(path: Path) -> dict[str, str]:
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


def _write_settings_file(path: Path, values: dict[str, str]) -> None:
    ordered_keys = [
        "SHERPADESK_API_BASE_URL",
        "SHERPADESK_ORG_KEY",
        "SHERPADESK_INSTANCE_KEY",
        "SHERPAMIND_NOTIFY_CHANNEL",
        "SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS",
        "SHERPAMIND_REQUEST_TIMEOUT_SECONDS",
        "SHERPAMIND_SEED_PAGE_SIZE",
        "SHERPAMIND_SEED_MAX_PAGES",
        "SHERPAMIND_HOT_OPEN_PAGES",
        "SHERPAMIND_WARM_CLOSED_PAGES",
        "SHERPAMIND_WARM_CLOSED_DAYS",
        "SHERPAMIND_COLD_CLOSED_PAGES_PER_RUN",
        "SHERPAMIND_SERVICE_HOT_OPEN_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_WARM_CLOSED_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_COLD_CLOSED_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_ENRICHMENT_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_PUBLIC_SNAPSHOT_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_VECTOR_REFRESH_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_DOCTOR_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_ENRICHMENT_LIMIT",
        "SHERPAMIND_SERVICE_COLD_BOOTSTRAP_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_ENRICHMENT_BOOTSTRAP_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_ENRICHMENT_BOOTSTRAP_LIMIT",
        "SHERPAMIND_COLD_CLOSED_BOOTSTRAP_PAGES_PER_RUN",
        "SHERPAMIND_API_HOURLY_LIMIT",
        "SHERPAMIND_API_BUDGET_WARN_RATIO",
        "SHERPAMIND_API_BUDGET_CRITICAL_RATIO",
        "SHERPAMIND_API_REQUEST_LOG_RETENTION_DAYS",
    ]
    lines = [
        "# SherpaMind staged non-secret settings",
        "# Runtime state lives under .SherpaMind/ outside the skill tree.",
        "# Secrets are stored separately under .SherpaMind/secrets/.",
    ]
    for key in ordered_keys:
        if key in values:
            lines.append(f"{key}={values[key]}")
    for key in sorted(values):
        if key not in ordered_keys:
            lines.append(f"{key}={values[key]}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _migrate_legacy_layout(paths: SherpaMindPaths) -> None:
    private_root = paths.legacy_private_root
    if not private_root.exists():
        return

    legacy_values = _read_env_file(paths.legacy_env_file)
    current_settings = _read_env_file(paths.settings_file)
    changed_settings = False
    for key in [
        "SHERPADESK_API_BASE_URL",
        "SHERPADESK_ORG_KEY",
        "SHERPADESK_INSTANCE_KEY",
        "SHERPAMIND_NOTIFY_CHANNEL",
        "SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS",
        "SHERPAMIND_REQUEST_TIMEOUT_SECONDS",
        "SHERPAMIND_SEED_PAGE_SIZE",
        "SHERPAMIND_SEED_MAX_PAGES",
        "SHERPAMIND_HOT_OPEN_PAGES",
        "SHERPAMIND_WARM_CLOSED_PAGES",
        "SHERPAMIND_WARM_CLOSED_DAYS",
        "SHERPAMIND_COLD_CLOSED_PAGES_PER_RUN",
        "SHERPAMIND_SERVICE_HOT_OPEN_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_WARM_CLOSED_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_COLD_CLOSED_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_ENRICHMENT_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_PUBLIC_SNAPSHOT_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_VECTOR_REFRESH_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_DOCTOR_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_ENRICHMENT_LIMIT",
        "SHERPAMIND_SERVICE_COLD_BOOTSTRAP_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_ENRICHMENT_BOOTSTRAP_EVERY_SECONDS",
        "SHERPAMIND_SERVICE_ENRICHMENT_BOOTSTRAP_LIMIT",
        "SHERPAMIND_COLD_CLOSED_BOOTSTRAP_PAGES_PER_RUN",
        "SHERPAMIND_API_HOURLY_LIMIT",
        "SHERPAMIND_API_BUDGET_WARN_RATIO",
        "SHERPAMIND_API_BUDGET_CRITICAL_RATIO",
        "SHERPAMIND_API_REQUEST_LOG_RETENTION_DAYS",
    ]:
        value = legacy_values.get(key)
        if value is not None and key not in current_settings:
            current_settings[key] = value
            changed_settings = True
    if changed_settings or (legacy_values and not paths.settings_file.exists()):
        _write_settings_file(paths.settings_file, current_settings)

    if not paths.api_key_file.exists() and legacy_values.get("SHERPADESK_API_KEY"):
        _write_secret_file(paths.api_key_file, legacy_values["SHERPADESK_API_KEY"])
    if not paths.api_user_file.exists() and legacy_values.get("SHERPADESK_API_USER"):
        _write_secret_file(paths.api_user_file, legacy_values["SHERPADESK_API_USER"])

    file_moves: list[tuple[Path, Path]] = [
        (private_root / "sherpamind.sqlite3", paths.db_path),
        (private_root / "watch_state.json", paths.watch_state_path),
        (private_root / "service-state.json", paths.service_state_file),
        (private_root / "logs" / "service.log", paths.service_log),
    ]
    for source, target in file_moves:
        if source.exists() and not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(target))

    legacy_runtime = private_root / "runtime"
    if legacy_runtime.exists():
        if not any(paths.runtime_root.iterdir()):
            for child in sorted(legacy_runtime.iterdir()):
                shutil.move(str(child), str(paths.runtime_root / child.name))
            try:
                legacy_runtime.rmdir()
            except OSError:
                pass


def ensure_path_layout() -> SherpaMindPaths:
    paths = resolve_paths()
    for path in [
        paths.root,
        paths.config_root,
        paths.secrets_root,
        paths.data_root,
        paths.state_root,
        paths.logs_root,
        paths.runtime_root,
        paths.public_root,
        paths.exports_root,
        paths.docs_root,
    ]:
        path.mkdir(parents=True, exist_ok=True)
    _migrate_legacy_layout(paths)
    if not paths.settings_file.exists():
        _write_settings_file(paths.settings_file, {"SHERPADESK_API_BASE_URL": "https://api.sherpadesk.com"})
    return paths
