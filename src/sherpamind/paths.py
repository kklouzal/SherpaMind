from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os


@dataclass(frozen=True)
class SherpaMindPaths:
    workspace_root: Path
    root: Path
    private_root: Path
    public_root: Path
    runtime_root: Path
    db_path: Path
    watch_state_path: Path
    exports_root: Path
    docs_root: Path
    runtime_venv: Path
    env_file: Path
    logs_root: Path
    service_log: Path
    service_state_file: Path


def resolve_paths() -> SherpaMindPaths:
    workspace_root = Path(os.getenv("SHERPAMIND_WORKSPACE_ROOT", os.getcwd())).resolve()
    root = workspace_root / ".SherpaMind"
    private_root = root / "private"
    public_root = root / "public"
    runtime_root = private_root / "runtime"
    exports_root = public_root / "exports"
    docs_root = public_root / "docs"
    logs_root = private_root / "logs"
    return SherpaMindPaths(
        workspace_root=workspace_root,
        root=root,
        private_root=private_root,
        public_root=public_root,
        runtime_root=runtime_root,
        db_path=private_root / "sherpamind.sqlite3",
        watch_state_path=private_root / "watch_state.json",
        exports_root=exports_root,
        docs_root=docs_root,
        runtime_venv=runtime_root / "venv",
        env_file=private_root / "config.env",
        logs_root=logs_root,
        service_log=logs_root / "service.log",
        service_state_file=private_root / "service-state.json",
    )


def ensure_path_layout() -> SherpaMindPaths:
    paths = resolve_paths()
    for path in [
        paths.root,
        paths.private_root,
        paths.public_root,
        paths.runtime_root,
        paths.exports_root,
        paths.docs_root,
        paths.logs_root,
    ]:
        path.mkdir(parents=True, exist_ok=True)
    return paths
