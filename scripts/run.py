#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def workspace_root() -> Path:
    explicit_workspace = os.getenv("SHERPAMIND_WORKSPACE_ROOT")
    if explicit_workspace:
        return Path(explicit_workspace).expanduser().resolve()
    explicit_root = os.getenv("SHERPAMIND_ROOT")
    if explicit_root:
        root = Path(explicit_root).expanduser().resolve()
        return root.parent if root.name == ".SherpaMind" else root
    repo = repo_root().resolve()
    if repo.parent.name == "skills":
        return repo.parent.parent.resolve()
    for parent in [repo, *repo.parents]:
        if parent.name == "workspace" and parent.parent.name == ".openclaw":
            return parent.resolve()
    current = Path.cwd().resolve()
    for parent in [current, *current.parents]:
        if parent.name == "workspace" and parent.parent.name == ".openclaw":
            return parent.resolve()
    return current


def sherpamind_root() -> Path:
    explicit_root = os.getenv("SHERPAMIND_ROOT")
    if explicit_root:
        return Path(explicit_root).expanduser().resolve()
    return workspace_root() / ".SherpaMind"


def venv_python() -> Path:
    return sherpamind_root() / "private" / "runtime" / "venv" / "bin" / "python"


def ensure_bootstrap() -> Path:
    python = venv_python()
    if python.exists():
        return python
    bootstrap = repo_root() / "scripts" / "bootstrap.py"
    subprocess.run([sys.executable, str(bootstrap)], check=True)
    return python


def main() -> int:
    python = ensure_bootstrap()
    env = os.environ.copy()
    if os.getenv("SHERPAMIND_ROOT"):
        env.setdefault("SHERPAMIND_ROOT", str(sherpamind_root()))
    env.setdefault("SHERPAMIND_WORKSPACE_ROOT", str(workspace_root()))
    env["PYTHONPATH"] = str(repo_root() / "src") + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    cmd = [str(python), "-m", "sherpamind.cli", *sys.argv[1:]]
    return subprocess.call(cmd, env=env, cwd=str(repo_root()))


if __name__ == "__main__":
    raise SystemExit(main())
