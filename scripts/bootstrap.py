#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import venv


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def skill_runtime_root() -> Path:
    workspace_root = Path(os.getenv("SHERPAMIND_WORKSPACE_ROOT", os.getcwd())).resolve()
    return workspace_root / ".SherpaMind" / "private" / "runtime"


def venv_python(venv_root: Path) -> Path:
    return venv_root / "bin" / "python"


def ensure_venv(venv_root: Path) -> None:
    if venv_python(venv_root).exists():
        return
    venv_root.parent.mkdir(parents=True, exist_ok=True)
    builder = venv.EnvBuilder(with_pip=True)
    builder.create(venv_root)


def pip_install(venv_root: Path) -> None:
    python = venv_python(venv_root)
    requirements = repo_root() / "requirements.txt"
    subprocess.run([str(python), "-m", "pip", "install", "--upgrade", "pip"], check=True)
    subprocess.run([str(python), "-m", "pip", "install", "-r", str(requirements)], check=True)
    subprocess.run([str(python), "-m", "pip", "install", "-e", str(repo_root())], check=True)


def main() -> int:
    runtime_root = skill_runtime_root()
    venv_root = runtime_root / "venv"
    ensure_venv(venv_root)
    pip_install(venv_root)
    print(str(venv_python(venv_root)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
