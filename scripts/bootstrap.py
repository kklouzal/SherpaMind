#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import venv


DEFAULT_ENV_TEMPLATE = """# SherpaMind skill-local config
# Fill in SherpaDesk credentials/keys here or use `python3 scripts/run.py configure ...`.
SHERPADESK_API_BASE_URL=https://api.sherpadesk.com
SHERPADESK_API_KEY=
SHERPADESK_API_USER=
SHERPADESK_ORG_KEY=
SHERPADESK_INSTANCE_KEY=
SHERPAMIND_NOTIFY_CHANNEL=
"""


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def workspace_root() -> Path:
    return Path(os.getenv("SHERPAMIND_WORKSPACE_ROOT", os.getcwd())).resolve()


def sherpamind_root() -> Path:
    return workspace_root() / ".SherpaMind"


def skill_runtime_root() -> Path:
    return sherpamind_root() / "private" / "runtime"


def env_file() -> Path:
    return sherpamind_root() / "private" / "config.env"


def venv_python(venv_root: Path) -> Path:
    return venv_root / "bin" / "python"


def ensure_layout() -> None:
    for path in [
        sherpamind_root(),
        sherpamind_root() / "private",
        sherpamind_root() / "public",
        sherpamind_root() / "private" / "runtime",
        sherpamind_root() / "public" / "exports",
        sherpamind_root() / "public" / "docs",
    ]:
        path.mkdir(parents=True, exist_ok=True)
    if not env_file().exists():
        env_file().write_text(DEFAULT_ENV_TEMPLATE)


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
    ensure_layout()
    runtime_root = skill_runtime_root()
    venv_root = runtime_root / "venv"
    ensure_venv(venv_root)
    pip_install(venv_root)
    print(str(venv_python(venv_root)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
