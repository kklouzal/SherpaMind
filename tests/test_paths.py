from pathlib import Path

from sherpamind.paths import discover_sherpamind_root, discover_workspace_root, ensure_path_layout, resolve_paths


def test_resolve_paths_uses_workspace_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPAMIND_ROOT", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    paths = resolve_paths()
    assert paths.root == tmp_path / ".SherpaMind"
    assert paths.config_root == tmp_path / ".SherpaMind" / "private" / "config"
    assert paths.secrets_root == tmp_path / ".SherpaMind" / "private" / "secrets"
    assert paths.data_root == tmp_path / ".SherpaMind" / "private" / "data"
    assert paths.state_root == tmp_path / ".SherpaMind" / "private" / "state"
    assert paths.runtime_venv == tmp_path / ".SherpaMind" / "private" / "runtime" / "venv"


def test_ensure_path_layout_creates_directories(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPAMIND_ROOT", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    paths = ensure_path_layout()
    assert paths.config_root.exists()
    assert not paths.secrets_root.exists()
    assert paths.data_root.exists()
    assert paths.state_root.exists()
    assert paths.public_root.exists()
    assert paths.exports_root.exists()
    assert paths.docs_root.exists()


def test_discover_workspace_root_prefers_openclaw_workspace_parent(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPAMIND_ROOT", raising=False)
    monkeypatch.delenv("SHERPAMIND_WORKSPACE_ROOT", raising=False)
    repo_root = tmp_path / "skills" / "sherpamind"
    repo_root.mkdir(parents=True)
    assert discover_workspace_root(repo_root=repo_root, cwd=tmp_path / "elsewhere") == tmp_path


def test_resolve_paths_can_point_directly_at_existing_sherpamind_root(monkeypatch, tmp_path: Path) -> None:
    runtime_root = tmp_path / "kept-state" / ".SherpaMind"
    monkeypatch.delenv("SHERPAMIND_WORKSPACE_ROOT", raising=False)
    monkeypatch.setenv("SHERPAMIND_ROOT", str(runtime_root))

    paths = resolve_paths()

    assert paths.workspace_root == runtime_root.parent
    assert paths.root == runtime_root
    assert paths.db_path == runtime_root / "private" / "data" / "sherpamind.sqlite3"
    assert discover_sherpamind_root() == runtime_root


def test_explicit_workspace_root_still_allows_direct_sherpamind_root(monkeypatch, tmp_path: Path) -> None:
    workspace_root = tmp_path / "workspace"
    runtime_root = tmp_path / "persistent" / ".SherpaMind"
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(workspace_root))
    monkeypatch.setenv("SHERPAMIND_ROOT", str(runtime_root))

    paths = resolve_paths()

    assert paths.workspace_root == workspace_root
    assert paths.root == runtime_root
