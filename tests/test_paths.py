from pathlib import Path

from sherpamind.paths import ensure_path_layout, resolve_paths


def test_resolve_paths_uses_workspace_root(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    paths = resolve_paths()
    assert paths.root == tmp_path / ".SherpaMind"
    assert paths.private_root == tmp_path / ".SherpaMind" / "private"
    assert paths.public_root == tmp_path / ".SherpaMind" / "public"
    assert paths.runtime_venv == tmp_path / ".SherpaMind" / "private" / "runtime" / "venv"


def test_ensure_path_layout_creates_directories(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    paths = ensure_path_layout()
    assert paths.private_root.exists()
    assert paths.public_root.exists()
    assert paths.exports_root.exists()
    assert paths.docs_root.exists()
