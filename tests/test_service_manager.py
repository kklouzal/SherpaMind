import json
from pathlib import Path

from sherpamind.service_manager import unit_contents


def test_unit_contents_contains_worker_run(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    home = tmp_path / 'home'
    monkeypatch.setenv('HOME', str(home))
    (home / '.openclaw').mkdir(parents=True, exist_ok=True)
    (home / '.openclaw' / 'openclaw.json').write_text(json.dumps({
        'skills': {
            'entries': {
                'sherpamind': {
                    'apiKey': 'ui-secret-key'
                }
            }
        }
    }))
    text = unit_contents('hot_watch')
    assert 'ExecStart=' in text
    assert 'hot-watch-run' in text
    assert 'SHERPAMIND_WORKSPACE_ROOT=' in text
    assert 'EnvironmentFile=' not in text
    assert 'Environment=SHERPADESK_API_KEY=ui-secret-key' in text
