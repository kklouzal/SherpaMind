import json
from pathlib import Path

from sherpamind import service_manager
from sherpamind.service_manager import unit_contents, write_unit_files


def test_unit_contents_contains_worker_run(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv('SHERPAMIND_ROOT', raising=False)
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
    assert 'Environment=SHERPADESK_API_KEY=' not in text
    assert 'PassEnvironment=SHERPADESK_API_KEY' in text
    assert 'CPUQuota=25%' in text
    assert 'CPUWeight=30' in text
    assert 'IOWeight=30' in text
    assert 'Nice=10' in text
    assert 'TasksMax=128' in text
    assert 'Restart=on-failure' in text
    assert 'RestartSec=30' in text
    assert 'StartLimitIntervalSec=300' in text
    assert 'NoNewPrivileges=true' in text
    assert 'PrivateTmp=true' in text

    calls = []
    monkeypatch.setattr(service_manager, '_run', lambda args, check=True: calls.append(args))
    written = write_unit_files()
    assert any(call[:3] == ['systemctl', '--user', 'set-environment'] for call in calls)
    set_env_call = next(call for call in calls if call[:3] == ['systemctl', '--user', 'set-environment'])
    assert 'SHERPADESK_API_KEY=ui-secret-key' in set_env_call


def test_unit_contents_preserves_direct_sherpamind_root(monkeypatch, tmp_path: Path) -> None:
    workspace_root = tmp_path / 'workspace'
    runtime_root = tmp_path / 'persistent' / '.SherpaMind'
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(workspace_root))
    monkeypatch.setenv('SHERPAMIND_ROOT', str(runtime_root))
    monkeypatch.setenv('HOME', str(tmp_path / 'home'))

    text = unit_contents('alert_dispatch')

    assert f'Environment=SHERPAMIND_WORKSPACE_ROOT={workspace_root}' in text
    assert f'Environment=SHERPAMIND_ROOT={runtime_root}' in text
    assert 'EnvironmentFile=' not in text
    assert 'PassEnvironment=SHERPADESK_API_KEY' in text
    assert 'CPUQuota=10%' in text
    assert 'CPUWeight=20' in text
    assert 'IOWeight=20' in text
