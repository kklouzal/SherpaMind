import json
from pathlib import Path

from sherpamind.paths import ensure_path_layout
from sherpamind.settings import load_settings, stage_connection_settings


def test_load_settings_reads_request_controls(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPADESK_API_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_API_USER", raising=False)
    monkeypatch.delenv("SHERPADESK_ORG_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_INSTANCE_KEY", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS", "3.5")
    monkeypatch.setenv("SHERPAMIND_REQUEST_TIMEOUT_SECONDS", "45")
    settings = load_settings()
    assert settings.request_min_interval_seconds == 3.5
    assert settings.request_timeout_seconds == 45.0


def test_load_settings_defaults_paths(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPADESK_API_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_API_USER", raising=False)
    monkeypatch.delenv("SHERPADESK_ORG_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_INSTANCE_KEY", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    settings = load_settings()
    assert settings.db_path == tmp_path / ".SherpaMind" / "private" / "data" / "sherpamind.sqlite3"
    assert settings.watch_state_path == tmp_path / ".SherpaMind" / "private" / "state" / "watch_state.json"


def test_load_settings_reads_seed_controls(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPADESK_API_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_API_USER", raising=False)
    monkeypatch.delenv("SHERPADESK_ORG_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_INSTANCE_KEY", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("SHERPAMIND_SEED_PAGE_SIZE", "50")
    monkeypatch.setenv("SHERPAMIND_SEED_MAX_PAGES", "3")
    settings = load_settings()
    assert settings.seed_page_size == 50
    assert settings.seed_max_pages == 3


def test_load_settings_reads_service_controls(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPADESK_API_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_API_USER", raising=False)
    monkeypatch.delenv("SHERPADESK_ORG_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_INSTANCE_KEY", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("SHERPAMIND_SERVICE_HOT_OPEN_EVERY_SECONDS", "123")
    monkeypatch.setenv("SHERPAMIND_SERVICE_ENRICHMENT_LIMIT", "77")
    monkeypatch.setenv("SHERPAMIND_SERVICE_COLD_BOOTSTRAP_EVERY_SECONDS", "456")
    monkeypatch.setenv("SHERPAMIND_SERVICE_ENRICHMENT_BOOTSTRAP_LIMIT", "222")
    monkeypatch.setenv("SHERPAMIND_COLD_CLOSED_BOOTSTRAP_PAGES_PER_RUN", "9")
    settings = load_settings()
    assert settings.service_hot_open_every_seconds == 123
    assert settings.service_enrichment_limit == 77
    assert settings.service_cold_bootstrap_every_seconds == 456
    assert settings.service_enrichment_bootstrap_limit == 222
    assert settings.cold_closed_bootstrap_pages_per_run == 9


def test_staged_connection_settings_are_loaded_but_api_key_remains_env_only(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPADESK_API_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_API_USER", raising=False)
    monkeypatch.delenv("SHERPADESK_ORG_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_INSTANCE_KEY", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    settings_file = stage_connection_settings(org_key="org1", instance_key="inst1")
    assert settings_file.exists()
    settings = load_settings()
    assert settings.api_key is None
    assert settings.org_key == "org1"
    assert settings.instance_key == "inst1"


def test_load_settings_uses_openclaw_skill_entry_only_for_non_key_context(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPADESK_API_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_API_USER", raising=False)
    monkeypatch.delenv("SHERPADESK_ORG_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_INSTANCE_KEY", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    (home / ".openclaw").mkdir(parents=True, exist_ok=True)
    (home / ".openclaw" / "openclaw.json").write_text(json.dumps({
        "skills": {
            "entries": {
                "sherpamind": {
                    "apiKey": "ui-secret-key",
                    "orgKey": "org-ui",
                    "instanceKey": "inst-ui"
                }
            }
        }
    }))

    settings = load_settings()
    assert settings.api_key is None
    assert settings.org_key == "org-ui"
    assert settings.instance_key == "inst-ui"


def test_load_settings_reads_alert_config_from_openclaw_skill_entry(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPADESK_API_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_API_USER", raising=False)
    monkeypatch.delenv("SHERPADESK_ORG_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_INSTANCE_KEY", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    (home / ".openclaw").mkdir(parents=True, exist_ok=True)
    (home / ".openclaw" / "openclaw.json").write_text(json.dumps({
        "skills": {
            "entries": {
                "sherpamind": {
                    "config": {
                        "newTicketAlertsEnabled": True,
                        "ticketUpdateAlertsEnabled": True,
                        "newTicketAlertChannel": "channel:1488924125736079492",
                        "ticketUpdateAlertChannel": "channel:1488924125736079492",
                    }
                }
            }
        }
    }))
    settings_file = stage_connection_settings(
        org_key="org1",
        instance_key="inst1",
        openclaw_webhook_url="http://127.0.0.1:18789/hooks/agent",
        openclaw_webhook_token="token123",
    )

    settings_text = settings_file.read_text()
    paths = ensure_path_layout()
    assert "SHERPAMIND_OPENCLAW_WEBHOOK_TOKEN" not in settings_text
    assert paths.openclaw_webhook_token_file.read_text().strip() == "token123"

    settings = load_settings()
    assert settings.new_ticket_alerts_enabled is True
    assert settings.ticket_update_alerts_enabled is True
    assert settings.openclaw_webhook_url == "http://127.0.0.1:18789/hooks/agent"
    assert settings.openclaw_webhook_token == "token123"
    assert settings.new_ticket_alert_channel == "channel:1488924125736079492"
    assert settings.ticket_update_alert_channel == "channel:1488924125736079492"


def test_load_settings_defaults_openclaw_hook_from_local_config(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPAMIND_OPENCLAW_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SHERPAMIND_OPENCLAW_WEBHOOK_TOKEN", raising=False)
    monkeypatch.delenv("SHERPADESK_API_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_API_USER", raising=False)
    monkeypatch.delenv("SHERPADESK_ORG_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_INSTANCE_KEY", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    (home / ".openclaw").mkdir(parents=True, exist_ok=True)
    (home / ".openclaw" / "openclaw.json").write_text(json.dumps({
        "gateway": {"port": 19999},
        "hooks": {
            "enabled": True,
            "path": "/hooks",
            "token": "hook-token",
            "allowedAgentIds": ["main"],
        },
    }))

    settings = load_settings()
    assert settings.openclaw_webhook_url == "http://127.0.0.1:19999/hooks/agent"
    assert settings.openclaw_webhook_token == "hook-token"


def test_staged_openclaw_hook_settings_override_local_config(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPAMIND_OPENCLAW_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SHERPAMIND_OPENCLAW_WEBHOOK_TOKEN", raising=False)
    monkeypatch.delenv("SHERPADESK_API_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_API_USER", raising=False)
    monkeypatch.delenv("SHERPADESK_ORG_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_INSTANCE_KEY", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    (home / ".openclaw").mkdir(parents=True, exist_ok=True)
    (home / ".openclaw" / "openclaw.json").write_text(json.dumps({
        "gateway": {"port": 19999},
        "hooks": {"enabled": True, "path": "/hooks", "token": "auto-token"},
    }))
    stage_connection_settings(
        openclaw_webhook_url="http://127.0.0.1:18789/custom/agent",
        openclaw_webhook_token="staged-token",
    )

    settings = load_settings()
    assert settings.openclaw_webhook_url == "http://127.0.0.1:18789/custom/agent"
    assert settings.openclaw_webhook_token == "staged-token"
    assert "SHERPAMIND_OPENCLAW_WEBHOOK_TOKEN" not in ensure_path_layout().settings_file.read_text()


def test_load_settings_migrates_legacy_webhook_token_to_secret_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPAMIND_OPENCLAW_WEBHOOK_TOKEN", raising=False)
    monkeypatch.delenv("SHERPADESK_API_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_API_USER", raising=False)
    monkeypatch.delenv("SHERPADESK_ORG_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_INSTANCE_KEY", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    paths = ensure_path_layout()
    paths.settings_file.parent.mkdir(parents=True, exist_ok=True)
    paths.settings_file.write_text("SHERPAMIND_OPENCLAW_WEBHOOK_TOKEN=legacy-token\n")

    settings = load_settings()

    assert settings.openclaw_webhook_token == "legacy-token"
    assert paths.openclaw_webhook_token_file.read_text().strip() == "legacy-token"
    assert "SHERPAMIND_OPENCLAW_WEBHOOK_TOKEN" not in paths.settings_file.read_text()


def test_load_settings_reads_api_user_secret_file(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("SHERPADESK_API_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_API_USER", raising=False)
    monkeypatch.delenv("SHERPADESK_ORG_KEY", raising=False)
    monkeypatch.delenv("SHERPADESK_INSTANCE_KEY", raising=False)
    monkeypatch.setenv("SHERPAMIND_WORKSPACE_ROOT", str(tmp_path))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    paths = ensure_path_layout()
    paths.api_user_file.parent.mkdir(parents=True, exist_ok=True)
    paths.api_user_file.write_text("api-user@example.com\n")

    settings = load_settings()

    assert settings.api_user == "api-user@example.com"
