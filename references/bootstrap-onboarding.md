# Bootstrap / onboarding

Use this when SherpaMind is being installed, reviewed for portability, or prepared on a new OpenClaw instance.

## Goal

Turn this repo into a working SherpaMind installation without storing runtime state inside the skill tree.

Default runtime layout lives under the resolved workspace root:
- `.SherpaMind/private/config/`
- `.SherpaMind/private/data/`
- `.SherpaMind/private/state/`
- `.SherpaMind/private/logs/`
- `.SherpaMind/private/runtime/`
- `.SherpaMind/public/`

Use `python3 scripts/run.py workspace-layout` to verify the real paths. Override with `SHERPAMIND_WORKSPACE_ROOT` when the operator wants a different workspace root, or `SHERPAMIND_ROOT=/path/to/.SherpaMind` when a moved/reinstalled skill must reuse pre-existing runtime data directly.

## First command

```bash
cd {baseDir}
python3 scripts/run.py bootstrap-audit
```

Use `--summary` for terse line-oriented output.

## What bootstrap-audit tells you

- resolved skill root, workspace root, and runtime root
- staged runtime path layout
- whether the runtime venv already exists
- whether the OpenClaw-provided API key plus org / instance are present
- whether the local DB exists yet
- whether user-level `systemd` is available and whether the split worker services are already installed
- explicit next steps before unattended mode is attempted

## Recommended bootstrap flow

1. Audit first:
   ```bash
   cd {baseDir}
   python3 scripts/run.py bootstrap-audit
   ```
2. Bootstrap the runtime venv and staged dirs:
   ```bash
   cd {baseDir}
   python3 scripts/bootstrap.py
   python3 scripts/run.py setup
   python3 scripts/run.py doctor
   ```
3. Ensure the OpenClaw `sherpamind` skill entry is configured with the SherpaDesk API key and any wanted alert fields (`config.newTicketAlertsEnabled`, `config.ticketUpdateAlertsEnabled`, `config.newTicketAlertChannel`, `config.ticketUpdateAlertChannel`).
4. Discover organizations/instances, then stage the chosen org/instance:
   ```bash
   cd {baseDir}
   python3 scripts/run.py discover-orgs
   python3 scripts/run.py configure --org-key <org> --instance-key <instance>
   ```
5. Seed the local dataset and validate derived artifacts:
   ```bash
   cd {baseDir}
   python3 scripts/run.py seed
   python3 scripts/run.py generate-public-snapshot
   python3 scripts/run.py generate-runtime-status
   python3 scripts/run.py dataset-summary
   ```
6. Only after the above is healthy, decide whether unattended background mode is wanted:
   ```bash
   cd {baseDir}
   python3 scripts/run.py install-service
   python3 scripts/run.py service-status
   ```
   This installs the split worker runtime (`hot-watch`, `alert-dispatch`, `maintenance`) rather than one monolithic daemon.

## Non-goals

- Do not put runtime state back into the repo.
- Do not make user-level service installation the first onboarding step.
- Do not center inline API-token arguments in the happy-path install story.
- Do not claim unattended automation is healthy unless bootstrap-audit/doctor/service-status agree.
