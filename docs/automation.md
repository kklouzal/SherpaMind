# Automation

## Runtime model

SherpaMind should run as a **long-lived local Python backend service**, not as a set of token-burning OpenClaw cron jobs.

Why:
- syncing/enrichment/public artifact refresh are backend chores, not conversational agent turns
- a service can wake on host startup and run internal timers without spending model tokens
- OpenClaw should query and consume SherpaMind outputs, not pretend to be the scheduler for a Python backend

## Service model

SherpaMind uses a user-level systemd service where available:
- unit name: `sherpamind.service`
- managed through `systemctl --user`
- executes the skill-local venv python with `-m sherpamind.cli service-run`

## Internal periodic lanes

The backend service owns the periodic work directly:
- hot open watcher/sync loop
- warm closed reconciliation loop
- cold closed audit loop
- priority enrichment loop
- public snapshot generation loop
- periodic service-state/health marker updates

These run from internal Python timers, not OpenClaw cron.

## Install vs update behavior

### First install
- bootstrap the skill-local runtime
- migrate legacy state if needed
- initialize the DB
- optionally generate initial public docs
- install/start the user service
- doctor the result
- clean up any legacy SherpaMind cron jobs

### Update / re-bootstrap
- rerun bootstrap safely
- preserve `.SherpaMind/private` and `.SherpaMind/public`
- reinstall/rewrite the systemd user unit idempotently
- restart the service safely
- doctor the runtime
- regenerate public artifacts if needed
- clean up any old SherpaMind cron jobs that no longer belong

## Commands

### Service management
- `python3 scripts/run.py install-service`
- `python3 scripts/run.py uninstall-service`
- `python3 scripts/run.py start-service`
- `python3 scripts/run.py stop-service`
- `python3 scripts/run.py restart-service`
- `python3 scripts/run.py service-status`
- `python3 scripts/run.py service-run` (foreground/debug)
- `python3 scripts/run.py service-run-once`

### Lifecycle helpers
- `python3 scripts/run.py setup`
- `python3 scripts/run.py doctor`
- `python3 scripts/run.py migrate-legacy-state`
- `python3 scripts/run.py cleanup-legacy-cron`

## Legacy cron cleanup

SherpaMind briefly used OpenClaw cron during refactor exploration. That is no longer the desired architecture.

`cleanup-legacy-cron` removes any old managed SherpaMind cron jobs so the system converges on the service-first model.

## Health checking

`doctor` should verify:
- runtime venv exists
- config exists
- DB exists
- watch state exists
- systemd user service file exists
- service enabled/active state
- service log/state file presence
- legacy repo-local state presence (for upgrade hints)
- any leftover legacy SherpaMind cron jobs that should be cleaned up
