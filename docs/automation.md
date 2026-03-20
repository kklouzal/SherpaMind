# Automation

## Scheduling model

SherpaMind uses OpenClaw **cron** for precise recurring jobs.
Heartbeat remains useful for broad main-session awareness, but the actual SherpaMind sync/enrichment/public-artifact jobs are intended to be isolated cron jobs so they persist across restarts and do not depend on an active chat turn.

## Managed cron jobs

SherpaMind manages a stable manifest of cron jobs by name:
- `sherpamind:hot-open-sync`
- `sherpamind:warm-closed-sync`
- `sherpamind:cold-closed-audit`
- `sherpamind:priority-enrichment`
- `sherpamind:public-snapshot`
- `sherpamind:doctor`

## Idempotent install/update behavior

SherpaMind does **not** treat automation setup as append-only.
Instead, it uses a **manifest + reconcile** model:
- list existing managed jobs
- remove the current managed set
- recreate the expected managed set from the current manifest

This makes updates and re-bootstrap safe:
- expected jobs are restored
- duplicates are removed
- drifted job definitions are normalized to the current skill definition

## First install vs update

### First install
- bootstrap the skill-local runtime
- run `setup`
- configure credentials
- discover org/instance
- seed
- generate public snapshot

### Update / re-bootstrap
- rerun bootstrap safely
- run `doctor`
- migrate legacy state if applicable
- run `reconcile-automation`
- regenerate public artifacts as needed

## Health checking

`doctor` should verify:
- runtime venv exists
- config exists
- DB exists
- legacy repo-local state presence (for upgrade hints)
- managed automation missing/duplicate state

## Delivery stance

SherpaMind cron jobs should default to internal/no-delivery unless a specific alerting job is intentionally configured otherwise.
The watcher/sync/enrichment/public-artifact jobs are maintenance workflows, not chat-spam generators.
