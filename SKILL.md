---
name: sherpamind
description: Read-only SherpaDesk ingest, sync, enrichment, analysis, and local backend service skill for OpenClaw. Use when you need to seed or sync SherpaDesk data into a local SQLite knowledge base, maintain hot/warm/cold ticket freshness through a host-started Python service, enrich tickets with logs and attachment metadata, materialize OpenClaw-facing ticket documents/chunks and public Markdown artifacts, or answer natural-language questions about SherpaDesk ticket history, account trends, technician workload, stale open tickets, and similar support operations questions.
---

# SherpaMind

Use the bundled bootstrap/runtime scripts instead of assuming global Python packages.

## Runtime model

- Keep all skill-generated state under `workspace/.SherpaMind/`
- Treat `.SherpaMind/private/` as non-indexed/internal data
- Treat `.SherpaMind/public/` as OpenClaw-friendly derived outputs
- Do not store attachment blobs by default; metadata only unless explicitly extending for targeted screenshot/image analysis
- Prefer a long-running local backend service over OpenClaw cron for normal periodic backend work

## Layout contract

- `.SherpaMind/private/sherpamind.sqlite3` — canonical SQLite store
- `.SherpaMind/private/watch_state.json` — watcher/open-ticket state
- `.SherpaMind/private/runtime/venv/` — skill-local Python environment
- `.SherpaMind/private/config.env` — local config/env file
- `.SherpaMind/private/logs/service.log` — backend service log
- `.SherpaMind/private/service-state.json` — service loop state
- `.SherpaMind/public/exports/` — exported JSONL and similar outputs
- `.SherpaMind/public/docs/` — generated Markdown/public NL artifacts

## First-use bootstrap

Run:

```bash
python3 scripts/bootstrap.py
```

This creates:
- `.SherpaMind/private/runtime/venv`
- `.SherpaMind/private/config.env`
- `.SherpaMind/public/exports`
- `.SherpaMind/public/docs`

It also installs pinned dependencies from `requirements.txt` and installs the bundled code into the skill-local venv.

## Stable runtime entrypoint

Run commands through:

```bash
python3 scripts/run.py <command> [args...]
```

Examples:

```bash
python3 scripts/run.py workspace-layout
python3 scripts/run.py doctor
python3 scripts/run.py setup
python3 scripts/run.py migrate-legacy-state
python3 scripts/run.py cleanup-legacy-cron
python3 scripts/run.py archive-legacy-state
python3 scripts/run.py configure --api-key <token>
python3 scripts/run.py discover-orgs
python3 scripts/run.py seed
python3 scripts/run.py install-service
python3 scripts/run.py service-status
python3 scripts/run.py generate-public-snapshot
```

## Onboarding flow

Typical first-run sequence:
1. `python3 scripts/bootstrap.py`
2. `python3 scripts/run.py setup`
3. `python3 scripts/run.py configure --api-key <token>`
4. `python3 scripts/run.py discover-orgs`
5. `python3 scripts/run.py configure --org-key <org> --instance-key <instance>`
6. `python3 scripts/run.py seed`
7. `python3 scripts/run.py install-service`
8. `python3 scripts/run.py generate-public-snapshot`

Update/re-bootstrap sequence:
1. `python3 scripts/bootstrap.py`
2. `python3 scripts/run.py doctor`
3. `python3 scripts/run.py migrate-legacy-state` (if upgrading from older repo-local state)
4. `python3 scripts/run.py cleanup-legacy-cron`
5. `python3 scripts/run.py install-service`
6. `python3 scripts/run.py restart-service`
7. `python3 scripts/run.py generate-public-snapshot`

## Service model

SherpaMind’s normal background work should come from the local Python backend service, not from OpenClaw cron.

The service is intended to run as a user-level systemd unit (`sherpamind.service`) and own:
- hot open watcher/sync cadence
- warm closed reconciliation cadence
- cold closed audit cadence
- priority enrichment cadence
- public snapshot refresh cadence

## Important operating rules

- Keep SherpaDesk access read-only unless the skill is explicitly expanded later
- Keep SQLite as canonical truth
- Treat `ticket_documents` / `ticket_document_chunks` and public Markdown docs as replaceable derived caches
- Regenerate and replace derived docs/chunks/artifacts for changed tickets so stale NL artifacts do not linger
- Keep attachment handling metadata-only by default
- Track real SherpaDesk request usage in SQLite and use it for budget-aware scheduling/backoff decisions
- Prune old API request-event rows periodically so request logging does not grow unbounded
- Clean up any old SherpaMind OpenClaw cron jobs; they are legacy refactor artifacts, not the intended runtime

## References

Read these when needed:
- `docs/openclaw-query-model.md` — OpenClaw-facing query/retrieval design
- `docs/delta-sync-strategy.md` — hot/warm/cold sync lane design
- `docs/api-reference.md` — verified API behavior and caveats
- `docs/automation.md` — service/install/update/health model
del.md` — OpenClaw-facing query/retrieval design
- `docs/delta-sync-strategy.md` — hot/warm/cold sync lane design
- `docs/api-reference.md` — verified API behavior and caveats
- `docs/automation.md` — service/install/update/health model
odel
