# SherpaMind

> Canonical SherpaDesk API docs: <https://github.com/sherpadesk/api/wiki>

SherpaMind is a local-first SherpaDesk ingest, sync, enrichment, analysis, and OpenClaw retrieval toolkit.

## Goals

1. **Initial seeding**
   - Pull down relevant SherpaDesk data into a local database.
   - Capture tickets and their useful related entities.
   - Build a durable, queryable local knowledge base.

2. **Ongoing backend sync**
   - Keep the local dataset current through a long-running Python backend service.
   - Avoid spending model tokens on periodic work that belongs in a local backend.

3. **Rich source capture and structure**
   - Pull down as much useful SherpaDesk source data as practical without breaking rate/safety constraints.
   - Preserve strong structured metadata and deep ticket history where available.
   - Keep the stored data rich enough that OpenClaw can answer open-ended questions later.

4. **OpenClaw-friendly retrieval**
   - Materialize ticket documents/chunks and public Markdown artifacts for natural-language access.
   - Keep canonical truth in SQLite while exposing easy-to-consume derived views.
   - Focus on clean structure, cleanup, chunking, and retrieval quality rather than hardcoded interpretation.

## Architecture

SherpaMind is being refactored into a **distributable skill** with a split runtime/storage model:
- `.SherpaMind/private/` — canonical/private skill state (SQLite, watcher state, runtime venv, config, logs, service state)
- `.SherpaMind/public/` — derived OpenClaw-friendly artifacts (Markdown snapshots, exports, similar cacheable outputs)

Runtime architecture:
- **user-level systemd service** for automatic host startup
- **internal Python timers/loops** for hot/warm/cold sync, enrichment, and public artifact refresh
- **OpenClaw** used for querying/consuming outputs, not for pretending to be the backend scheduler

Data architecture:
- **SQLite** as the canonical structured system of record
- **materialized ticket documents/chunks** as replaceable retrieval caches for OpenClaw
- a future **semantic/vector sidecar** when the current SQL + docs/chunks stack needs deeper semantic recall

North star:
- maximize rich source capture
- improve cleanup/normalization
- preserve strong metadata and history
- generate clean, replaceable retrieval artifacts
- let OpenClaw perform the interpretation at query time instead of hardcoding brittle conclusions into SherpaMind

Project mantra:
- **Backend prepares the data**
- **Skill-front teaches access**
- **OpenClaw reasons at query time**

### Main components
- `SKILL.md` — skill entry instructions and operating contract
- `scripts/bootstrap.py` — skill-local runtime bootstrap
- `scripts/run.py` — stable runtime entrypoint through the skill-local venv
- `sherpamind.client` — SherpaDesk API client
- `sherpamind.db` — SQLite schema, migrations, and repository helpers
- `sherpamind.ingest` — initial seed and explicit sync lane logic
- `sherpamind.enrichment` — bounded detail enrichment for high-priority tickets
- `sherpamind.watch` — open-ticket polling/change detection
- `sherpamind.service_runtime` — long-running backend loop with internal timers
- `sherpamind.service_manager` — user-level service installation/status helpers
- `sherpamind.analysis` — reusable analytical queries and derived views
- `sherpamind.documents` — ticket document/chunk materialization for retrieval/search
- `sherpamind.public_artifacts` — generated Markdown/public artifacts under `.SherpaMind/public/docs`
- `sherpamind.cli` — operator-facing command surface

## Current status

SherpaMind has working live read-only integration against the real SherpaDesk account and is being transitioned from a repo-style tool into a self-contained skill with a local backend service model.

Implemented:
- initial live seed into SQLite for accounts, users, technicians, and tickets
- explicit sync lane logic for hot open tickets, warm recently closed tickets, and cold rolling closed audits
- watcher polling with local open-ticket state tracking
- selective priority-ticket detail enrichment
- ticket log ingestion from single-ticket detail responses
- attachment metadata ingestion only (no blob download by default)
- materialized ticket documents and deterministic ticket-document chunks for retrieval/query use
- generated public Markdown artifacts under `.SherpaMind/public/docs`
- structured insight/report commands plus text/chunk search commands
- skill-local bootstrap, runtime, config, doctor, legacy migration, and service management flow

Still not implemented:
- broad full-history detail enrichment across the entire corpus
- semantic/vector index sidecar
- native outbound watcher alert routing
- richer attachment/image analysis flows (intentionally deferred and opt-in only)

## Bootstrap and configuration

Skill-local bootstrap:

```bash
python3 scripts/bootstrap.py
```

This creates:
- `.SherpaMind/private/runtime/venv`
- `.SherpaMind/private/config.env`
- `.SherpaMind/public/exports`
- `.SherpaMind/public/docs`

Stable runtime entrypoint:

```bash
python3 scripts/run.py <command> [args...]
```

Useful onboarding commands:

```bash
python3 scripts/run.py workspace-layout
python3 scripts/run.py doctor
python3 scripts/run.py setup
python3 scripts/run.py migrate-legacy-state
python3 scripts/run.py cleanup-legacy-cron
python3 scripts/run.py configure --api-key <token>
python3 scripts/run.py discover-orgs
python3 scripts/run.py install-service
python3 scripts/run.py service-status
```

Environment variables are documented in `.env.example`.
Important conservative controls include:
- `SHERPAMIND_WORKSPACE_ROOT`
- `SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS`
- `SHERPAMIND_REQUEST_TIMEOUT_SECONDS`
- `SHERPAMIND_SEED_PAGE_SIZE`
- `SHERPAMIND_SEED_MAX_PAGES`
- `SHERPAMIND_SERVICE_*`
- `SHERPAMIND_API_HOURLY_LIMIT`
- `SHERPAMIND_API_BUDGET_WARN_RATIO`
- `SHERPAMIND_API_BUDGET_CRITICAL_RATIO`
- `SHERPAMIND_API_REQUEST_LOG_RETENTION_DAYS`

## Useful current commands

### Lifecycle / service
- `python3 scripts/run.py workspace-layout`
- `python3 scripts/run.py doctor`
- `python3 scripts/run.py setup`
- `python3 scripts/run.py migrate-legacy-state`
- `python3 scripts/run.py cleanup-legacy-cron`
- `python3 scripts/run.py configure`
- `python3 scripts/run.py install-service`
- `python3 scripts/run.py uninstall-service`
- `python3 scripts/run.py start-service`
- `python3 scripts/run.py stop-service`
- `python3 scripts/run.py restart-service`
- `python3 scripts/run.py service-status`
- `python3 scripts/run.py service-run`
- `python3 scripts/run.py service-run-once`

### Sync / enrichment / queries
- `python3 scripts/run.py discover-orgs`
- `python3 scripts/run.py seed`
- `python3 scripts/run.py watch`
- `python3 scripts/run.py sync-hot-open`
- `python3 scripts/run.py sync-warm-closed`
- `python3 scripts/run.py sync-cold-closed-audit`
- `python3 scripts/run.py enrich-priority-ticket-details`
- `python3 scripts/run.py materialize-ticket-docs`
- `python3 scripts/run.py dataset-summary`
- `python3 scripts/run.py insight-snapshot`
- `python3 scripts/run.py report-ticket-counts`
- `python3 scripts/run.py report-status-counts`
- `python3 scripts/run.py report-priority-counts`
- `python3 scripts/run.py report-technician-counts`
- `python3 scripts/run.py report-ticket-log-types`
- `python3 scripts/run.py report-attachment-summary`
- `python3 scripts/run.py recent-tickets`
- `python3 scripts/run.py open-ticket-ages`
- `python3 scripts/run.py recent-account-activity`
- `python3 scripts/run.py recent-technician-load`
- `python3 scripts/run.py search-ticket-docs`
- `python3 scripts/run.py search-ticket-chunks`
- `python3 scripts/run.py export-ticket-docs`
- `python3 scripts/run.py export-ticket-chunks`
- `python3 scripts/run.py export-embedding-chunks`
- `python3 scripts/run.py export-embedding-manifest`
- `python3 scripts/run.py build-vector-index`
- `python3 scripts/run.py report-vector-index-status`
- `python3 scripts/run.py search-vector-index`
- `python3 scripts/run.py generate-public-snapshot`

## External API caution

SherpaDesk integration should be implemented conservatively:
- consult the canonical API wiki before changing request behavior: <https://github.com/sherpadesk/api/wiki>
- confirm auth behavior explicitly
- confirm endpoint behavior explicitly
- rate-limit conservatively by default
- document live quirks in-repo
- avoid bursty seed/sync behavior
- keep secrets local only; do not commit API tokens or live account identifiers carelessly

Current verified direction:
- API base: `https://api.sherpadesk.com`
- organization discovery can use `x:{api_token}` Basic auth against `/organizations/`
- normal API access uses `{org_key}-{instance_key}:{api_token}` Basic auth
- stated rate limit is `600 requests/hour`

## Service model

SherpaMind’s normal background behavior should come from the local backend service, not OpenClaw cron.

The service tracks real SherpaDesk request usage in SQLite (`api_request_events`) and reports a rolling hourly usage budget so cadence can be tuned from measurements instead of guesswork. Lower-priority work can now back off when budget utilization gets high, and old request-event rows are pruned automatically by retention policy so the log stays bounded.

The intended first-install flow is:
1. `python3 scripts/bootstrap.py`
2. `python3 scripts/run.py setup`
3. `python3 scripts/run.py configure --api-key <token>`
4. `python3 scripts/run.py discover-orgs`
5. `python3 scripts/run.py configure --org-key <org> --instance-key <instance>`
6. `python3 scripts/run.py seed`
7. `python3 scripts/run.py install-service`
8. `python3 scripts/run.py generate-public-snapshot`

On update / re-bootstrap:
- preserve `.SherpaMind/private` and `.SherpaMind/public`
- rerun bootstrap safely to refresh the skill-local venv if needed
- rerun `doctor`
- migrate legacy state if needed
- archive old repo-local `state/` leftovers once migrated
- install/restart the user service idempotently
- clean up any legacy SherpaMind OpenClaw cron jobs

## Open questions

- Best long-term breadth/cadence for full-corpus detail enrichment without wasting API budget
- Whether SherpaDesk exposes additional useful comment/history/detail surfaces beyond the currently captured ticket detail + ticket log structures
- Preferred notification channel for native watcher alerts
- When to introduce the semantic/vector sidecar on top of the current SQLite + materialized-doc/chunk design
l for native watcher alerts
- When to introduce the semantic/vector sidecar on top of the current SQLite + materialized-doc/chunk design
