# SherpaMind

> Canonical SherpaDesk API docs: <https://github.com/sherpadesk/api/wiki>

SherpaMind is a local-first SherpaDesk ingest, sync, enrichment, analysis, and OpenClaw retrieval toolkit.

## Goals

1. **Initial seeding**
   - Pull down relevant SherpaDesk data into a local database.
   - Capture tickets and their useful related entities.
   - Build a durable, queryable local knowledge base.

2. **Ongoing delta sync**
   - Keep the local dataset current by periodically ingesting new/updated records.
   - Make it easy for AI and operators to reason over current support/account activity.

3. **Analytical access**
   - Support questions like:
     - common issue types by account or user
     - response/close times by account, user, or technician
     - recurring problem themes
     - resolution patterns and operational bottlenecks

4. **OpenClaw-friendly retrieval**
   - Materialize ticket documents/chunks and public Markdown artifacts for natural-language access.
   - Keep canonical truth in SQLite while exposing easy-to-consume derived views.

5. **New-ticket watcher**
   - Detect newly created tickets on a schedule.
   - Produce alert-ready structured summaries and analysis.
   - Suggest likely next questions and possible solution directions.

## Architecture

SherpaMind is being refactored into a **distributable skill** with a split runtime/storage model:
- `.SherpaMind/private/` — canonical/private skill state (SQLite, watcher state, runtime venv, config)
- `.SherpaMind/public/` — derived OpenClaw-friendly artifacts (Markdown snapshots, exports, similar cacheable outputs)

Data architecture:
- **SQLite** as the canonical structured system of record
- **materialized ticket documents/chunks** as replaceable retrieval caches for OpenClaw
- a future **semantic/vector sidecar** when the current SQL + docs/chunks stack needs deeper semantic recall

### Main components
- `SKILL.md` — skill entry instructions and operating contract
- `scripts/bootstrap.py` — skill-local runtime bootstrap
- `scripts/run.py` — stable runtime entrypoint through the skill-local venv
- `sherpamind.client` — SherpaDesk API client
- `sherpamind.db` — SQLite schema, migrations, and repository helpers
- `sherpamind.ingest` — initial seed and delta sync logic
- `sherpamind.enrichment` — bounded detail enrichment for high-priority tickets
- `sherpamind.watch` — new-ticket polling and alert payload generation
- `sherpamind.analysis` — reusable analytical queries and derived views
- `sherpamind.documents` — ticket document/chunk materialization for retrieval/search
- `sherpamind.public_artifacts` — generated Markdown/public artifacts under `.SherpaMind/public/docs`
- `sherpamind.cli` — operator-facing command surface

## Current status

SherpaMind is now beyond scaffold stage and has working live read-only integration against the real SherpaDesk account.

Implemented:
- initial live seed into SQLite for accounts, users, technicians, and tickets
- tiered delta lanes for hot open tickets, warm recently closed tickets, and cold rolling closed audits
- watcher polling with local open-ticket state tracking
- selective priority-ticket detail enrichment
- ticket log ingestion from single-ticket detail responses
- attachment metadata ingestion only (no blob download by default)
- materialized ticket documents and deterministic ticket-document chunks for retrieval/query use
- generated public Markdown artifacts under `.SherpaMind/public/docs`
- structured insight/report commands plus text/chunk search commands
- skill-local bootstrap, runtime, config, doctor, and legacy migration flow

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
python3 scripts/run.py configure --api-key <token>
python3 scripts/run.py discover-orgs
```

Environment variables are documented in `.env.example`.
Important conservative controls include:
- `SHERPAMIND_WORKSPACE_ROOT`
- `SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS`
- `SHERPAMIND_REQUEST_TIMEOUT_SECONDS`
- `SHERPAMIND_SEED_PAGE_SIZE`
- `SHERPAMIND_SEED_MAX_PAGES`

## Useful current commands

- `python3 scripts/run.py workspace-layout`
- `python3 scripts/run.py doctor`
- `python3 scripts/run.py setup`
- `python3 scripts/run.py migrate-legacy-state`
- `python3 scripts/run.py configure`
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

## Delta sync direction

SherpaDesk currently looks better suited to **tiered recency rescans** than to a clean server-side dirty-record pull.

Current documented direction:
- new/open active slice refreshed about every 5 minutes
- maintain a local set of currently observed open ticket IDs
- closed tickets newer than 7 days treated as warm and reconciled every few hours
- older closed history audited in small rolling batches

See `docs/delta-sync-strategy.md` for the reasoning and proposed lane design.

## Install vs update behavior

### First install
The intended first-install flow is:
1. `python3 scripts/bootstrap.py`
2. `python3 scripts/run.py setup`
3. `python3 scripts/run.py configure --api-key <token>`
4. `python3 scripts/run.py discover-orgs`
5. `python3 scripts/run.py configure --org-key <org> --instance-key <instance>`
6. `python3 scripts/run.py seed`
7. `python3 scripts/run.py generate-public-snapshot`

### Update / re-bootstrap
On update, SherpaMind should **reconcile**, not duplicate:
- keep `.SherpaMind/private` and `.SherpaMind/public` intact
- preserve config and SQLite data
- rerun `bootstrap.py` safely to refresh the skill-local venv if needed
- rerun `setup` or `reconcile-automation` to ensure expected cron jobs exist and duplicates are removed
- rely on `doctor` to verify the runtime, config, DB, legacy-state presence, and managed automation state

Cron job management is intentionally **manifest + reconcile** based. Managed SherpaMind jobs are recreated by stable name so updates do not accumulate duplicates.

## Open questions

- Best long-term breadth/cadence for full-corpus detail enrichment without wasting API budget
- Whether SherpaDesk exposes additional useful comment/history/detail surfaces beyond the currently captured ticket detail + ticket log structures
- Preferred notification channel for native watcher alerts
- When to introduce the semantic/vector sidecar on top of the current SQLite + materialized-doc/chunk design
