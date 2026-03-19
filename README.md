# SherpaMind

> Canonical SherpaDesk API docs: <https://github.com/sherpadesk/api/wiki>

SherpaMind is a local-first SherpaDesk data ingestion, sync, analysis, and alerting toolkit.

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

4. **New-ticket watcher**
   - Detect newly created tickets on a schedule.
   - Produce alert-ready structured summaries and analysis.
   - Suggest likely next questions and possible solution directions.

## Architecture

SherpaMind is designed as a Python toolkit backed by **hybrid local storage**:
- **SQLite** as the canonical structured system of record
- a planned **retrieval sidecar** for semantic/vector and keyword search over ticket/comment knowledge

### Main components
- `sherpamind.client` — SherpaDesk API client
- `sherpamind.db` — SQLite schema, migrations, and repository helpers
- `sherpamind.ingest` — initial seed and delta sync logic
- `sherpamind.watch` — new-ticket polling and alert payload generation
- `sherpamind.analysis` — reusable analytical queries and derived views
- retrieval/export pipeline — document building, chunking, FTS/vector indexing for OpenClaw-facing retrieval
- `sherpamind.cli` — operator-facing command surface

## Planned workflows

### Seed
Pull down relevant SherpaDesk entities and populate the local database.

Current implemented seed slice:
- accounts
- users
- technicians
- tickets

### Sync
Perform delta syncs based on modified timestamps or equivalent cursor state.

### Analyze
Run structured queries over local data for support, trend, and resolution analysis.

### Watch
Poll for newly created tickets and emit actionable summaries.

## Current status

SherpaMind is now beyond scaffold stage and has working live read-only integration against the real SherpaDesk account.

Implemented today:
- initial live seed into SQLite for accounts, users, technicians, and tickets
- tiered delta lanes for hot open tickets, warm recently closed tickets, and cold rolling closed audits
- watcher polling with local open-ticket state tracking
- selective priority-ticket detail enrichment
- ticket log ingestion from single-ticket detail responses
- attachment metadata ingestion only (no blob download by default)
- materialized ticket documents and deterministic ticket-document chunks for retrieval/query use
- structured insight/report commands plus text/chunk search commands

Still not implemented:
- broad full-history detail enrichment across the entire corpus
- semantic/vector index sidecar
- native outbound watcher alert routing
- richer attachment/image analysis flows (intentionally deferred and opt-in only)

## Retrieval / OpenClaw access strategy

SherpaMind should support OpenClaw through a **hybrid query model**:
- **SQL / structured queries** for exact metrics, counts, SLA-like timing, ownership, and state
- **keyword/full-text retrieval** for precise strings, account names, products, error messages, and IDs
- **vector/semantic retrieval** for fuzzy problem similarity, recurring issue themes, prior resolutions, and investigative context gathering

This is intentionally **not** a vector-only system. The SherpaDesk dataset mixes hard operational facts with messy support prose, so the right shape is canonical structured storage plus a retrieval sidecar.

## External API caution

SherpaDesk integration should be implemented conservatively:
- consult the canonical API wiki before changing request behavior: <https://github.com/sherpadesk/api/wiki>
- confirm auth behavior explicitly
- confirm endpoint behavior explicitly
- rate-limit conservatively by default
- document live quirks in-repo
- avoid bursty seed/sync behavior
- keep secrets local only; do not commit API tokens or live account identifiers carelessly

Current wiki-derived direction:
- API base: `https://api.sherpadesk.com`
- organization discovery can likely use `x:{api_token}` Basic auth against `/organizations/`
- normal API access appears to use `{org_key}-{instance_key}:{api_token}` Basic auth
- stated rate limit is `600 requests/hour`

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

## Delta sync direction

SherpaDesk currently looks better suited to **tiered recency rescans** than to a clean server-side dirty-record pull.

Current documented direction:
- new/open active slice refreshed about every 5 minutes
- maintain a local set of currently observed open ticket IDs
- closed tickets newer than 7 days treated as warm and reconciled every few hours
- older closed history audited in small rolling batches

See `docs/delta-sync-strategy.md` for the reasoning and proposed lane design.

## Open questions

- Best long-term breadth/cadence for full-corpus detail enrichment without wasting API budget
- Whether SherpaDesk exposes additional useful comment/history/detail surfaces beyond the currently captured ticket detail + ticket log structures
- Preferred notification channel for native watcher alerts
- When to introduce the semantic/vector sidecar on top of the current SQLite + materialized-doc/chunk design
