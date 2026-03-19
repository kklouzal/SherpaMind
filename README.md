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
Pull down all relevant SherpaDesk entities and populate the local database.

### Sync
Perform delta syncs based on modified timestamps or equivalent cursor state.

### Analyze
Run structured queries over local data for support, trend, and resolution analysis.

### Watch
Poll for newly created tickets and emit actionable summaries.

## Current status

Repository scaffold, caution docs, request-pacing foundations, and the first hybrid-storage direction are in place.
Live SherpaDesk integration still needs real auth/endpoint verification.

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

## Configuration knobs

Environment variables are documented in `.env.example`.
Important conservative controls include:
- `SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS`
- `SHERPAMIND_REQUEST_TIMEOUT_SECONDS`

## Open questions

- Exact SherpaDesk authentication details to use for the API client
- Exact endpoint set and pagination behavior required for tickets, users, and accounts
- Preferred notification channel for new-ticket watcher alerts
- Whether SQLite alone is sufficient long-term or whether an adjacent vector/search layer is desirable later
