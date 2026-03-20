# OpenClaw Query Model

## Goal

Make SherpaMind naturally usable from OpenClaw for open-ended questions without forcing the LLM to reverse-engineer raw SherpaDesk payloads every time.

## Runtime stance

SherpaMind’s background syncing/enrichment should come from its local Python backend service.
OpenClaw’s job is to query, inspect, summarize, and reason over SherpaMind outputs — not to burn tokens on acting like the periodic backend scheduler.

## Current local data layers

### 1. Canonical structured tables
Use for exact facts, counts, filters, state, and time-based analysis:
- `accounts`
- `users`
- `technicians`
- `tickets`
- `ticket_details`
- `ticket_attachments` (metadata only; no blob download by default)
- `ticket_logs`
- `ticket_time_logs`
- `sync_state`
- `ingest_runs`

### 2. Materialized retrieval documents
Use for natural-language recall and fuzzy investigation:
- `ticket_documents`
- `ticket_document_chunks`

These are derived from canonical tables and are **replaceable caches**, not source-of-truth memory.

### 3. Public markdown artifacts
Use for quick human/agent inspection under `.SherpaMind/public/docs/`, including:
- `index.md`
- `insight-snapshot.md`
- `stale-open-tickets.md`
- `recent-account-activity.md`
- `recent-technician-load.md`
- `runtime/status.md`

These are generated/replaced artifacts, not append-only memory files.

## How OpenClaw should reason about SherpaMind data

### For exact questions
Examples:
- how many tickets does account X have?
- how many open tickets exist right now?
- which technician has the highest recent load?
- which tickets have the most attachments?

Best path:
- structured analysis commands or direct SQL-backed analysis functions

### For fuzzy/support-history questions
Examples:
- have we seen a problem like this before?
- what kinds of issues keep happening for account X?
- which tickets mention printer issues and what happened?

Best path:
- search materialized ticket documents/chunks first
- then drill into structured rows or enriched ticket details as needed

## Staleness rule for derived artifacts

OpenClaw-facing NL artifacts must never become unmanaged side files.

Rules:
- canonical truth lives in structured SQLite tables
- materialized docs/chunks/public snapshots are derived caches
- when a ticket changes, regenerate its docs/chunks/artifacts by stable identity
- replace old docs/chunks for that ticket
- delete stale chunks that no longer belong to the current materialization

This prevents old stale natural-language support artifacts from lingering after ticket updates.

## Attachment rule

By default, SherpaMind stores **attachment metadata only**:
- attachment id
- filename
- URL/reference
- size
- recorded/upload timestamp

Do **not** auto-download attachment bodies by default.
Possible future exception:
- explicitly targeted screenshot/image retrieval for specific tickets when visual context is genuinely needed

## Current OpenClaw-friendly command surface

### Service/lifecycle commands
- `python3 scripts/run.py doctor`
- `python3 scripts/run.py service-status`
- `python3 scripts/run.py service-run-once`
- `python3 scripts/run.py generate-public-snapshot`

### Structured insight commands
- `python3 scripts/run.py insight-snapshot`
- `python3 scripts/run.py report-enrichment-coverage`
- `python3 scripts/run.py report-ticket-counts`
- `python3 scripts/run.py report-status-counts`
- `python3 scripts/run.py report-priority-counts`
- `python3 scripts/run.py report-technician-counts`
- `python3 scripts/run.py report-ticket-log-types`
- `python3 scripts/run.py report-attachment-summary`
- `python3 scripts/run.py open-ticket-ages`
- `python3 scripts/run.py recent-account-activity`
- `python3 scripts/run.py recent-technician-load`

### Retrieval-oriented commands
- `python3 scripts/run.py materialize-ticket-docs`
- `python3 scripts/run.py search-ticket-docs "printer"`
- `python3 scripts/run.py search-ticket-chunks "printer"`
- `python3 scripts/run.py search-vector-index "printer"`
- `python3 scripts/run.py search-vector-index "printer" --account "<account>" --status Open`
- `python3 scripts/run.py search-vector-index "printer" --technician "Kyle" --priority High --category Hardware`
- `python3 scripts/run.py report-vector-index-status`
- `python3 scripts/run.py export-ticket-docs`
- `python3 scripts/run.py export-embedding-chunks`
- `python3 scripts/run.py export-embedding-manifest`
- `python3 scripts/run.py generate-public-snapshot`
- `python3 scripts/run.py generate-runtime-status`

## Design rule

Do not force the LLM to parse raw SherpaDesk JSON blobs unless necessary.
Whenever a field becomes operationally useful more than once, promote it into:

SherpaMind should also expose enough observability that OpenClaw can trust the retrieval layer before leaning on it heavily. In practice that means coverage/freshness/readiness outputs should make it easy to see:
- how much of the corpus has detail enrichment
- whether ticket docs/chunks cover the full ticket set
- whether important retrieval metadata (category, issue summary, resolution summary) is materially populated
- whether chunk sizes look sane for vector/semantic use
- a real SQLite column/table
- a reusable structural query
- the ticket document/chunk materialization layer
- or a generated public artifact if it helps OpenClaw/humans inspect the state quickly

If a proposed feature mainly hardcodes conclusions that OpenClaw could derive at query time from well-prepared data, prefer better data preparation over baking that interpretation into SherpaMind.

That is how SherpaMind becomes naturally queryable instead of technically queryable only in theory.
r
- or a generated public artifact if it helps OpenClaw/humans inspect the state quickly

That is how SherpaMind becomes naturally queryable instead of technically queryable only in theory.
