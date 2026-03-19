# OpenClaw Query Model

## Goal

Make SherpaMind naturally usable from OpenClaw for open-ended questions without forcing the LLM to reverse-engineer raw SherpaDesk payloads every time.

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
- materialized docs/chunks are derived caches
- when a ticket changes, regenerate its docs/chunks by stable identity
- replace old docs/chunks for that ticket
- delete stale chunks that no longer belong to the current materialization

This prevents old stale natural-language support artifacts from lingering after ticket updates.

## Attachment rule

By default, SherpaMind should store **attachment metadata only**:
- attachment id
- filename
- URL/reference
- size
- recorded/upload timestamp

Do **not** auto-download attachment bodies by default.
Possible future exception:
- explicitly targeted screenshot/image retrieval for specific tickets when visual context is genuinely needed

## Current OpenClaw-friendly command surface

### Structured insight commands
- `sherpamind insight-snapshot`
- `sherpamind report-ticket-counts`
- `sherpamind report-status-counts`
- `sherpamind report-priority-counts`
- `sherpamind report-technician-counts`
- `sherpamind report-ticket-log-types`
- `sherpamind report-attachment-summary`
- `sherpamind open-ticket-ages`
- `sherpamind recent-account-activity`
- `sherpamind recent-technician-load`

### Retrieval-oriented commands
- `sherpamind materialize-ticket-docs`
- `sherpamind search-ticket-docs "printer"`
- `sherpamind search-ticket-chunks "printer"`
- `sherpamind export-ticket-docs`

## Design rule

Do not force the LLM to parse raw SherpaDesk JSON blobs unless necessary.
Whenever a field becomes operationally useful more than once, promote it into:
- a real SQLite column/table
- a reusable analysis query
- the ticket document/chunk materialization layer

That is how SherpaMind becomes naturally queryable instead of technically queryable only in theory.
