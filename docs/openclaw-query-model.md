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
- `ticket_logs`
- `ticket_time_logs`
- `sync_state`
- `ingest_runs`

### 2. Materialized retrieval documents
Use for natural-language recall and fuzzy investigation:
- `ticket_documents`

These documents are built from:
- ticket list fields
- selected enriched ticket-detail fields
- recent ticket log text

## How OpenClaw should reason about SherpaMind data

### For exact questions
Examples:
- how many tickets does account X have?
- how many open tickets exist right now?
- which technician has the highest recent load?

Best path:
- structured analysis commands or direct SQL-backed analysis functions

### For fuzzy/support-history questions
Examples:
- have we seen a problem like this before?
- what kinds of issues keep happening for account X?
- which tickets mention printer issues and what happened?

Best path:
- search/materialized ticket documents first
- then drill into structured rows or enriched ticket details as needed

## Current OpenClaw-friendly command surface

### Structured insight commands
- `sherpamind insight-snapshot`
- `sherpamind report-ticket-counts`
- `sherpamind report-status-counts`
- `sherpamind report-priority-counts`
- `sherpamind report-technician-counts`
- `sherpamind report-ticket-log-types`
- `sherpamind open-ticket-ages`
- `sherpamind recent-account-activity`
- `sherpamind recent-technician-load`

### Retrieval-oriented commands
- `sherpamind materialize-ticket-docs`
- `sherpamind search-ticket-docs "printer"`
- `sherpamind export-ticket-docs`

## Design rule

Do not force the LLM to parse raw SherpaDesk JSON blobs unless necessary.
Whenever a field becomes operationally useful more than once, promote it into:
- a real SQLite column
- a reusable analysis query
- the ticket document materialization layer

That is how SherpaMind becomes naturally queryable instead of technically queryable only in theory.
