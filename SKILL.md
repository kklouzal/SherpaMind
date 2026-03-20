---
name: sherpamind
description: Use for SherpaDesk-related requests, especially ticket lookup, support-history retrieval, account/user/technician analysis, stale-ticket review, workload questions, operational reporting, and open-ended natural-language questions about SherpaDesk data. Activates when the user mentions SherpaDesk or asks about tickets, support issues, clients/accounts, technicians, resolution history, recurring incidents, backlog, response timing, or similar support operations analysis.
---

# SherpaMind

SherpaMind is the **SherpaDesk data/retrieval front-end skill** for OpenClaw.

Its purpose is **not** to replace OpenClaw’s reasoning.
Its purpose is to teach OpenClaw how to:
- access the prepared SherpaDesk data correctly
- use the right SherpaMind command surface efficiently
- prefer factual retrieval/structure over brittle hardcoded interpretation
- answer open-ended SherpaDesk questions from rich local data with high confidence

## Use this skill when

Activate SherpaMind whenever the user asks about **SherpaDesk** or anything strongly related to SherpaDesk support data, including:

- SherpaDesk tickets
- support issues/incidents/problems
- accounts/clients/customers in SherpaDesk
- users/contacts in SherpaDesk
- technicians / workload / assignments
- stale open tickets / backlog / waiting tickets
- recent ticket activity
- historical ticket context
- "have we seen this before?"
- "what happened with X ticket/account/user?"
- operational reporting over SherpaDesk data
- natural-language analytical questions over synced SherpaDesk history

Strong trigger phrases/examples:
- "SherpaDesk"
- "tickets"
- "support history"
- "account X"
- "technician Y"
- "backlog"
- "open tickets"
- "closed tickets"
- "stale tickets"
- "what do we know about this issue"
- "have we seen this before"
- "show me context for this problem"

## Core operating model

SherpaMind has two layers:

### 1. Background backend/service
A local Python backend keeps SherpaDesk data synced and enriched under:
- `.SherpaMind/private/` — canonical/private state
- `.SherpaMind/public/` — derived OpenClaw-friendly artifacts

OpenClaw should **not** act as the periodic backend scheduler for this skill.
That is handled by the SherpaMind service.

### 2. OpenClaw-facing query/retrieval layer
OpenClaw should use SherpaMind to:
- inspect structured SherpaDesk data
- retrieve cleaned ticket documents/chunks
- read public factual artifacts
- answer the user’s question using OpenClaw’s own reasoning on top of those inputs

## North star

SherpaMind should prioritize:
- richer source capture
- stronger cleanup/normalization
- stronger structured metadata
- better per-ticket / per-account / per-technician derived artifacts
- stable replaceable chunking
- retrieval/vector readiness

SherpaMind should **avoid** prematurely hardcoding interpretation when OpenClaw can reason at query time from well-prepared data.

So, when using this skill:
- prefer rich/factual retrieval over canned conclusions
- prefer structural summaries over opinionated summaries
- let OpenClaw interpret retrieved evidence for the specific user question

## Runtime / storage contract

- `.SherpaMind/private/sherpamind.sqlite3` — canonical SQLite store
- `.SherpaMind/private/watch_state.json` — watcher/open-ticket state
- `.SherpaMind/private/config.env` — local SherpaDesk config
- `.SherpaMind/private/runtime/venv/` — skill-local Python environment
- `.SherpaMind/private/logs/service.log` — service log
- `.SherpaMind/private/service-state.json` — backend loop state
- `.SherpaMind/public/docs/` — factual public Markdown artifacts
- `.SherpaMind/public/exports/` — JSONL exports, chunk exports, vector-ready exports

## Stable command entrypoint

Always use the stable runner:

```bash
python3 scripts/run.py <command> [args...]
```

Do **not** invent alternate runtime paths or assume global Python packages.

## Query strategy for OpenClaw

When answering user questions, choose the lightest/factual path that can answer the request.

### A. Use structured commands for factual questions
Use these first for exact/state/reporting questions:

- `python3 scripts/run.py dataset-summary`
- `python3 scripts/run.py report-api-usage`
- `python3 scripts/run.py report-enrichment-coverage`
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
- `python3 scripts/run.py account-summary "<account>"`
- `python3 scripts/run.py technician-summary "<technician>"`

Examples:
- "How many open tickets do we have?" → `report-status-counts`
- "What’s Kyle’s current backlog?" → `technician-summary "Kyle"`
- "What’s going on with <account>?" → `account-summary "<account>"`

### B. Use retrieval commands for fuzzy/open-ended context questions
Use these for investigative/natural-language recall:

- `python3 scripts/run.py search-ticket-docs "<query>"`
- `python3 scripts/run.py search-ticket-chunks "<query>"`
- `python3 scripts/run.py search-ticket-chunks "<query>" --account "<account>"`
- `python3 scripts/run.py search-ticket-chunks "<query>" --status Open`
- `python3 scripts/run.py search-ticket-chunks "<query>" --technician "<technician>"`
- `python3 scripts/run.py search-vector-index "<query>"`
- `python3 scripts/run.py search-vector-index "<query>" --account "<account>" --status Open`
- `python3 scripts/run.py search-vector-index "<query>" --technician "<technician>" --priority High --category "Hardware"`

Examples:
- "Have we seen this printer problem before?" → start with `search-ticket-chunks printer`, then widen with `search-vector-index printer` if keyword recall looks thin
- "Show me Outlook-related tickets for <account>" → `search-ticket-chunks Outlook --account "<account>"`
- "What context do we have around tickets Kyle touched related to MFA?" → `search-ticket-chunks MFA --technician "Kyle"`
- "Find semantically similar high-priority hardware issues" → `search-vector-index printer --priority High --category Hardware`

### C. Use public artifacts for quick factual context
Read from `.SherpaMind/public/docs/` when a concise factual derived artifact is enough:
- `index.md`
- `insight-snapshot.md`
- `stale-open-tickets.md`
- `recent-account-activity.md`
- `recent-technician-load.md`
- `runtime/status.md`
- `accounts/*.md`
- `technicians/*.md`

These are especially useful when OpenClaw needs quick context without pulling many command outputs.

## Preferred workflow for open-ended SherpaDesk questions

For a natural-language question like:
> "What’s been going on with account X lately, and have we seen this issue before?"

Preferred flow:
1. run a structural summary first
   - `account-summary "X"`
2. run retrieval for the issue/problem text
   - `search-ticket-chunks "<issue words>" --account "X"`
3. if needed, consult public docs for supporting context
4. answer using OpenClaw’s own reasoning over those retrieved results

Do **not** jump straight to a hand-authored conclusion if the retrieval evidence is thin.

## Setup / lifecycle commands

Use these for installation and maintenance, not normal user questions:

- `python3 scripts/bootstrap.py`
- `python3 scripts/run.py workspace-layout`
- `python3 scripts/run.py doctor`
- `python3 scripts/run.py setup`
- `python3 scripts/run.py migrate-legacy-state`
- `python3 scripts/run.py archive-legacy-state`
- `python3 scripts/run.py cleanup-legacy-cron`
- `python3 scripts/run.py configure --api-key <token>`
- `python3 scripts/run.py discover-orgs`
- `python3 scripts/run.py install-service`
- `python3 scripts/run.py restart-service`
- `python3 scripts/run.py service-status`

## Important boundaries

- Keep SherpaDesk access read-only unless explicitly expanded later
- Keep attachment handling metadata-only by default
- Do not auto-download attachment bodies by default
- Treat docs/chunks/public Markdown artifacts as replaceable derived caches
- Let OpenClaw do the interpretation; let SherpaMind do the data prep and retrieval prep

## Maintenance rule

If backend capabilities evolve, update the skill-front in the same wave whenever needed.
At minimum, re-check:
- activation guidance
- preferred command strategy
- examples
- references to public/private artifacts
- any commands that became newly available, obsolete, or misleading

## References

Read these when needed:
- `docs/architecture-doctrine.md` — project-wide backend/skill/OpenClaw boundary doctrine
- `docs/openclaw-query-model.md` — OpenClaw-facing query/retrieval design
- `docs/retrieval-architecture.md` — retrieval/vector-readiness design
- `docs/delta-sync-strategy.md` — hot/warm/cold sync lane design
- `docs/api-reference.md` — verified API behavior and caveats
- `docs/automation.md` — service/install/update/health model
