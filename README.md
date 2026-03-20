# SherpaMind

> Canonical SherpaDesk API docs: <https://github.com/sherpadesk/api/wiki>

SherpaMind is a local-first SherpaDesk ingest, sync, enrichment, retrieval-preparation, and analysis system built for OpenClaw.

It keeps canonical SherpaDesk data in SQLite, derives rebuildable retrieval artifacts from that data, runs background maintenance through a local Python service, and exposes a CLI for sync, observability, analysis, and search.

## Project stance

SherpaMind follows a strict split:

- **Backend prepares the data**
- **Skill-front teaches access**
- **OpenClaw reasons at query time**

That split matters:

- SherpaMind owns ingest, sync, cleanup, normalization, metadata extraction, enrichment, chunking, indexing, public artifact generation, and background runtime behavior.
- The skill-front owns activation/query guidance for OpenClaw.
- OpenClaw owns interpretation, synthesis, and user-facing reasoning.

SherpaMind is not meant to hardcode brittle conclusions into the backend. It is meant to make SherpaDesk data easy to trust, inspect, search, and reason over.

## Public-repo anonymization rule

This repository is intended to be safe for public distribution.

Treat anonymization as a standing development rule:

- keep secrets, tokens, and local runtime state out of the repo
- keep customer/account/user/technician examples anonymized in docs, tests, fixtures, and comments
- prefer placeholders like `<account>`, `<technician>`, `Acme`, `User One`, and `Tech One` over real names
- keep any live validation notes or API observations generalized unless specific identities are strictly necessary
- if a change adds new documentation, examples, fixtures, or reference material, scrub it before commit rather than cleaning it up later

Assume future autonomous development should preserve public-safe, anonymous examples by default.

## What SherpaMind does

SherpaMind currently covers five major areas:

1. **Local capture of SherpaDesk data**
   - discovers organizations
   - authenticates against the real SherpaDesk API
   - seeds accounts, users, technicians, and tickets into SQLite
   - tracks ingest runs and sync state

2. **Ongoing sync and bounded enrichment**
   - watches open tickets
   - refreshes hot open-ticket state
   - reconciles recently closed tickets
   - performs rolling cold closed-ticket audits
   - enriches a bounded priority ticket set through single-ticket detail fetches
   - stores ticket logs and attachment metadata from detail responses

3. **Retrieval preparation**
   - materializes ticket documents from canonical rows
   - chunks long documents deterministically
   - supports keyword/text search over docs and chunks
   - exports embedding-ready chunk payloads
   - builds and queries a local vector index

4. **Operator and OpenClaw observability**
   - reports dataset counts and freshness
   - reports enrichment coverage and retrieval coverage
   - reports API usage and hourly budget pressure
   - reports vector index readiness and drift
   - generates public Markdown artifacts for lightweight inspection

5. **Local backend runtime**
   - runs as a user-level systemd service
   - performs periodic sync/enrichment/artifact-refresh work without burning OpenClaw tokens
   - keeps service state, logs, and request-usage history locally

## Current live state

SherpaMind is live against a real read-only SherpaDesk account and is running as a local service on this host.

Observed from the current local runtime state:

- service unit exists and is active/enabled
- canonical database lives at `.SherpaMind/private/sherpamind.sqlite3`
- current live dataset includes **43 accounts**, **495 users**, **2 technicians**, and **12,041 tickets**
- current enrichment coverage includes **114 ticket details**, **752 ticket logs**, and **97 attachment metadata rows**
- materialized retrieval layer includes **12,040 ticket documents** and **12,081 ticket document chunks**
- local vector index includes **12,081 indexed chunks** with **0 missing**, **0 dangling**, and **0 outdated** index rows at the latest check
- current request-budget telemetry shows the system operating well below the documented 600 requests/hour ceiling during normal service activity
- current public artifact set exists under `.SherpaMind/public/docs/`
- current test suite passes locally (`50 passed` in the latest run)

The dataset is live and changes as SherpaDesk changes. Generated status artifacts reflect the last generation time, not a frozen project milestone.

## OpenClaw skill packaging

This repository is intended to be the **skill bundle root**.

The packaging target is:
- zip the repository contents
- install that bundle into an OpenClaw skill location on another instance
- let that OpenClaw instance discover the bundled `SKILL.md`

In other words, the repo itself is the thing being packaged and installed. It is **not** required to be installed into the current local OpenClaw during development.

Minimum required skill file:

- `SKILL.md`

Useful supporting files in this repo:

- `scripts/` — stable runtime/bootstrap entrypoints
- `references/` — deeper reference material that the skill can point to when needed
- `src/` — implementation
- `requirements.txt` / `pyproject.toml` — Python dependency and package metadata
- `.env.example` — public config template
- `tests/` — development validation; useful during development, not required by OpenClaw at runtime

Treat this as the strict bundle boundary:

**Keep in the bundle**
- `SKILL.md`
- `scripts/`
- `references/`
- `src/`
- `requirements.txt`
- `pyproject.toml`
- `.env.example`
- optionally `tests/` if you want the distributable repo to carry its validation surface

**Keep out of the bundle**
- `.git/`
- `.venv/`
- `.SherpaMind/`
- `.pytest_cache/`
- `__pycache__/`
- `*.pyc`
- `*.egg-info/`
- `build/`
- `dist/`
- `.env.local` or any other secret-bearing local env files

Files under `.SherpaMind/`, `.venv/`, `.pytest_cache/`, `__pycache__/`, and other ignored runtime/state paths are development/runtime artifacts, not bundle-defining skill files.

For distribution, the important rule is that the repo root contains a valid `SKILL.md`, uses repo-relative instructions, and ships the supporting files the skill instructions rely on.

## Architecture

### Storage layout

SherpaMind uses a skill-local split storage model:

- `.SherpaMind/private/`
  - canonical SQLite database
  - persistent config
  - service state
  - watch state
  - logs
  - runtime venv
  - legacy migration leftovers when applicable

- `.SherpaMind/public/`
  - derived Markdown artifacts for OpenClaw/human inspection
  - JSONL exports for retrieval/indexing workflows

Canonical truth stays in SQLite.
Derived artifacts are rebuildable caches, not source-of-truth memory.

### Data layers

SherpaMind currently uses these main data layers:

#### 1. Canonical structured SQLite tables
Used for exact facts, filters, counts, freshness, and analysis:

- `accounts`
- `users`
- `technicians`
- `tickets`
- `ticket_details`
- `ticket_logs`
- `ticket_time_logs`
- `ticket_attachments`
- `ticket_comments`
- `ingest_runs`
- `sync_state`
- `api_request_events`

#### 2. Derived retrieval documents
Used for natural-language recall and investigation:

- `ticket_documents`
- `ticket_document_chunks`
- `vector_chunk_index`

These are rebuildable from canonical data.

#### 3. Public inspection artifacts
Generated under `.SherpaMind/public/docs/` for lightweight OpenClaw/human access.

Current public artifact surface includes:

- `index.md`
- `insight-snapshot.md`
- `stale-open-tickets.md`
- `recent-account-activity.md`
- `recent-technician-load.md`
- `runtime/status.md`
- per-account docs under `accounts/`
- per-technician docs under `technicians/`

### Runtime model

SherpaMind’s normal background behavior comes from a local Python backend service, not OpenClaw cron.

The service owns:

- hot open-ticket watcher/sync work
- warm closed-ticket reconciliation
- cold rolling audit work
- bounded priority enrichment
- runtime status generation
- public snapshot generation
- service-state updates
- request-budget tracking and retention cleanup

Legacy OpenClaw cron usage is considered old architecture and is explicitly removable through the CLI.

## Verified API/auth behavior

Current verified live behavior:

- API base: `https://api.sherpadesk.com`
- organization discovery works with `x:{api_token}` Basic auth against `/organizations/`
- normal API access uses `{org_key}-{instance_key}:{api_token}` Basic auth
- stated rate limit is `600 requests/hour`

Current operating bias is conservative:

- verify endpoint behavior before widening usage
- rate-limit requests conservatively
- keep secrets local only
- prefer bounded enrichment and measured sync lanes over aggressive crawling

## Retrieval model

SherpaMind uses a hybrid retrieval approach:

- **SQLite** for canonical structured truth
- **keyword/text search** for exact-text lookup over materialized docs/chunks
- **local vector search** for similarity retrieval over chunked ticket content

This supports three different query styles:

- exact structured questions
- exact-text investigative questions
- fuzzy “have we seen something like this before?” questions

SherpaMind also exposes retrieval-readiness observability so OpenClaw can tell whether the prepared retrieval layer is current before leaning on it heavily.

## Attachment handling

SherpaMind stores **attachment metadata only** by default.

Stored attachment fields include things like:

- attachment identifier
- filename
- URL/reference
- size
- upload/recorded timestamp

SherpaMind does **not** automatically download attachment bodies by default.
Targeted future attachment/image retrieval remains an explicit opt-in path, not the default ingest model.

## CLI command surface

Stable runtime entrypoint:

```bash
python3 scripts/run.py <command> [args...]
```

### Lifecycle and setup

- `python3 scripts/bootstrap.py`
- `python3 scripts/run.py workspace-layout`
- `python3 scripts/run.py init-db`
- `python3 scripts/run.py setup`
- `python3 scripts/run.py configure`
- `python3 scripts/run.py doctor`
- `python3 scripts/run.py migrate-legacy-state`
- `python3 scripts/run.py archive-legacy-state`
- `python3 scripts/run.py cleanup-legacy-cron`

### Service management

- `python3 scripts/run.py install-service`
- `python3 scripts/run.py uninstall-service`
- `python3 scripts/run.py start-service`
- `python3 scripts/run.py stop-service`
- `python3 scripts/run.py restart-service`
- `python3 scripts/run.py service-status`
- `python3 scripts/run.py service-run`
- `python3 scripts/run.py service-run-once`

### SherpaDesk ingest and sync

- `python3 scripts/run.py discover-orgs`
- `python3 scripts/run.py seed`
- `python3 scripts/run.py sync`
- `python3 scripts/run.py watch`
- `python3 scripts/run.py sync-hot-open`
- `python3 scripts/run.py sync-warm-closed`
- `python3 scripts/run.py sync-cold-closed-audit`
- `python3 scripts/run.py enrich-priority-ticket-details`
- `python3 scripts/run.py materialize-ticket-docs`

### Reporting and analysis

- `python3 scripts/run.py dataset-summary`
- `python3 scripts/run.py report-api-usage`
- `python3 scripts/run.py insight-snapshot`
- `python3 scripts/run.py report-enrichment-coverage`
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
- `python3 scripts/run.py account-summary`
- `python3 scripts/run.py technician-summary`

### Search and export

- `python3 scripts/run.py search-ticket-docs <query>`
- `python3 scripts/run.py search-ticket-chunks <query>`
- `python3 scripts/run.py export-ticket-docs`
- `python3 scripts/run.py export-ticket-chunks`
- `python3 scripts/run.py export-embedding-chunks`
- `python3 scripts/run.py export-embedding-manifest`
- `python3 scripts/run.py build-vector-index`
- `python3 scripts/run.py report-vector-index-status`
- `python3 scripts/run.py search-vector-index <query>`
- `python3 scripts/run.py generate-public-snapshot`
- `python3 scripts/run.py generate-runtime-status`

## End-to-end install and onboarding on another OpenClaw instance

If another OpenClaw instance is told to install SherpaMind properly end-to-end, the job is not finished when the repo is cloned or copied into a skill directory. A complete install means the runtime is bootstrapped, configuration is written, the local dataset is seeded, derived artifacts are generated, and the service/runtime is verified.

Before running the flow, the installer should check and report host prerequisites:

- `python3` is present
- Python venv/pip bootstrap works on that host
- the host has network access for Python package installation
- `systemctl --user` is available if background service mode is expected

If any prerequisite is missing, the installer should stop and tell the user exactly what is missing and what needs to be corrected.

Recommended end-to-end sequence from the installed bundle root:

```bash
python3 scripts/bootstrap.py
python3 scripts/run.py setup
python3 scripts/run.py doctor
python3 scripts/run.py configure --api-key <token>
python3 scripts/run.py discover-orgs
python3 scripts/run.py configure --org-key <org> --instance-key <instance>
python3 scripts/run.py seed
python3 scripts/run.py generate-public-snapshot
python3 scripts/run.py generate-runtime-status
python3 scripts/run.py dataset-summary
python3 scripts/run.py insight-snapshot
python3 scripts/run.py report-vector-index-status
```

On a normal Linux host, `python3 scripts/run.py setup` is expected to:

- migrate/archive any legacy SherpaMind state if present
- initialize the SQLite database
- clean up any legacy SherpaMind OpenClaw cron jobs
- generate an initial public snapshot
- install and start the user-level `systemd` service

If the target host does not support usable `systemctl --user`, the install is still valid in fallback mode, but the operator/agent should say so plainly and use:

```bash
python3 scripts/run.py service-run-once
```

or:

```bash
python3 scripts/run.py service-run
```

instead of claiming the background service was installed.

## Bootstrap and local configuration

Bootstrapping the local runtime:

```bash
python3 scripts/bootstrap.py
```

This creates the main skill-local layout:

- `.SherpaMind/private/runtime/venv`
- `.SherpaMind/private/config.env`
- `.SherpaMind/public/exports`
- `.SherpaMind/public/docs`

Useful first-run sequence:

```bash
python3 scripts/bootstrap.py
python3 scripts/run.py setup
python3 scripts/run.py configure --api-key <token>
python3 scripts/run.py discover-orgs
python3 scripts/run.py configure --org-key <org> --instance-key <instance>
python3 scripts/run.py seed
python3 scripts/run.py install-service
python3 scripts/run.py generate-public-snapshot
python3 scripts/run.py generate-runtime-status
```

Environment variables are documented in `.env.example`.

Important controls include:

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

## Observability and public artifacts

SherpaMind generates public/runtime artifacts for fast inspection.

Common outputs include:

- insight snapshot
- stale open tickets
- recent account activity
- recent technician load
- runtime status
- account summaries
- technician summaries

SherpaMind also tracks real API usage in SQLite and reports:

- requests in the last hour
- error count in the last hour
- remaining hourly budget
- budget utilization ratio
- most-hit API paths

Vector readiness reporting includes:

- indexed chunk count
- total chunk rows
- ready ratio
- embedding dimension consistency
- missing index rows
- dangling index rows
- outdated content rows

## Current limitations and intentionally deferred areas

SherpaMind is functional and live, but the current surface is still intentionally bounded.

Current limits include:

- full-corpus detail enrichment is not in place; enrichment is selective and bounded
- open-ticket detail coverage is strong, but whole-corpus detail coverage is still shallow
- attachment bodies are not downloaded by default
- native outbound watcher alert routing is not implemented
- broader comment/history/detail capture depends on what SherpaDesk actually exposes cleanly and consistently

One current live-state nuance matters:

- the ticket table can advance ahead of the materialized document layer between sync and rematerialization cycles, so brief short-lived gaps between `tickets` and `ticket_documents` counts are expected until the next materialization pass

## Update/rebootstrap behavior

On update or re-bootstrap:

- preserve `.SherpaMind/private`
- preserve `.SherpaMind/public`
- rerun bootstrap safely to refresh the runtime venv if needed
- rerun `doctor`
- migrate/archive legacy state if needed
- reinstall or restart the user service idempotently
- regenerate public/runtime artifacts if needed
- remove old SherpaMind cron jobs if any legacy managed jobs remain

## Repository docs

Additional project docs live under `references/`.

Important ones:

- `references/architecture-doctrine.md`
- `references/architecture.md`
- `references/automation.md`
- `references/openclaw-query-model.md`
- `references/retrieval-architecture.md`
- `references/delta-sync-strategy.md`
- `references/operator-notes.md`
- `references/api-reference.md`
- `references/testing-strategy.md`
- `references/development-roadmap.md`

## Development

Install dependencies and run tests:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
pip install -e .[dev]
pytest -q
```

Project metadata lives in `pyproject.toml`.

## Practical summary

SherpaMind already operates as a live local SherpaDesk backend with:

- authenticated live ingest
- persistent SQLite storage
- sync-state tracking
- bounded detail enrichment
- ticket-log and attachment-metadata capture
- public Markdown artifacts
- text search
- local vector search
- service-managed background runtime
- API-budget observability
- retrieval-readiness reporting

The remaining work is mostly about widening depth and polish, not proving the base architecture.