# SherpaMind

> Canonical SherpaDesk API docs: <https://github.com/sherpadesk/api/wiki>

SherpaMind is a local-first SherpaDesk ingest, sync, enrichment, retrieval-preparation, and analysis system built for OpenClaw.

It keeps canonical SherpaDesk data in SQLite, derives rebuildable retrieval artifacts from that data, runs background work through split local Python workers, and exposes a CLI for sync, observability, analysis, and search.

## Transparency summary

SherpaMind is a real local backend, not just a passive instruction skill.

For live use it can:
- authenticate to the SherpaDesk API
- create workspace-local runtime state under `.SherpaMind/`
- create and maintain a local SQLite database plus generated public artifacts
- create staged runtime dirs under `.SherpaMind/private/config/`, `.SherpaMind/private/secrets/`, `.SherpaMind/private/data/`, `.SherpaMind/private/state/`, `.SherpaMind/private/logs/`, `.SherpaMind/private/runtime/`, and `.SherpaMind/public/`
- create a Python runtime venv under `.SherpaMind/private/runtime/venv`
- install Python packages from PyPI during bootstrap
- require the SherpaDesk API key to be configured through the OpenClaw `sherpamind` skill so runtime receives `SHERPADESK_API_KEY`
- optionally store a SherpaDesk API user hint in `.SherpaMind/private/secrets/sherpadesk_api_user.txt`
- store non-secret connection/runtime settings in `.SherpaMind/private/config/settings.env`
- optionally install and run a **user-level** `systemd` background service for ongoing sync/enrichment

Primary live credentials/config required:
- OpenClaw-managed `SHERPADESK_API_KEY` configured for the `sherpamind` skill
- org/instance settings: `.SherpaMind/private/config/settings.env`

Persistent behavior is intentionally workspace-local and user-scoped; SherpaMind does not require system-wide privilege for its normal service model.

## Project stance

SherpaMind follows a strict split:

- **Backend prepares the data**
- **Skill-front teaches access**
- **OpenClaw reasons at query time**

That split matters:

- SherpaMind owns ingest, sync, cleanup, normalization, metadata extraction, enrichment, chunking, indexing, public artifact generation, and background runtime behavior.
- The skill-front owns activation/query guidance for OpenClaw.
- OpenClaw owns interpretation, synthesis, comparative analysis, and user-facing reasoning.

SherpaMind is not meant to hardcode brittle conclusions into the backend. It is meant to make SherpaDesk data easy to trust, inspect, search, and reason over.

In practice, that means SherpaMind should make questions like these possible without pre-baking the answers:
- what kinds of recurring issues does an account show?
- how do two technicians differ in ticket-writing or support style?
- what habits show up in client/technician back-and-forth?
- what routine service work should probably become scheduled/proactive?

Those are OpenClaw-time reasoning tasks over prepared support history, not backend-side canned conclusions.

## Public-repo anonymization rule

This repository is intended to be safe for public distribution.

Treat anonymization as a standing development rule:

- keep secrets, tokens, and local runtime state out of the repo
- keep customer/account/user/technician examples anonymized in docs, tests, fixtures, and comments
- prefer placeholders like `<account>`, `<technician>`, `Acme`, `User One`, and `Tech One` over real names
- keep any live validation notes or API observations generalized unless specific identities are strictly necessary
- if a change adds new documentation, examples, fixtures, or reference material, scrub it before commit rather than cleaning it up later

Assume future autonomous development should preserve public-safe, anonymous examples by default.

## GitHub issue reporting rule

If a running OpenClaw instance encounters a runtime problem, installation problem, documentation gap, bug, unexpected API behavior, or meaningful feature request while using SherpaMind, it should report that back to the project repository:

- issue tracker: <https://github.com/kklouzal/SherpaMind/issues>
- if a matching issue already exists, add supporting detail instead of opening a duplicate
- if no matching issue exists, open a new issue with clear reproduction/context
- keep issue content anonymized and public-safe

This rule is intended to help autonomous use feed actionable maintenance back into the public project.

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
   - enforces single-flight ingest leases per sync lane so overlapping manual/service runs do not double-run the same hot/warm/cold ingest mode and then “clean up” the duplicate afterward
   - tracks whether the historical cold corpus has completed at least one full pass
   - uses spare hourly API headroom opportunistically to accelerate cold audit/detail deepening before that first full pass completes
   - slows cold re-audit/re-enrichment back down after the first full pass so most budget remains available for hot/warm freshness while still preserving long-tail maintenance
   - backfills account, user, and technician stub rows from stable ticket payload labels when standalone entity endpoints are thinner than real ticket history
   - enriches a bounded priority ticket set through single-ticket detail fetches
   - tracks per-ticket detail-fetch failures with retry/backoff state so non-retriable or cooling-down tickets do not get hammered every maintenance wave
   - biases cold-detail enrichment toward under-covered categories/accounts/technicians so historical retrieval depth broadens instead of clustering only around the newest cold tickets
   - stores ticket logs and attachment metadata from detail responses

3. **Retrieval preparation**
   - materializes ticket documents from canonical rows
   - normalizes ticket text into cleaner retrieval-ready summaries
- keeps action-oriented ticket cues honest by separating explicit follow-up notes from waiting-log fallback notes while still publishing a unified action-cue field for retrieval consumers
   - normalizes account/user/technician labels so retrieval/vector facets prefer human-readable names over raw numeric IDs when ticket payloads provide them
   - normalizes taxonomy/workflow labels (priority, category, class/submission/resolution labels, department-style fallbacks) to collapse multiline/HTML/whitespace noise before those labels feed filters, artifacts, and retrieval metadata
   - promotes ticket-observed account/user/technician labels into canonical stub rows when that is the cleanest stable source available, so entity-facing summaries/filter facets stay readable even when endpoint coverage is thin
   - carries workflow/state metadata such as subject, user email plus normalized user/creator/technician email domains, stable ticket identifiers (ticket number/key), technician and creator contact context, participant email-domain rollups, waiting/age timing signals, recent log types, log-interaction counts/date summaries (public/internal/waiting/response/resolution), distinct public/internal participant counts, latest/recent participant labels, mixed-visibility activity flags, derived waiting/follow-up cues, derived latest-response and closure cues from ticket logs, next-step hints, derived action-cue text/source (from explicit next steps, follow-up notes, request-completion notes, or waiting-log fallback), class/submission/resolution taxonomy, human-readable department labels plus label-source provenance, contract/confirmation context, account-location and department context, intake-channel/handling flags, request-completion cues, project/scheduled-ticket linkage, effort-tracking signals (estimated/remaining/total hours, total minutes, labor cost, percent complete), attachment presence plus normalized attachment name/name-token/extension/kind/size summaries (including size-known counts and broader kind-family counts), and resolution highlights into derived artifacts
   - chunks long documents deterministically, with small-tail rebalancing so vector exports do not devolve into metadata-only shard rows
   - supports keyword/text search over docs and chunks
   - exports metadata-rich embedding-ready chunk payloads, including chunk-order/position metadata for vector sidecars
   - builds and queries a local vector index

4. **Operator and OpenClaw observability**
   - reports dataset counts and sync freshness health, including per-lane staleness/missing/error state
   - reports enrichment coverage and retrieval coverage
   - reports tracked detail-fetch failure state (temporary vs permanent, cooldown pressure, and common HTTP status classes) so enrichment gaps can be distinguished from unreachable-ticket/API problems
   - surfaces detail-gap pressure across under-covered accounts, categories, and technicians so enrichment breadth can be steered deliberately instead of guessed
   - reports retrieval-metadata readiness across the materialized document layer
   - reports source-vs-materialized coverage for source-backed metadata so thin fields can be distinguished as upstream absence vs backend promotion drift, with transformed-field hygiene that treats malformed upstream email strings as source-quality issues instead of false promotion gaps
   - reports action-cue provenance so operators can see whether ticket guidance came from literal next-step text, explicit follow-up notes, request-completion notes, or waiting-log fallback
   - reports API usage and hourly budget pressure
   - classifies repeated API failure signatures from captured HTTP error detail/body previews so auth/config breakage is obvious instead of hiding inside undifferentiated 4xx counts
   - reports vector index readiness and drift
   - generates public Markdown artifacts for lightweight inspection

5. **Local backend runtime**
   - runs as split user-level systemd workers (`hot-watch`, `alert-dispatch`, `maintenance`)
   - keeps latency-sensitive alert detection separate from heavier maintenance work
   - uses a durable SQLite-backed alert queue for webhook dispatch retries/dedupe
   - performs periodic sync/enrichment/artifact-refresh work without burning OpenClaw tokens
   - keeps worker state, logs, queue state, request-usage history, and ingest-mode single-flight leases locally

## Proven capability

SherpaMind is not just a paper design. It has been exercised against a real read-only SherpaDesk account and observed performing the core behaviors this repository describes.

Observed capability evidence includes:

- successful live authentication against the SherpaDesk API
- successful local persistence into the canonical SQLite store at `.SherpaMind/private/data/sherpamind.sqlite3`
- successful live ingestion of a non-trivial dataset including **43 accounts**, **495 users**, **2 technicians**, and **12,041 tickets** during observed runs
- successful bounded detail enrichment producing **114 ticket details**, **752 ticket logs**, and **97 attachment metadata rows** during observed runs
- successful retrieval materialization producing **12,040 ticket documents** and **12,081 ticket document chunks** during observed runs
- successful local vector indexing with **12,081 indexed chunks** and an observed ready state of **0 missing**, **0 dangling**, and **0 outdated** index rows at the latest validation point
- successful request-budget tracking showing observed runtime behavior well below the documented `600 requests/hour` ceiling during normal service activity
- successful generated public artifact output under `.SherpaMind/public/docs/`
- successful local automated validation with the current test suite passing (`60 passed` in the latest run)
- successful split-worker runtime operation on a Linux host using the documented user-level `systemd` model

These figures are evidence from real observed runs, not guaranteed fixed installation outcomes. Exact counts will vary by target SherpaDesk account, sync timing, and local runtime state, but the project has been proven in live use to perform the ingest, sync, enrichment, retrieval-preparation, artifact-generation, and validation behaviors described here.

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

SherpaMind uses a workspace-local split storage model:

- `.SherpaMind/private/config/`
  - staged non-secret connection/runtime settings
- `.SherpaMind/private/secrets/`
  - optional staged SherpaDesk API user hint and other non-key local secret-side artifacts
- `.SherpaMind/private/data/`
  - canonical SQLite database
- `.SherpaMind/private/state/`
  - hot/warm watch state, per-worker state, aggregate service state, queue/lease-adjacent runtime state
- `.SherpaMind/private/logs/`
  - per-worker local logs plus aggregate compatibility log surfaces
- `.SherpaMind/private/runtime/`
  - runtime venv and other purely local execution artifacts
- `.SherpaMind/public/`
  - derived Markdown artifacts for OpenClaw/human inspection
  - JSONL exports for retrieval/indexing workflows

Canonical truth stays in SQLite.
Derived artifacts are rebuildable caches, not source-of-truth memory.

### Runtime architecture

SherpaMind no longer runs as one serialized background loop.

The live runtime is split into three worker classes:

1. **Hot watch worker**
   - polls hot open tickets on the fast lane
   - performs warm-watch-style change detection on the recent closed slice
   - detects new tickets and requester-side non-tech updates
   - keeps touched hot/warm ticket documents and chunks close to canonical freshness instead of waiting for a later full maintenance sweep
   - writes durable alert events into the local queue instead of dispatching webhooks inline

2. **Alert dispatch worker**
   - leases queued alert events from SQLite
   - refreshes per-ticket detail context just-in-time
   - builds the OpenClaw webhook payload
   - posts the synopsis request to OpenClaw
   - marks events sent / failed / retrying with dedupe-safe queue semantics

3. **Maintenance worker**
   - owns warm closed reconciliation, cold audit, enrichment, retrieval artifact refresh, public snapshot generation, vector refresh, and runtime-status generation
   - keeps heavy maintenance work isolated so it cannot block the hot alert detection lane

This split exists specifically to avoid the old failure mode where latency-sensitive ticket alerts waited behind cold audit, enrichment, or corpus-wide artifact work.

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
- `retrieval-readiness.md`
- `stale-open-tickets.md`
- `recent-account-activity.md`
- `recent-technician-load.md`
- `runtime/status.md`
- per-account docs under `accounts/`, including entity-level retrieval health, vector/materialization/hash readiness, explicit retrieval-gap ticket tables, lag buckets, and retrieval-metadata coverage
- per-technician docs under `technicians/`, including entity-level retrieval health, vector/materialization/hash readiness, explicit retrieval-gap ticket tables, lag buckets, and retrieval-metadata coverage
- per-ticket docs under `tickets/` for open and artifact-bearing/enriched tickets, including ticket-level retrieval health, chunk inventory, and vector-readiness cues

Snapshot generation also prunes stale per-account/per-technician/per-ticket Markdown files that no longer match the current dataset-derived entity set, so old fallback-label artifacts do not linger beside newer readable-name docs.

### Runtime model

SherpaMind’s normal background behavior comes from a local Python backend service, not OpenClaw cron.

The service owns:

- hot open-ticket watcher/sync work
- warm closed-ticket reconciliation
- cold rolling audit work
- bounded priority enrichment
- retrieval-artifact self-healing when materialized docs fall behind the current backend materializer version
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
- retry only transient transport/server-side failures; do not waste repeated retries on non-retriable 4xx responses
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
- `python3 scripts/run.py backfill-technician-stubs`
- `python3 scripts/run.py backfill-ticket-entity-stubs`
- `python3 scripts/run.py backfill-ticket-core-fields`
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
- `python3 scripts/run.py hot-watch-run`
- `python3 scripts/run.py hot-watch-run-once`
- `python3 scripts/run.py alert-dispatch-run`
- `python3 scripts/run.py alert-dispatch-run-once`
- `python3 scripts/run.py maintenance-run`
- `python3 scripts/run.py maintenance-run-once`
- `python3 scripts/run.py service-run` *(legacy compatibility alias to maintenance loop; not the preferred unattended entrypoint)*
- `python3 scripts/run.py service-run-once` *(legacy compatibility alias to maintenance one-shot)*

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
- `python3 scripts/run.py ticket-summary`

### Search and export

- `python3 scripts/run.py search-ticket-docs <query>`
- `python3 scripts/run.py search-ticket-docs <query> --account <account> --status Open --department <department>`
- `python3 scripts/run.py search-ticket-chunks <query>`
- `python3 scripts/run.py search-ticket-chunks <query> --priority High --category <category> --class-name <class>`
- `python3 scripts/run.py export-ticket-docs`
- `python3 scripts/run.py export-ticket-chunks`
- `python3 scripts/run.py export-embedding-chunks`
- `python3 scripts/run.py export-embedding-manifest`
- `python3 scripts/run.py build-vector-index`
- `python3 scripts/run.py report-vector-index-status`
- `python3 scripts/run.py report-retrieval-readiness`
- `python3 scripts/run.py search-vector-index <query>`
- `python3 scripts/run.py search-vector-index <query> --department <department> --class-name <class> --submission-category <channel> --resolution-category <resolution>`
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
python3 scripts/run.py bootstrap-audit
python3 scripts/bootstrap.py
python3 scripts/run.py setup
python3 scripts/run.py doctor
python3 scripts/run.py discover-orgs
python3 scripts/run.py configure --org-key <org> --instance-key <instance>
python3 scripts/run.py seed
python3 scripts/run.py generate-public-snapshot
python3 scripts/run.py generate-runtime-status
python3 scripts/run.py dataset-summary
python3 scripts/run.py insight-snapshot
python3 scripts/run.py report-vector-index-status
python3 scripts/run.py report-retrieval-readiness
python3 scripts/run.py install-service
python3 scripts/run.py service-status
```

`install-service` now installs and enables three user-level worker units together:
- `sherpamind-hot-watch.service`
- `sherpamind-alert-dispatch.service`
- `sherpamind-maintenance.service`

`service-status` reports those workers as a set so unattended runtime health is evaluated against the split runtime rather than an old single-daemon mental model.

On a normal Linux host, `python3 scripts/run.py setup` is expected to:

- migrate/archive any legacy SherpaMind state if present
- initialize the SQLite database
- clean up any legacy SherpaMind OpenClaw cron jobs
- generate an initial public snapshot

Treat user-level `systemd` installation as a later, explicit operator decision after bootstrap, OpenClaw skill API-key configuration, discovery, and seed validation are complete.

If the target host does not support usable `systemctl --user`, the install is still valid in fallback mode, but the operator/agent should say so plainly and use worker-specific foreground/one-shot commands such as:

```bash
python3 scripts/run.py hot-watch-run-once
python3 scripts/run.py alert-dispatch-run-once
python3 scripts/run.py maintenance-run-once
```

or the corresponding foreground worker loops:

```bash
python3 scripts/run.py hot-watch-run
python3 scripts/run.py alert-dispatch-run
python3 scripts/run.py maintenance-run
```

The legacy `service-run` / `service-run-once` commands remain compatibility aliases to the maintenance worker only and should not be presented as the preferred split-runtime fallback story.

## Bootstrap and local configuration

Bootstrapping the local runtime:

```bash
python3 scripts/run.py bootstrap-audit
python3 scripts/bootstrap.py
```

When SherpaMind is installed under an OpenClaw `skills/` directory, the default runtime root auto-resolves to the parent workspace so `.SherpaMind/` lives at the workspace level instead of inside the repo checkout.

This creates the main workspace-local layout:

- `.SherpaMind/private/config/settings.env`
- `.SherpaMind/private/secrets/sherpadesk_api_user.txt`
- `.SherpaMind/private/data/sherpamind.sqlite3`
- `.SherpaMind/private/state/watch_state.json`
- `.SherpaMind/private/state/warm_watch_state.json`
- `.SherpaMind/private/state/hot-watch-state.json`
- `.SherpaMind/private/state/alert-dispatch-state.json`
- `.SherpaMind/private/state/maintenance-state.json`
- `.SherpaMind/private/state/service-state.json`
- `.SherpaMind/private/logs/service.log` *(aggregate/compatibility surface)*
- `.SherpaMind/private/logs/hot-watch.log`
- `.SherpaMind/private/logs/alert-dispatch.log`
- `.SherpaMind/private/logs/maintenance.log`
- `.SherpaMind/private/runtime/venv`
- `.SherpaMind/public/exports`
- `.SherpaMind/public/docs`

Useful first-run sequence:

```bash
python3 scripts/run.py bootstrap-audit
python3 scripts/bootstrap.py
python3 scripts/run.py setup
python3 scripts/run.py discover-orgs
python3 scripts/run.py configure --org-key <org> --instance-key <instance>
python3 scripts/run.py seed
python3 scripts/run.py generate-public-snapshot
python3 scripts/run.py generate-runtime-status
python3 scripts/run.py install-service
python3 scripts/run.py service-status
```

Runtime control/environment overrides are documented in `.env.example`, but the normal live API-key path is OpenClaw skill configuration feeding `SHERPADESK_API_KEY`, while `.SherpaMind/` continues to hold non-secret runtime settings and local derived state.

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
- detail-gap pressure tables for under-covered accounts/categories/technicians inside the public insight snapshot
- stale open tickets
- recent account activity
- recent technician load
- runtime status
- account summaries
- technician summaries

SherpaMind also tracks real API usage in SQLite and reports:

- requests in the last hour
- split success vs HTTP-error vs transport-error counts
- error ratio in the last hour
- remaining hourly budget
- budget utilization ratio
- most-hit API paths
- HTTP status-code breakdowns and top failing paths so repeated 404/401/429-style pressure is visible immediately

Vector and retrieval readiness reporting includes:

- indexed chunk count
- total chunk rows
- ready ratio
- embedding dimension consistency
- missing index rows
- dangling index rows
- outdated content rows
- bounded drift samples showing which chunks/documents are currently missing, outdated, or dangling so operators can inspect concrete vector-index repair targets instead of only totals
- chunk-size quality metrics (avg/min/max, tiny, over-target)
- filter-facet inventories for accounts, technicians, statuses, priorities, categories, normalized user/creator/technician/participant email domains, normalized attachment extensions, and attachment kinds
- chunk-level and document-level metadata coverage for cleaned subject/issue summary/next-step/action-cue/log-type/resolution/attachment readiness plus log-interaction counts/date summaries, participant-count/name visibility metadata, attachment name/name-token/extension/kind/size summaries, size-known coverage, broader attachment kind-family counts, explicit-followup-vs-waiting-log cue splits, class/submission/resolution taxonomy, human-readable department labels, account-location, confirmation, and intake-channel metadata
- source-vs-materialized coverage for source-backed metadata fields such as support group, contract, location, department key, ticket identifiers, email-domain promotions where the upstream payload actually exposes the underlying email, timing flags, and confirmation fields, including whether low coverage reflects upstream absence or backend materialization drift
- when readiness is run with a row limit for fast inspection, source-backed coverage is scoped to the sampled ticket set so limited runs do not misreport full-corpus promotion gaps
- entity-label quality summaries for account/user/technician/department facets so operators can see readable-vs-identifier-like label ratios and fallback-source pressure before trusting filters heavily
- retrieval signal pressure summaries for accounts/categories/technicians so operators can see where detail/action/activity/resolution/attachment metadata is still too thin for confident retrieval and steer enrichment breadth deliberately
- chunk-topology readiness signals such as chunks-per-document and multi-chunk-document ratio so vector-sidecar consumers can reason about chunk fanout cleanly
- embedding-ready exports carry chunk-order/position fields (`chunk_start_char`, `chunk_end_char`, `previous_chunk_id`, `next_chunk_id`) plus inferred chunk semantic section cues (`chunk_primary_section`, `chunk_section_labels`, semantic-context booleans) so downstream vector sidecars can reconstruct both nearby context and chunk intent without guessing
- freshness windows for materialized chunks vs ticket update timestamps, including per-document lag counts, status splits, lag buckets, and top lagging-document samples
- raw readiness JSON now exposes both detailed machine-facing maps (`source_metadata_coverage`, `freshness`) and operator-oriented rollups (`source_backed_metadata`, `materialization_freshness_lag`) so consumers do not have to reconstruct those views themselves

## Current limitations and intentionally deferred areas

SherpaMind is functional and live, but the current surface is still intentionally bounded.

Current limits include:

- full-corpus detail enrichment is not in place; enrichment is selective and bounded
- open-ticket detail coverage is strong, but whole-corpus detail coverage is still shallow enough that some deeper comparative/history questions will surface "not enough detail yet" pressure
- attachment bodies are not downloaded by default
- native outbound watcher alert routing is not implemented
- broader comment/history/detail capture depends on what SherpaDesk actually exposes cleanly and consistently
- the service now supports an explicit cold-bootstrap-versus-steady-state posture, but the exact adaptive budget heuristics are still early and should be tuned against real long-running usage

One current live-state nuance matters:

- the ticket table can advance ahead of the materialized document layer between sync and rematerialization cycles, so brief short-lived gaps between `tickets` and `ticket_documents` counts are expected until the next materialization pass

## Update/rebootstrap behavior

On update or re-bootstrap:

- preserve `.SherpaMind/{config,secrets,data,state,logs,runtime}`
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
- `references/bootstrap-onboarding.md`
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