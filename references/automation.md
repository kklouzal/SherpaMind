# Automation

## Runtime model

SherpaMind should run as a **long-lived local Python backend service**, not as a set of token-burning OpenClaw cron jobs.

Why:
- syncing/enrichment/public artifact refresh are backend chores, not conversational agent turns
- a service can wake on host startup and run internal timers without spending model tokens
- OpenClaw should query and consume SherpaMind outputs, not pretend to be the scheduler for a Python backend

## Service model

SherpaMind now uses **split user-level systemd workers** where available:
- `sherpamind-hot-watch.service`
- `sherpamind-alert-dispatch.service`
- `sherpamind-maintenance.service`
- all managed through `systemctl --user`

The worker entrypoints are:
- `-m sherpamind.cli hot-watch-run`
- `-m sherpamind.cli alert-dispatch-run`
- `-m sherpamind.cli maintenance-run`

Generated units include conservative resource governance by default:
- hot-watch: `CPUQuota=25%`, lower CPU/IO weight, `Nice=10`
- alert-dispatch: `CPUQuota=10%`, lower CPU/IO weight, `Nice=10`
- maintenance: `CPUQuota=50%`, lower CPU/IO weight, `Nice=10`
- all workers: `TasksMax=128`, `Restart=on-failure`, restart-rate limits, `TimeoutStopSec=45`, `UMask=0077`, `NoNewPrivileges=true`, and `PrivateTmp=true`

Service-only secrets are staged in `.SherpaMind/private/secrets/service.env` with mode `0600`; systemd units reference that file via `EnvironmentFile=` rather than embedding API keys directly. Operator-specific overrides may be added as normal user-systemd drop-ins under `~/.config/systemd/user/sherpamind-*.service.d/*.conf`.

## Internal periodic lanes

The backend runtime no longer treats all work as one serialized service pass.

Instead it owns three distinct worker lanes:
- **hot watch worker**
  - hot open polling
  - warm-watch-style recent closed polling
  - new-ticket / requester-update detection
  - near-live rematerialization of touched hot/warm ticket retrieval artifacts so canonical sync does not leave open-ticket docs stale until a later maintenance pass
  - durable alert-event enqueueing
- **alert dispatch worker**
  - alert queue leasing
  - ticket-detail refresh for queued events
  - OpenClaw webhook delivery
  - retry / dedupe / failure accounting
- **maintenance worker**
  - warm closed reconciliation
  - cold closed audit
  - priority enrichment
  - retrieval artifact refresh
  - public snapshot generation
  - vector refresh
  - runtime-status generation and maintenance cleanup

The priority enrichment loop should stay retrieval-oriented rather than purely recency-oriented:
- open tickets first
- recently closed tickets next
- then broaden historical detail coverage across under-covered categories/accounts/technicians so the retrieval corpus gets deeper breadth over time instead of repeatedly clustering around one narrow slice of recent history
- record per-ticket detail-fetch failures with retry/backoff state so non-retriable or currently cooling-down tickets do not get re-hit every maintenance wave, while still allowing retry if the ticket later changes upstream

These run from internal Python timers, not OpenClaw cron.

The service also tracks real SherpaDesk request usage in SQLite and should use that to reserve the forecast hot/warm budget first, then spend spare hourly headroom opportunistically on cold audit and enrichment work instead of throttling cold depth with only a static conservative cadence.
When last-hour API traffic is dominated by auth/config failure storms (for example repeated invalid-token 404s), the runtime should temporarily suppress remote sync/enrichment lanes instead of continuing to burn budget on doomed requests, while still running local observability/artifact refresh work so the failure stays visible.
Client retry behavior should stay selective: retry transient transport/server pressure, not persistent/non-retriable 4xx responses.
Old request-event rows are pruned automatically by retention policy so the request log remains bounded.

Cold-history work should run in two phases:
- **bootstrap mode** until the historical corpus has completed one real full cold pass and closed-ticket detail coverage catches up
- **steady-state mode** after that, where cold re-audit/re-enrichment continues more slowly for drift correction and enrichment evolution

That first full-pass completion should be durable state, not guesswork.

The service should also repair stale derived retrieval artifacts when the current document materializer version no longer matches what is stored in `ticket_documents`. That keeps metadata/chunking improvements from depending on a human remembering to force a rematerialization pass.

Individual ingest lanes should also be **single-flight**. If `sync_hot_open`, `sync_warm_closed`, or especially `sync_cold_closed_audit` is already running, a second caller should skip cleanly behind an active ingest lease instead of starting a duplicate run and merely abandoning the older `running` row afterward.

## Install vs update behavior

### First install
- run `bootstrap-audit` first
- bootstrap the skill-local runtime
- migrate legacy state if needed
- initialize the DB
- configure the OpenClaw `sherpamind` skill entry and stage connection settings
- validate discovery/seed behavior
- optionally generate initial public docs
- only then install/start the user service if unattended mode is actually wanted
- doctor the result
- clean up any legacy SherpaMind cron jobs

### Update / re-bootstrap
- rerun bootstrap safely
- preserve `.SherpaMind/{config,secrets,data,state,logs,runtime}` and `.SherpaMind/public`
- archive old repo-local `state/` leftovers once migrated
- reinstall/rewrite the systemd user unit idempotently
- restart the service safely
- doctor the runtime
- regenerate public artifacts if needed
- clean up any old SherpaMind cron jobs that no longer belong

## Commands

### Service management
- `python3 scripts/run.py install-service`
- `python3 scripts/run.py uninstall-service`
- `python3 scripts/run.py start-service`
- `python3 scripts/run.py stop-service`
- `python3 scripts/run.py restart-service`
- `python3 scripts/run.py service-status`
- `python3 scripts/run.py hot-watch-run` (foreground/debug)
- `python3 scripts/run.py hot-watch-run-once`
- `python3 scripts/run.py alert-dispatch-run` (foreground/debug)
- `python3 scripts/run.py alert-dispatch-run-once`
- `python3 scripts/run.py maintenance-run` (foreground/debug)
- `python3 scripts/run.py maintenance-run-once`
- `python3 scripts/run.py service-run` *(legacy compatibility alias to maintenance loop)*
- `python3 scripts/run.py service-run-once` *(legacy compatibility alias to maintenance one-shot)*

### Lifecycle helpers
- `python3 scripts/run.py setup`
- `python3 scripts/run.py doctor`
- `python3 scripts/run.py migrate-legacy-state`
- `python3 scripts/run.py cleanup-legacy-cron`

## Legacy cron cleanup

SherpaMind briefly used OpenClaw cron during refactor exploration. That is no longer the desired architecture.

`cleanup-legacy-cron` removes any old managed SherpaMind cron jobs so the system converges on the service-first model.

## Issue escalation

When install/runtime automation fails in a way that looks like a product gap, bug, or recurring operational problem, check:

- <https://github.com/kklouzal/SherpaMind/issues>

If a matching issue exists, add supporting detail.
If not, open a new issue with reproduction steps, observed behavior, expected behavior, and relevant host/runtime constraints.
Keep issue content anonymized and public-safe.

## Health checking

`doctor` should verify:
- runtime venv exists
- staged settings/secrets paths exist
- DB exists
- watch state exists
- systemd user service file exists
- service enabled/active state
- service log/state file presence
- legacy repo-local state presence (for upgrade hints)
- any leftover legacy SherpaMind cron jobs that should be cleaned up

When backend/runtime capabilities change, the skill-front/references should be reviewed in the same wave so the backend/skill/OpenClaw split remains coherent.

## Event-scoped class/sub-class classification

SherpaMind classification should stay event-scoped and token-conscious. It should not run broad periodic reclassification sweeps.

Current intended flow:

1. `watch_new_tickets` observes a genuinely new open ticket after watcher baseline exists and enqueues one `initial` classification event keyed as `classification:initial:<ticket_id>`.
2. `watch_warm_tickets` observes a newly-seen closed ticket after watcher baseline exists and enqueues one `final` classification event keyed as `classification:final:<ticket_id>:<closed_or_updated_time>`.
3. The maintenance worker runs `classification_dispatch` as a lightweight task, leasing only a tiny batch of pending classification events.
4. Each classification hook payload includes only:
   - compact event/ticket metadata
   - bounded initial/final ticket context
   - active leaf class/sub-class candidates from the local `ticket_taxonomy_classes` cache as `id: path`
   - an explicit instruction not to do broad retrieval or unrelated ticket searches
5. The OpenClaw classification agent records the result with `record-ticket-classification`, which validates the class id against cached taxonomy and stores confidence/rationale locally.

This keeps LLM work to at most two intended invocations per observed ticket: once for initial intake and once at final closure. Result capture remains local-first, and SherpaDesk write-back is a separate guarded production lane: only completed results at/above the configured confidence threshold are eligible; the class must be active, leaf/sub-class, and present in a fresh taxonomy cache; the remote ticket is read before write; same-class results are skipped; writes use minimal form data `class_id=<id>`; and the ticket is fetched/materialized after success to confirm the class changed.

Ticket class taxonomy is refreshed every 24 hours by maintenance, and classification record/write-back paths can force an immediate refresh when a class id looks invalid or stale.

## LLM accuracy-to-token policy

SherpaMind should spend cheap local computation before model tokens. Any system that invokes OpenClaw/LLM work should follow this shape:

- trigger only from real events or explicit user queries, never broad periodic model sweeps
- precompute/cache local evidence first, then pass only the bounded fields needed for the decision
- prefer compact JSON (`id:path` taxonomy candidates, selected ticket fields, clipped notes) over raw SherpaDesk payloads
- default alert prompts to provided compact evidence, allowing at most narrow SherpaMind retrieval when it would materially improve alert accuracy; keep classification prompts no-retrieval
- preserve accuracy by including high-signal context and locally retrieved evidence, not by shipping every available log/blob to the model

Current LLM-backed automatic paths are: new-ticket alert synthesis, requester-side update alert synthesis, and event-scoped ticket classification. Alert synthesis receives a tiny locally retrieved `similar_ticket_evidence` list so it can mention prior patterns cheaply; the alert agent may still perform one narrow SherpaMind lookup when that would materially improve accuracy. Classification stays strict: use the supplied event context and taxonomy only.
