---
name: sherpamind
description: "Use for SherpaDesk-related requests: ticket lookup, support-history retrieval, account/user/technician analysis, stale-ticket review, workload questions, operational reporting, and open-ended natural-language questions about SherpaDesk data. Trigger when the user mentions SherpaDesk or asks about tickets, support issues, clients/accounts, technicians, resolution history, recurring incidents, backlog, response timing, or similar support-operations analysis."
---

# SherpaMind

Use SherpaMind as the **OpenClaw query/action layer** over the local SherpaDesk dataset prepared by the backend service.

## Repo root and stable entrypoint

Work from the repo root:

```bash
cd {baseDir}
```

When the repo is installed under an OpenClaw `skills/` directory, SherpaMind automatically uses the parent workspace as `SHERPAMIND_WORKSPACE_ROOT`, so runtime state stays in workspace-level `.SherpaMind/` rather than inside the skill checkout.

Use the stable runner:

```bash
python3 scripts/run.py <command> [args...]
```

Do not invent alternate runtime paths.
Do not treat OpenClaw as the background scheduler for this backend.

## Choose the lightest path that answers the question

### Exact facts, counts, status, and workload

Start with structured commands:

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
- open-ticket count → `report-status-counts`
- technician backlog/load → `technician-summary "<technician>"`
- account snapshot → `account-summary "<account>"`

### Fuzzy investigation, prior-art lookup, and support-history recall

Use retrieval commands:

- `python3 scripts/run.py search-ticket-docs "<query>"`
- `python3 scripts/run.py search-ticket-chunks "<query>"`
- `python3 scripts/run.py search-ticket-chunks "<query>" --account "<account>"`
- `python3 scripts/run.py search-ticket-chunks "<query>" --status Open`
- `python3 scripts/run.py search-ticket-chunks "<query>" --technician "<technician>"`
- `python3 scripts/run.py search-vector-index "<query>"`
- `python3 scripts/run.py search-vector-index "<query>" --account "<account>" --status Open`
- `python3 scripts/run.py search-vector-index "<query>" --technician "<technician>" --priority High --category "<category>"`

Default retrieval workflow:
1. Start with keyword/text search when the issue words are concrete.
2. Widen to vector search when wording may vary or keyword recall looks thin.
3. Use account/technician/status/priority/category filters when they materially narrow the search.
4. Answer from retrieved evidence instead of jumping to canned conclusions.

### Quick factual context from generated artifacts

Read these when a concise derived artifact is enough:

- `{baseDir}/.SherpaMind/public/docs/index.md`
- `{baseDir}/.SherpaMind/public/docs/insight-snapshot.md`
- `{baseDir}/.SherpaMind/public/docs/stale-open-tickets.md`
- `{baseDir}/.SherpaMind/public/docs/recent-account-activity.md`
- `{baseDir}/.SherpaMind/public/docs/recent-technician-load.md`
- `{baseDir}/.SherpaMind/public/docs/runtime/status.md`
- `{baseDir}/.SherpaMind/public/docs/accounts/index.md`
- `{baseDir}/.SherpaMind/public/docs/technicians/index.md`
- `{baseDir}/.SherpaMind/public/docs/accounts/*.md`
- `{baseDir}/.SherpaMind/public/docs/technicians/*.md`

## Preferred answer flow

For broad questions like “what’s been going on with account X lately?” or “have we seen this before?”:

1. Pull one structural summary first.
2. Pull retrieval evidence second.
3. Use generated public docs only when they add concise context.
4. Give the user an answer grounded in the retrieved evidence.

Prefer factual retrieval over hand-authored interpretation.

## End-to-end install and onboarding on another OpenClaw instance

If the user asks to install SherpaMind properly end-to-end into an OpenClaw instance, first check the host prerequisites and report any missing pieces plainly before continuing.

Minimum prerequisites to check:
- `python3` is present
- Python venv/pip bootstrap works on that host
- the host has network access for Python package installation
- `systemctl --user` is available if background service mode is expected

If any prerequisite is missing, stop and tell the user exactly what is missing and what needs to be fixed.

Then use this flow from the installed skill bundle root:

1. bootstrap the skill-local runtime
   - `python3 scripts/bootstrap.py`
2. run the setup flow
   - `python3 scripts/run.py setup`
3. verify runtime/service state
   - `python3 scripts/run.py doctor`
   - `python3 scripts/run.py service-status`
4. configure the SherpaDesk API token
   - `python3 scripts/run.py configure --api-key <token>`
5. discover organizations/instances
   - `python3 scripts/run.py discover-orgs`
6. write the chosen org/instance into config
   - `python3 scripts/run.py configure --org-key <org> --instance-key <instance>`
7. seed the local dataset
   - `python3 scripts/run.py seed`
8. generate/refine the derived artifacts if needed
   - `python3 scripts/run.py generate-public-snapshot`
   - `python3 scripts/run.py generate-runtime-status`
9. confirm the install is actually usable
   - `python3 scripts/run.py dataset-summary`
   - `python3 scripts/run.py insight-snapshot`
   - `python3 scripts/run.py report-vector-index-status`

Default expectation on Linux is that `setup` will also initialize the DB, clean up any old SherpaMind cron jobs, generate an initial public snapshot, and install/start the user-level `systemd` service. Do not stop at cloning or placing files; complete the runtime bootstrap, config, seed, and verification steps.

If service installation fails because the target host lacks usable `systemctl --user`, continue the bootstrap/config/seed flow anyway, report the service limitation clearly, and use `python3 scripts/run.py service-run-once` or `python3 scripts/run.py service-run` as the fallback operational mode instead of pretending the service installed.

If install/runtime/use issues or meaningful feature gaps are discovered while operating SherpaMind, check <https://github.com/kklouzal/SherpaMind/issues>. If a matching issue exists, add supporting detail; otherwise open a new issue with clear reproduction/context. Keep issue content anonymized and public-safe.

## Lifecycle and maintenance commands

Use these for setup/maintenance, not routine user queries:

- `python3 scripts/bootstrap.py`
- `python3 scripts/run.py workspace-layout`
- `python3 scripts/run.py doctor`
- `python3 scripts/run.py backfill-technician-stubs`
- `python3 scripts/run.py setup`
- `python3 scripts/run.py migrate-legacy-state`
- `python3 scripts/run.py archive-legacy-state`
- `python3 scripts/run.py cleanup-legacy-cron`
- `python3 scripts/run.py configure --api-key <token>`
- `python3 scripts/run.py discover-orgs`
- `python3 scripts/run.py install-service`
- `python3 scripts/run.py restart-service`
- `python3 scripts/run.py service-status`
- `python3 scripts/run.py generate-public-snapshot`
- `python3 scripts/run.py generate-runtime-status`

## Boundaries

- Treat SherpaMind as read-only unless the project explicitly grows write behavior later.
- Keep attachment handling metadata-only by default.
- Do not auto-download attachment bodies by default.
- Treat docs, chunks, vector rows, and public Markdown artifacts as replaceable derived caches.
- Let SherpaMind prepare and expose data; let OpenClaw interpret it at answer time.

## References

Read these only when needed. Keep the action layer in this file lean; use the reference files for deeper architecture, retrieval, automation, and API details.

- `{baseDir}/README.md` — current live project overview and command surface
- `{baseDir}/references/openclaw-query-model.md` — query/retrieval model
- `{baseDir}/references/architecture-doctrine.md` — backend vs skill-front boundary
- `{baseDir}/references/retrieval-architecture.md` — retrieval and vector design
- `{baseDir}/references/automation.md` — service/install/update model
- `{baseDir}/references/delta-sync-strategy.md` — hot/warm/cold sync behavior
- `{baseDir}/references/api-reference.md` — verified API/auth behavior
