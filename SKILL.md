---
name: sherpamind
description: Read-only SherpaDesk ingest, sync, enrichment, and analysis skill for OpenClaw. Use when you need to seed or sync SherpaDesk data into a local SQLite knowledge base, maintain hot/warm/cold ticket freshness, enrich tickets with logs and attachment metadata, materialize OpenClaw-facing ticket documents/chunks, or answer natural-language questions about SherpaDesk ticket history, account trends, technician workload, stale open tickets, and similar support operations questions.
---

# SherpaMind

Use the bundled bootstrap/runtime scripts instead of assuming global Python packages.

## Runtime model

- Keep all skill-generated state under `workspace/.SherpaMind/`
- Treat `.SherpaMind/private/` as non-indexed/internal data
- Treat `.SherpaMind/public/` as OpenClaw-friendly derived outputs
- Do not store attachment blobs by default; metadata only unless explicitly extending for targeted screenshot/image analysis

## Layout contract

- `.SherpaMind/private/sherpamind.sqlite3` — canonical SQLite store
- `.SherpaMind/private/watch_state.json` — watcher/open-ticket state
- `.SherpaMind/private/runtime/venv/` — skill-local Python environment
- `.SherpaMind/private/config.env` — local config/env file if used
- `.SherpaMind/public/exports/` — exported JSONL and similar outputs
- `.SherpaMind/public/docs/` — future Markdown/public NL artifacts if needed

## First-use bootstrap

Run:

```bash
python3 scripts/bootstrap.py
```

This creates a skill-local venv under `.SherpaMind/private/runtime/venv`, installs pinned dependencies from `requirements.txt`, and installs the bundled code into that venv.

## Stable runtime entrypoint

Run commands through:

```bash
python3 scripts/run.py <command> [args...]
```

Examples:

```bash
python3 scripts/run.py workspace-layout
python3 scripts/run.py doctor
python3 scripts/run.py migrate-legacy-state
python3 scripts/run.py discover-orgs
python3 scripts/run.py seed
python3 scripts/run.py watch
python3 scripts/run.py enrich-priority-ticket-details --limit 25
python3 scripts/run.py insight-snapshot
python3 scripts/run.py search-ticket-docs printer --limit 5
python3 scripts/run.py generate-public-snapshot
```

## Onboarding flow

Typical first-run sequence:
1. `python3 scripts/bootstrap.py`
2. `python3 scripts/run.py doctor`
3. `python3 scripts/run.py migrate-legacy-state` (if upgrading from an older repo-local install)
4. `python3 scripts/run.py configure --api-key <token>`
5. `python3 scripts/run.py discover-orgs`
6. `python3 scripts/run.py configure --org-key <org> --instance-key <instance>`
7. `python3 scripts/run.py seed`

## Important operating rules

- Keep SherpaDesk access read-only unless the skill is explicitly expanded later
- Keep SQLite as canonical truth
- Treat `ticket_documents` / `ticket_document_chunks` as replaceable derived caches
- Regenerate and replace derived docs/chunks for changed tickets so stale NL artifacts do not linger
- Keep attachment handling metadata-only by default

## References

Read these when needed:
- `docs/openclaw-query-model.md` — OpenClaw-facing query/retrieval design
- `docs/delta-sync-strategy.md` — hot/warm/cold sync lane design
- `docs/api-reference.md` — verified API behavior and caveats
