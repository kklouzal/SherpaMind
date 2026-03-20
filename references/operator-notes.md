# Operator Notes

## External dependency warning

SherpaDesk is the highest-friction external surface in this project.

Before changing API logic, consult:
- <https://github.com/sherpadesk/api/wiki>
- `references/api-reference.md`
- recent local verification notes once they exist

## Local environment

Expected configuration comes from:
- `.SherpaMind/private/config.env` for skill-local persistent config
- process environment overrides when needed
- `.env.example` as the public reference template

Important conservative controls:
- `SHERPAMIND_WORKSPACE_ROOT`
- `SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS`
- `SHERPAMIND_REQUEST_TIMEOUT_SECONDS`

## Suggested operator workflow

1. bootstrap the skill-local runtime
2. run `doctor`
3. migrate legacy state if upgrading from an older repo-local install
4. verify credentials/config
5. initialize local DB / seed carefully
6. test one endpoint carefully when expanding behavior
7. document observed behavior
8. add broader sync/enrichment only after assumptions are proven

## Why this order matters

If the project jumps straight to watcher/alerts before the data model and sync assumptions are solid, the entire local knowledge layer becomes untrustworthy.

## Public-repo anonymization rule

This repository is meant to remain safe for public sharing.

When adding or updating operator notes, examples, live verification notes, or troubleshooting guidance:
- keep identities anonymized
- use placeholders for accounts, technicians, users, and organizations unless identity is strictly required
- do not copy raw customer-facing data into tracked docs when a generalized description is enough
- keep secrets, tokens, config values, and local runtime artifacts out of tracked files

## Practical safety rule

If SherpaDesk starts behaving inconsistently, slow down instead of pushing harder:
- smaller requests
- more spacing between calls
- more local documentation of what was observed
- fewer assumptions carried forward unverified
