# Operator Notes

## External dependency warning

SherpaDesk is the highest-friction external surface in this project.

Before changing API logic, consult:
- <https://github.com/sherpadesk/api/wiki>
- `docs/api-reference.md`
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

## Practical safety rule

If SherpaDesk starts behaving inconsistently, slow down instead of pushing harder:
- smaller requests
- more spacing between calls
- more local documentation of what was observed
- fewer assumptions carried forward unverified
