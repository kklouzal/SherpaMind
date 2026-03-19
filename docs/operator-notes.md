# Operator Notes

## External dependency warning

SherpaDesk is the highest-friction external surface in this project.

Before changing API logic, consult:
- <https://github.com/sherpadesk/api/wiki>
- `docs/api-reference.md`
- recent local verification notes once they exist

## Local environment

Expected configuration comes from environment variables documented in:
- `.env.example`

Important conservative controls:
- `SHERPAMIND_REQUEST_MIN_INTERVAL_SECONDS`
- `SHERPAMIND_REQUEST_TIMEOUT_SECONDS`

## Suggested operator workflow

1. verify credentials/config
2. initialize local DB
3. test one endpoint carefully
4. document observed behavior
5. implement seed incrementally
6. add delta sync only after seed assumptions are proven
7. add watcher only after seed/delta correctness is credible

## Why this order matters

If the project jumps straight to watcher/alerts before the data model and sync assumptions are solid, the entire local knowledge layer becomes untrustworthy.

## Practical safety rule

If SherpaDesk starts behaving inconsistently, slow down instead of pushing harder:
- smaller requests
- more spacing between calls
- more local documentation of what was observed
- fewer assumptions carried forward unverified
