# Testing Strategy

## Layers

### Unit tests
- database schema/bootstrap
- path/layout resolution
- skill-local config loading/writing
- sync state helpers
- data transformation helpers
- query/report helpers
- public artifact generation

### Integration-style tests
- mocked API client behavior
- paging behavior
- retry/backoff logic
- seed and delta sync orchestration

### Live verification notes
Because SherpaDesk is awkward and externally controlled, some truths may only be learnable through live probing.
Those findings should be written down in this repository instead of living only in chat memory.

## Rule

Do not assume a green local unit test suite means the SherpaDesk API contract is understood correctly.
