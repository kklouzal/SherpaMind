# SherpaDesk API Reference Notes

## Canonical external documentation

The authoritative SherpaDesk API documentation currently lives here:

- <https://github.com/sherpadesk/api/wiki>

This URL should be treated as a first-class dependency of this project and kept visible in multiple project surfaces because the API is awkward enough that future work should never rely on hazy memory.

## Working assumptions

SherpaDesk API work should assume:
- inconsistent or under-documented endpoint behavior may exist
- auth/header requirements must be verified empirically
- pagination behavior may vary or be weakly documented
- delta-sync semantics may need endpoint-specific handling
- rate limiting and server-side fragility are real risks

## Project rule

Do not hard-code assumptions about SherpaDesk API behavior without either:
1. a direct citation to the SherpaDesk wiki, or
2. a clearly documented live verification note in this repository.

## Required discipline

Any API integration change should record:
- endpoint used
- auth/header pattern used
- pagination behavior observed
- timestamp / delta fields used
- retry / backoff behavior expected
- any rate-limit or burst-safety assumptions
