# Ingest and Sync Plan

## Modes

### 1. Initial seed
Goal:
- populate the local store with the core SherpaDesk entities needed for analysis

Expected first targets:
- accounts / clients
- users / contacts
- technicians
- tickets
- ticket comments / notes / relevant history

### 2. Ongoing delta sync
Goal:
- keep the local store current with minimal API load

Expected approach:
- track per-entity sync watermark(s)
- prefer modified/updated timestamps where supported
- use idempotent upserts
- record ingest runs and sync state locally

## Principles

- correctness over cleverness
- low request volume over aggressive freshness
- explicit cursor/watermark state over inferred magic
- preserve source timestamps and raw JSON where useful
- make analytical queries easy even if the source API is painful
- shape the canonical data so downstream retrieval/indexing for OpenClaw is straightforward

## Rate-limit / fragility policy

SherpaDesk API usage should be conservative by default.

Recommended discipline:
- serialize initial implementation work until real endpoint behavior is confirmed
- avoid large request bursts
- prefer small page sizes when endpoint behavior is uncertain
- use retry with backoff for transient failures
- cache known dimensions locally
- perform delta syncs against the narrowest viable entity set

## Watcher principle

The watcher should focus on **new tickets only** for the first implementation.
Do not attempt to solve every notification use case at once.
