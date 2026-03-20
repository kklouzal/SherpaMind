# Retrieval Architecture

> SherpaDesk API reference: <https://github.com/sherpadesk/api/wiki>

## Goal

Make SherpaDesk data genuinely useful to OpenClaw, not just stored.

## Design stance

SherpaMind should use a **hybrid retrieval model**:
- **SQLite** for canonical structured truth
- **FTS / keyword search** for exact-text retrieval
- **vector retrieval** for fuzzy similarity and theme discovery

## Why this matters

OpenClaw will need to answer different kinds of questions:

### Structured questions
Examples:
- how many tickets did account X open this month?
- how long do technician Y's tickets wait for first response?
- which accounts have the highest reopen rate?

Best source:
- SQL against canonical tables

### Exact-text investigative questions
Examples:
- find tickets mentioning a specific error string
- find all incidents involving a product/version
- find tickets for an exact customer/domain/user name

Best source:
- keyword/full-text search

### Fuzzy problem-solving questions
Examples:
- have we seen something like this before?
- what past tickets look semantically similar to this new issue?
- what historical context is most relevant to this ticket?

Best source:
- vector/semantic retrieval, ideally with SQL metadata filters layered on top

## Suggested pipeline

1. ingest canonical SherpaDesk data into SQLite
2. normalize linked entities and stable ticket/comment metadata
3. build retrieval documents from tickets, comments, accounts, users, and derived summaries
4. carry stable workflow/state metadata forward with those documents (subject cleanup, user/account/technician linkage, recent log types, next-step hints, attachment presence, resolution highlights)
5. chunk long text where needed
6. index documents into:
   - FTS/keyword search
   - vector embeddings/index
7. expose hybrid query commands for OpenClaw/tooling

Current practical implementation now includes:
- keyword/full-text style search over references/chunks
- metadata-rich embedding-ready exports with filter-friendly ticket context
- a lightweight local vector index/search layer for immediate similarity retrieval without external embedding APIs

## Recommended retrieval document types

- ticket summary documents
- ticket conversation/comment chunks
- account-level factual summary documents
- user-level support history summaries
- technician-level factual summary documents

Avoid prematurely synthesizing strongly interpretive "known-fix" or theme conclusions until the retrieval layer and source coverage are strong enough to support them well.

## Important rule

Do not let the retrieval layer become the only source of truth.
Derived retrieval artifacts should be rebuildable from canonical local data.
