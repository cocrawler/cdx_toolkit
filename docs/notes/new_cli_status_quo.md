# Status Quo: `cdxt` CLI

**Date:** 2026-03-20
**Related:** [Issue #582](https://github.com/commoncrawl/issues/issues/582), [PR #54](https://github.com/commoncrawl/cdx_toolkit/pull/54), [new_cli_proposal.md](new_cli_proposal.md)

---

## Issue #582 — "new cdx_toolkit CLI"

The issue proposes splitting the current CLI into two primary operations:

1. **`index`** — Iterate/query captures and produce an index (replaces `iter`)
2. **`repackage`** — Take an index as input and create WARC files (replaces `warc`)

Key design decisions agreed upon in the discussion:

- `iter` becomes a deprecated alias for `index`
- `warc` is retired with an error message pointing to the new workflow
- `index` supports multiple input sources: CDX API, local CDX files, Athena, DuckDB
- `repackage` only accepts structured `index` output as input (**cdx**, **cdxj**, **csv**, or **jsonl** — no glob)
- Athena/DuckDB queries use `--sql <filename>` (per wumpus, March 14 comment)
- WARC output keeps `--prefix` and `--subprefix` (WARC standard names)
- Metadata attachment uses `--add-metadata-record <filename>` (generic, not path-specific)

## PR #54 — "filter_cdx and warc_by_cdx commands"

PR #54 implements two new commands as a stepping stone:

- **`filter_cdx`** — Filters CDX files (local or S3) by URL/SURT whitelist with glob support
- **`warc_by_cdx`** — Fetches WARC records from pre-filtered CDX files (supports S3, aioboto3)

New capabilities from PR #54 that the new CLI must incorporate:
- S3 read/write support via fsspec and optional aioboto3
- Glob patterns for multiple CDX input files
- URL and SURT whitelist filtering
- Metadata records embedded in WARC files
- Overwrite protection for output files

## Current CLI (main branch)

| Command | Description |
|---------|-------------|
| `cdxt iter <url>` | Iterate captures, print index lines (text/csv/jsonl) |
| `cdxt warc <url>` | Iterate captures, write WARC files |
| `cdxt size <url>` | Estimate result count |

Global options: `--cc`, `--ia`, `--source`, `--crawl`, `--limit`, `--from`, `--to`, `--filter`, `--closest`, `--get`, `--cc-mirror`, `--cc-sort`, `--wb`, `-v`
