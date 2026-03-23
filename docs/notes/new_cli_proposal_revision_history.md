# Revision History & Resolved Questions: New `cdxt` CLI Proposal

**Related:** [new_cli_proposal.md](new_cli_proposal.md), [review 1](new_cli_proposal_review.md), [review 2](new_cli_proposal_review_2.md), [review 3](new_cli_proposal_review_3.md)

---

## Resolved questions

### From v3

| # | Question | Resolution |
|---|----------|------------|
| 1 | `--all-fields` from current `iter` | **Removed.** Use `--output-columns '*'` instead. No convenience alias needed |
| 2 | `--match` / `--match-from` on `index query` | **No.** Each pattern would require a separate remote API request — a whitelist of thousands of entries would generate thousands of rate-limited calls. `--match` / `--match-from` remain `index filter`-only. `--filter` (server-side, e.g., `=status:200`) stays on `index query` since it applies to the single request |
| 3 | `--backend` auto-detection | **Yes.** Default is `auto`: selects `aioboto3` when installed and output is S3, otherwise `default`. Explicit `--backend aioboto3` or `--backend default` still available for override |
| 4 | `size` as flag vs command | **Both.** `--count-only` flag added to `index query`. `cdxt size` kept as a convenience alias for `cdxt index query --count-only` |
| 5 | `index filter` output modes with `--glob` | **`--output-dir` is the default** when `--glob` is used. Use `--output` explicitly to merge into a single file instead |

### From v6 review

| # | Question | Resolution |
|---|----------|------------|
| 6 | `--from` / `--to` semantics across subcommands | **Resolved.** `--from` / `--to` are only available on `index query` (CDX API time range parameters, server-side). Not available on `index filter` (filter by timestamp in match patterns or post-process) or `index sql` (use SQL `WHERE` clause instead) |
| 7 | `--limit` semantics across subcommands | **Resolved.** `--limit` is available on `index query` (caps API results, server-side) and `index filter` (caps output rows, client-side). Not available on `index sql` — use SQL `LIMIT` clause instead |
| 8 | `--output-columns` behavior for CDXJ and JSONL formats | **Resolved.** For CDX and CSV: selects named columns. For CDXJ: selects keys from the JSON block (the JSON object is treated as a single column; selected keys are included in the emitted JSON). For JSONL: selects top-level keys only (no nested key selection) |

---

## Revision history

### v8 (2026-03-23) — Review 3: onboarding and first-run clarity

Based on [review 3](new_cli_proposal_review_3.md).

| Issue | v7 | v8 | Reason |
|-------|----|----|--------|
| Raw CDX output unlabeled | Quick Start said "print to screen" | Explicitly says "print raw CDX records (machine-readable index format)" | New users need to know what they are seeing |
| `--match-type` defaults to `surt` | `surt` | `url` | New users are far more likely to have URLs than SURTs. Wrong default causes silent partial results — bad failure mode |
| `--glob` relationship to `<input_path>` unclear | Not explained | Documented as "relative to `<input_path>`"; added local glob example | Avoids confusion about absolute vs relative paths |
| `repackage` not translated for new users | "Build WARC files from index records" | "Download captured web pages referenced by an index and write them into new WARC files" | Domain jargon should be paired with plain-language description |
| No error/help behavior spec | Not documented | New section 2.6 with guided error examples for common mistakes | First-run UX depends on recoverability, not just the happy path |
| `--output-format` default not explained | Listed without rationale | Description explains CDX is the default because it is the native format and pipes into `repackage`; suggests `-f csv`/`-f jsonl` for human-readable output | Helps new users choose the right format |

### v7 (2026-03-23) — Review 2 fixes

Based on [review 2](new_cli_proposal_review_2.md).

| Issue | v6 | v7 | Reason |
|-------|----|----|--------|
| Pipe footgun | `repackage -` defaults to `cdxj`, mismatching `index query` default `cdx` | `repackage -` defaults to `cdx` | The basic pipe `index query ... \| repackage -` must work without extra flags |
| `--output-columns '*'` shell-hostile | `--all-fields` removed, replaced by `--output-columns '*'` | All fields emitted by default when `--output-columns` is omitted | Avoids shell glob expansion trap for new users |
| `--details` undocumented on `index query` | Only shown in `size` alias section | Added to `index query` option table, scoped to `--count-only` | Docs must be coherent between canonical command and alias |
| `--glob` → `--output-dir` too implicit | Default mentioned only in option description | Comment added to filter example explaining the default | Prevents surprise output-mode change for new users |
| `--backend auto` not observable | No mention of logging | All `--backend` descriptions note chosen backend is logged at `-v` | Users need to know which backend ran for debugging |

### v6 (2026-03-23) — Consistency review fixes and new options

| Issue | v5 | v6 | Reason |
|-------|----|----|--------|
| `--input-format` on stdin | Required when stdin | Defaults to `cdxj` (not required) | CDXJ is the most common piped format; reduces friction |
| `--download-prefix` | No default | Defaults to `https://data.commoncrawl.org` | Sensible default for the primary use case |
| `--compression` | Not available | `auto` (default), `gzip`, `none` | Auto-detect from `.gz` extension; avoids external compression step |
| `--output-dir` scope | Common to all `index` subcommands | Restricted to `index filter` | Only `index filter` with `--glob` produces multiple files |
| `--backend` scope | `repackage` only | `index filter` and `repackage` | Both read/write S3 and benefit from `aioboto3` |
| `--match` / `--match-from` | Optional on `index filter` | Required (at least one) | Filtering without criteria is undefined; use `--match '*'` for pass-through |
| File extension mapping | Not documented | Documented (`.cdx`, `.cdxj`, `.csv`, `.jsonl`, `.gz` variants) | Needed for auto-detection clarity |
| Examples with `.csv` | Several examples used `.csv` as default | Updated to `.cdx.gz` | Consistent with `cdx` as default format |
| Open questions | None remaining | 3 new questions (from v6 review) | `--from`/`--to` semantics, `--limit` semantics, `--output-columns` for CDXJ/JSONL |

### v5 (2026-03-23) — CDX and CDXJ as distinct formats

| Issue | v4 | v5 | Reason |
|-------|----|----|--------|
| Output/input formats | `cdx`, `csv`, `jsonl` | `cdx`, `cdxj`, `csv`, `jsonl` | CDX (space-delimited with header) and CDXJ (SURT + timestamp + JSON per line) are distinct formats per IIPC and Webrecorder specs. Both must be first-class options |

### v4 (2026-03-23) — Resolved open questions

| Issue | v3 | v4 | Reason |
|-------|----|----|--------|
| `--all-fields` | Kept as open question | Removed. Use `--output-columns '*'` | One mechanism for column selection, not two |
| `--backend` default | `default` | `auto` (selects `aioboto3` when installed + S3 output) | Eliminates leaky abstraction; explicit override still available |
| `size` command | Separate command with duplicated options | Alias for `index query --count-only`. `--details` passed through | Reduces surface area; canonical form is a flag on `index query` |
| `--output-dir` with `--glob` | Required explicit choice | `--output-dir` is the default when `--glob` is used | Batch filtering naturally produces per-file output; merge via explicit `--output` |

### v3 (2026-03-23) — Task-oriented subcommands and output split

Based on [review](new_cli_proposal_review.md).

| Issue | v2 | v3 | Reason |
|-------|----|----|--------|
| Backend-oriented subcommands | `index cdx`, `index file`, `index athena`, `index duckdb` | `index query`, `index filter`, `index sql --engine ...` | Subcommands should describe user tasks, not backends. A new user understands "query", "filter", "sql" immediately |
| Redundant Athena/DuckDB subcommands | Two separate subcommands with near-identical structure | One `index sql` with `--engine athena\|duckdb` | Users see "run SQL" as one task, not two |
| `--output` overloaded | Single `--output` for file, directory, and stdout | `--output` (single file/stdout) vs `--output-dir` (mirrored multi-file). Mutually exclusive | Eliminates ambiguity when filtering batches of CDX files |
| stdin not formalized | Piping documented as example only | `repackage -` is a first-class input mode. `--input-format` defaults to `cdx` for stdin | Unix composability. Resolves former open question #1 |
| Filtering flags URL-biased | `--url-list`, `--url-list-column` | `--match`, `--match-from`, `--match-column`, `--match-type` | Neutral between URL and SURT. Added inline `--match` for simple cases |
| `--input-glob` verbose | `--input-glob` | `--glob` | Context (`index filter`) already implies input; shorter |
| No Quick Start | Document started with taxonomy | Quick Start section before full grammar | New users need a fast path to success |

### v2 (2026-03-20) — Consistency and ergonomics

| Issue | v1 (original) | v2 | Reason |
|-------|---------------|----|----|
| 4 positional tokens | `cdxt index cdx cc 'url'` | `cdxt index cdx 'url'` (with `--source cc` default) | Too many positionals; `cc` is a sensible default |
| `--filter` overloaded | `--filter`, `--filter-type`, `--include-url` | `--filter`, `--match-type`, `--url-list` | "filter" meant 3 different things |
| Duplicate options | `--from/--to/--limit` in both CDX-specific and common sections | Listed once in common section only | Ambiguous which set applied |
| Input format naming | `--index-format` on repackage | `--input-format` | Symmetric with `--output-format` |
| Ambiguous `--size` | `--size` on repackage | `--max-warc-size` | Could mean total size, per-file size, etc. |
| Opaque `--get` | `--get` on index cdx | `--no-paginate` | Leaked HTTP detail |
| Leaky `--implementation` | `--implementation` | `--backend` | Standard CLI terminology |
| Very long flag names | `--include-urls-from-file` (24 chars), `--include-urls-from-file-column` (31 chars), `--warc-download-prefix` (22 chars) | `--url-list` (10 chars), `--url-list-column` (17 chars), `--download-prefix` (17 chars) | Unwieldy to type and remember |
| No short flags | All flags were long-form only | `-o`, `-f`, `-l`, `-c`, `-s` for common options | Standard CLI ergonomics |
| Inconsistent `size` | `cdxt size cc 'url'` (positional source) | `cdxt size 'url' --source cc` (flag, default cc) | Align with `index cdx --source` pattern |
| `--overwrite` inconsistent | Only on `index file` | On both `index` and `repackage` | Both write output files |
| Missing stdin support | Not mentioned | `cdxt repackage -` documented | Unix composability (pipe index → repackage) |
