# Review: `new_cli_proposal.md`

## Scope

This review compares:

- the current CLI in `main`
- the functionality introduced by PR #54, which is available locally as `origin/feat/warc-by-cdx`
- the proposed UX in `docs/notes/new_cli_proposal.md`

The proposal is directionally good: splitting "find records" from "build WARCs" is the right move. The main remaining problem is that the proposed CLI is still too implementation-shaped. It exposes storage/backend distinctions (`cdx`, `file`, `athena`, `duckdb`) more prominently than user tasks, which makes the UX harder to learn than it needs to be.

## What Works Well

- Splitting `iter` and `warc` into `index` and `repackage` matches the real workflow better.
- Keeping `iter` as a deprecated alias is a low-friction migration path.
- Restricting `repackage` to structured `index` output is a good boundary. It reduces ambiguity and makes the pipeline clearer.
- Preserving WARC-oriented terms such as `--prefix` and `--subprefix` is sensible for experienced archive users.
- Including migration mappings and examples is the right instinct.

## Main UX Issues

### 1. `index` mixes too many different jobs

The proposal uses `index` for all of the following:

- query a CDX API
- filter one or more CDX files
- run Athena SQL
- run DuckDB SQL

Those are not one mental model for a new user. A new user thinks in terms of:

- "query the archive"
- "filter existing index files"
- "run an SQL query"

They do not naturally think in terms of backend families.

Recommendation:

- Keep `index` as the umbrella verb if issue #582 requires it.
- Change the subcommands to task-oriented names:
  - `cdxt index query`
  - `cdxt index filter`
  - `cdxt index sql`

This is easier to understand than:

- `cdxt index cdx`
- `cdxt index file`
- `cdxt index athena`
- `cdxt index duckdb`

### 2. `cdx` and `file` are internal terms, not user-facing task names

`cdx` is jargon. `file` is too generic. Neither tells a new user what they are trying to do.

`query` and `filter` are much clearer:

- `query` means "ask an archive index for matching captures"
- `filter` means "read existing index files and keep matching rows"

### 3. The proposal hides an important workflow mismatch with PR #54

PR #54's `filter_cdx` is a file-to-file filtering workflow that mirrors directory structure and supports globbed batches. The proposal turns that into `index file ... --output filtered.csv`, which looks like a single normalized table export.

Those are different UX models:

- batch file transformation
- normalized index export

The proposal should choose one explicitly. Right now it tries to sound simpler than the underlying workflow actually is.

Recommendation:

- Decide whether `index filter` is:
  - a batch CDX-file transformer, or
  - a normalized record exporter
- If both are needed, make them explicit with separate output modes.

Example:

```bash
cdxt index filter s3://commoncrawl/cc-index/collections \
  --glob "/CC-MAIN-2024-30/indexes/*.gz" \
  --match-from surt-whitelist.txt \
  --match-type surt \
  --output-dir s3://my-bucket/filtered-cdx/ \
  --preserve-layout
```

versus

```bash
cdxt index filter s3://commoncrawl/cc-index/collections \
  --glob "/CC-MAIN-2024-30/indexes/*.gz" \
  --match-from surt-whitelist.txt \
  --match-type surt \
  --output filtered.csv \
  --output-format csv
```

These should not be described as if they are the same thing.

### 4. `--output` is overloaded

In the proposal, `--output` can mean:

- stdout fallback when absent
- a single file
- an S3 destination
- in examples, sometimes a directory-like prefix

That will create avoidable mistakes.

Recommendation:

- Use explicit output flags:
  - `--output` for one file or stdout
  - `--output-dir` for mirrored multi-file output
- For WARC generation, prefer `--output-prefix` as the user-facing name and keep `--prefix` as an alias if needed for compatibility with existing WARC terminology.

### 5. `size` is now the odd one out

The proposal keeps:

```bash
cdxt size cc 'commoncrawl.org/*'
```

while the main command becomes:

```bash
cdxt index cdx cc 'commoncrawl.org/*'
```

That is not fatal, but it is inconsistent.

Recommendation:

- Either keep `size` short and explicit:
  - `cdxt size --source cc 'commoncrawl.org/*'`
- Or align it structurally:
  - `cdxt index query --estimate ...`

My preference is the first option. `size` is small and common enough that a flatter syntax is better.

### 6. The file-filtering flags are too verbose and too specific

Flags like:

- `--include-url`
- `--include-urls-from-file`
- `--include-urls-from-file-column`

read as low-level implementation details. They also imply URL-only filtering even though SURT is a first-class mode.

Recommendation:

- Replace them with a smaller set:
  - `--match <pattern>` repeatable
  - `--match-from <path>`
  - `--match-column <name>`
  - `--match-type url|surt`

That language works for both URL and SURT workflows.

### 7. Athena and DuckDB should not each be a separate command shape

The proposal says:

- `cdxt index athena <connection_url> --sql query.sql`
- `cdxt index duckdb <database_path> --sql query.sql`

That is structurally repetitive. Users will see them as "run SQL against the columnar index", not as two different top-level concepts.

Recommendation:

- Collapse them under one subcommand:

```bash
cdxt index sql --engine athena --connection 'athena://...' --sql query.sql
cdxt index sql --engine duckdb --database ./cc-index.duckdb --sql query.sql
```

### 8. The proposal needs stronger guidance for first-run success

The doc is thorough, but it starts with taxonomy. New users need a quick path to success:

1. query captures
2. save an index
3. repackage WARCs

Recommendation:

- Move a "Quick Start" section above the full grammar.
- Put the three most common commands first.
- Delay edge-case detail until later.

## Recommended CLI Shape

I recommend keeping the issue's high-level model, but making the interface more task-oriented:

```text
cdxt <command> ...

Commands:
  index query     Query a CDX API and emit index records
  index filter    Filter local or remote CDX files and emit index records
  index sql       Run SQL against the columnar index and emit index records
  repackage       Build WARC files from index records
  size            Estimate result count for an archive query
```

### `index query`

```bash
cdxt index query 'commoncrawl.org/*' --source cc --limit 100
cdxt index query 'example.com/*' --source ia --from 2020 --to 2024 --output results.jsonl
cdxt index query 'example.com/*' --source https://my-cdx-server.example.com --filter '=status:200'
```

Why this is better:

- the user starts with the task, not the backend name
- `--source` is explicit and self-documenting
- it reads naturally in help text

### `index filter`

```bash
cdxt index filter ./data/index.cdx.gz \
  --match-from whitelist.txt \
  --match-type url \
  --output filtered.csv

cdxt index filter s3://commoncrawl/cc-index/collections \
  --glob "/CC-MAIN-2024-30/indexes/*.gz" \
  --match-from surt-whitelist.txt \
  --match-type surt \
  --output-dir s3://my-bucket/filtered-cdx/ \
  --preserve-layout
```

Why this is better:

- "filter" describes the job directly
- output intent is clearer
- it leaves room for both normalized export and mirrored-file workflows

### `index sql`

```bash
cdxt index sql --engine athena \
  --connection 'athena://athena.us-east-1.amazonaws.com:443/ccindex?S3OutputLocation=s3://my-bucket/results/' \
  --sql query.sql \
  --output results.csv

cdxt index sql --engine duckdb \
  --database ./cc-index.duckdb \
  --sql query.sql \
  --output results.csv
```

Why this is better:

- one conceptual entry point for SQL-based indexing
- consistent option naming across engines

### `repackage`

```bash
cdxt repackage filtered.csv \
  --output-prefix GOV-REPACKAGE \
  --warc-download-prefix s3://commoncrawl

cdxt repackage s3://my-bucket/index.csv \
  --output-prefix s3://my-bucket/warcs/GOV-2024 \
  --size 1073741824 \
  --creator "Common Crawl Foundation" \
  --add-metadata-record s3://my-bucket/gov-urls.txt
```

Recommendation:

- Support `cdxt repackage -` for stdin.
- Accept `--output-prefix` and keep `--prefix` as a compatibility alias if required.

## Specific Improvements To The Proposal Document

### Improve the command overview

Replace the current backend-oriented overview with a task-oriented one. New users should be able to identify their workflow immediately.

### Add a Quick Start before the full grammar

Suggested order:

1. "Query captures and print to screen"
2. "Save an index file"
3. "Build WARCs from that index"
4. "Filter existing CDX files"
5. "Run SQL against the columnar index"

### Make output semantics explicit

Document these separately:

- stdout
- single output file
- mirrored multi-file output directory
- WARC output prefix

### Separate user-facing terms from implementation terms

Avoid leading with:

- CDX API
- DuckDB
- Athena
- glob replication

Lead with the user goal, then describe the backend.

### Resolve open questions in the proposal before implementation

The current open questions are not minor. At least these should be resolved first:

- whether `repackage` reads stdin
- whether `index filter` is a file transformer or a normalized exporter
- whether `size` stays flat or becomes structurally aligned
- whether `--all-fields` survives as a convenience flag

## Bottom Line

The proposal is close, but not yet optimal for new users.

The core correction is:

- keep the workflow split
- reduce backend-first naming
- make output modes explicit
- align the commands around user tasks: query, filter, sql, repackage

If the team wants the smallest possible change from the current proposal, the highest-value edits are:

1. rename `index cdx` to `index query`
2. rename `index file` to `index filter`
3. collapse `athena` and `duckdb` into `index sql --engine ...`
4. split `--output` and `--output-dir`
5. support `repackage -` from stdin
