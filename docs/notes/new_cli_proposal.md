# Proposal: New `cdxt` Command Line Interface

**Date:** 2026-03-23 (v8)
**Based on:** [Issue #582](https://github.com/commoncrawl/issues/issues/582), [PR #54](https://github.com/commoncrawl/cdx_toolkit/pull/54)
**Status quo:** [new_cli_status_quo.md](new_cli_status_quo.md)
**Reviews:** [review 1](new_cli_proposal_review.md), [review 2](new_cli_proposal_review_2.md), [review 3](new_cli_proposal_review_3.md)
**Revision history:** [new_cli_proposal_revision_history.md](new_cli_proposal_revision_history.md)

---

## 1. Quick Start

The three most common workflows:

```bash
# 1. Query captures and print raw CDX records to screen (machine-readable index format)
cdxt index query 'commoncrawl.org/*' -l 10

# 2. Save an index file
cdxt index query 'commoncrawl.org/*' --filter '=status:200' -o captures.cdx.gz

# 3. Download those captures and write new WARC files
cdxt repackage captures.cdx.gz --prefix my-archive
```

More advanced:

```bash
# 4. Filter existing CDX files by URL whitelist (--output-dir is default with --glob)
cdxt index filter s3://commoncrawl/cc-index/collections \
    --glob "/CC-MAIN-2024-30/indexes/*.gz" \
    --match-from url-whitelist.txt \
    --output-dir ./filtered-cdx/

# 5. Run SQL against the columnar index
cdxt index sql --engine duckdb --database ./cc-index.duckdb \
    --sql query.sql -o results.csv

# 6. Pipe query output directly into repackage (no intermediate file)
cdxt index query 'example.com/*' | \
    cdxt repackage - --prefix mydata
```

---

## 2. Proposed New CLI

### Overview

```
cdxt <command> [options]

Commands:
  index query     Query a CDX API and emit index records
  index filter    Filter local or remote CDX files and emit index records
  index sql       Run SQL against the columnar index and emit index records
  repackage       Download captured web pages referenced by an index and write them into new WARC files

Aliases:
  size            Shortcut for 'index query --count-only'

Deprecated aliases:
  iter            Alias for 'index query' (shows deprecation warning)

Retired:
  warc            Shows error with migration instructions
```

> **Design note:** Subcommands are named after user tasks (`query`, `filter`, `sql`),
> not backend implementations (`cdx`, `file`, `athena`). A new user can identify their
> workflow from the command name alone. Each subcommand gets its own `--help` output
> showing only relevant options.

---

### 2.1 Command: `index`

Queries or filters a CDX source and writes matching index records to stdout or a file.

#### Syntax

```
cdxt index <task> [task-specific args] [common options]
```

#### Task subcommands

| Subcommand | Description |
|------------|-------------|
| `query`   | Query a CDX API (Common Crawl, Internet Archive, or custom) |
| `filter`  | Filter local or remote CDX file(s) by whitelist |
| `sql`     | Query the Common Crawl columnar index via Athena or DuckDB |

#### Task-specific arguments

**`index query` — CDX API query:**

```
cdxt index query <url_pattern> [options]
```

- `<url_pattern>`: URL or wildcard pattern to query (e.g., `commoncrawl.org/*`)

| Option | Short | Description |
|--------|-------|-------------|
| `--source <src>` | `-s` | CDX source: `cc` (default), `ia`, or a custom server URL |
| `--crawl <name_or_count>` | `-c` | Crawl name(s) (comma-separated) or integer for N most recent |
| `--filter <expr>` | | CDX API filter expression (repeatable) |
| `--closest <timestamp>` | | Get closest capture to timestamp |
| `--no-paginate` | | Use single GET request instead of pagination |
| `--cc-mirror <url>` | | Use specific Common Crawl index mirror |
| `--cc-sort <order>` | | Sort order: `mixed` (default) or `ascending` |
| `--from <timestamp>` | | Start of time range (CDX API parameter) |
| `--to <timestamp>` | | End of time range (CDX API parameter) |
| `--limit <n>` | `-l` | Maximum number of results (server-side) |
| `--count-only` | | Print estimated result count instead of emitting records. `cdxt size` is an alias for `cdxt index query --count-only` |
| `--details` | | Show per-sub-index breakdown (only valid with `--count-only`) |

> **`--match` / `--match-from` are intentionally not available on `index query`.**
> Each pattern would require a separate remote API request. A whitelist of thousands
> of entries would generate thousands of rate-limited calls, making the operation
> impractically slow and error-prone. For bulk pattern matching, use `index filter`
> against local or S3-hosted CDX files instead. For one-off multi-pattern queries,
> loop in the shell:
> ```bash
> while read -r url; do
>     cdxt index query "$url" --source cc
> done < url-list.txt >> results.cdx.gz
> ```
>
> Note: `--filter` (e.g., `--filter '=status:200'`) is available — it is a server-side
> filter applied to the single query, not a separate request per expression.

**`index filter` — Local/remote CDX file filtering:**

```
cdxt index filter <input_path> [options]
```

- `<input_path>`: Path to a CDX file or a base directory (local or S3)

| Option | Description |
|--------|-------------|
| `--glob <pattern>` | Glob pattern to match multiple CDX files, evaluated relative to `<input_path>` (e.g., `"2024/*.gz"`) |
| `--match <pattern>` | Inline URL/SURT pattern to match (repeatable). **At least one `--match` or `--match-from` is required** |
| `--match-from <path>` | File with URL/SURT patterns (one per line, or CSV). **At least one `--match` or `--match-from` is required** |
| `--match-column <name>` | Column name if `--match-from` file is CSV |
| `--match-type <type>` | How patterns are matched: `url` (default) or `surt` |
| `--output-dir <path>` | Output to a directory, one file per input. **Default when `--glob` is used**; use `--output` to explicitly merge into a single file instead |
| `--limit <n>` | Maximum number of output records (client-side) |
| `--backend <name>` | I/O backend: `auto` (default — uses `aioboto3` when installed and I/O is S3, otherwise `default`), `default`, or `aioboto3`. The chosen backend is logged at `-v` |

> **Match required:** `index filter` requires at least one `--match` or `--match-from`
> flag. Running without any match criteria is an error — use `--match '*'` (note:
> the `*` must be quoted to prevent shell glob expansion) to explicitly pass through
> all records.
>
> **Renamed from v2:** `--url-list` → `--match-from`, `--url-list-column` →
> `--match-column`, `--input-glob` → `--glob`. The `--match-*` family is neutral
> between URL and SURT workflows, while `--url-list` implied URL-only filtering.
> Added `--match <pattern>` for inline patterns without needing a file.

**`index sql` — SQL query (Athena or DuckDB):**

```
cdxt index sql --engine <engine> --sql <file> [engine-specific options]
```

| Option | Description |
|--------|-------------|
| `--engine <name>` | SQL engine: `athena` or `duckdb` (**required**) |
| `--sql <file>` | SQL query file (**required**) |
| `--connection <url>` | Athena connection URL (**required** when `--engine athena`) |
| `--database <path>` | DuckDB database path (**required** when `--engine duckdb`) |

> **Collapsed from v2:** `index athena` and `index duckdb` were separate subcommands
> with nearly identical structure. Users see them as one task — "run SQL against the
> columnar index" — not two. The `--engine` flag selects the backend; `--connection`
> and `--database` are engine-specific required options.

#### Common `index` options (all task subcommands)

| Option | Short | Description |
|--------|-------|-------------|
| `--output <path>` | `-o` | Output to a single file (local or S3). Omit for stdout |
| `--output-format <fmt>` | `-f` | Output format: `cdx` (default), `cdxj`, `csv`, `jsonl`. Auto-detected from file extension if not specified. CDX is the default because it is the native archival index format and pipes directly into `repackage`. Use `-f csv` or `-f jsonl` for human-readable output |
| `--output-columns <cols>` | | Comma-separated list of output columns. Omit to emit all fields (the default) |
| `--compression <mode>` | | Compression: `auto` (default — gzip when output path ends in `.gz`), `gzip`, `none` |
| `--overwrite` | | Overwrite existing output files |

> **`--limit` is specific to `index query` and `index filter`** (see above). For
> `index sql`, use the SQL `LIMIT` clause directly in the query file.

> **File extension mapping for `--output-format` auto-detection:**
>
> | Extension(s) | Format |
> |--------------|--------|
> | `.cdx`, `.cdx.gz` | `cdx` |
> | `.cdxj`, `.cdxj.gz` | `cdxj` |
> | `.csv`, `.csv.gz` | `csv` |
> | `.jsonl`, `.jsonl.gz` | `jsonl` |
>
> When the extension ends in `.gz`, compression is applied automatically (equivalent
> to `--compression gzip`). Use `--compression none` to override.
>
> **`--output-dir` is specific to `index filter`** (see above). For `index query` and
> `index sql`, output is always a single stream (`--output` or stdout).

---

### 2.2 Command: `repackage`

Downloads the captured web pages referenced by an index file (output of `index`) and writes them into new WARC files.

#### Syntax

```
cdxt repackage <index_path> [options]
```

#### Required arguments

- `<index_path>`: Path to the index file (cdx, cdxj, csv, or jsonl; local or S3), or `-` to read from stdin

#### Options

| Option | Description |
|--------|-------------|
| `--input-format <fmt>` | Input format: `auto` (default — detects from file extension; falls back to `cdx` when reading from stdin), `cdx`, `cdxj`, `csv`, `jsonl` |
| `--prefix <name>` | WARC filename prefix (default: `TEST`) |
| `--subprefix <name>` | Additional WARC filename prefix |
| `--max-warc-size <bytes>` | Target per-file WARC size in bytes (default: `1000000000`) |
| `--download-prefix <url>` | URL prefix for fetching WARC content (default: `https://data.commoncrawl.org`) |
| `--creator <text>` | Creator metadata (person/organization/service) |
| `--operator <text>` | Operator metadata (person if creator is organization) |
| `--is-part-of <text>` | Collection name metadata |
| `--description <text>` | Description metadata |
| `--add-metadata-record <path>` | Add file as metadata record to WARC (repeatable) |
| `--backend <name>` | I/O backend: `auto` (default — uses `aioboto3` when installed and output is S3, otherwise `default`), `default`, or `aioboto3`. The chosen backend is logged at `-v` |
| `--overwrite` | Overwrite existing output files |

> **Stdin support:** `cdxt repackage -` reads index records from stdin, enabling
> `cdxt index query ... | cdxt repackage - ...` without intermediate files. When
> reading from stdin, `--input-format` defaults to `cdx` (matching the default
> output format of `index`), so the basic pipe works without extra flags.

---

### 2.3 Command: `size` (alias)

`size` is a convenience alias for `index query --count-only`. It accepts all `index query` options.

```
cdxt size <url_pattern> [options]

# equivalent to:
cdxt index query <url_pattern> --count-only [options]
```

Additional option (passed through to `--count-only`):

| Option | Description |
|--------|-------------|
| `--details` | Show details of each sub-index |

---

### 2.4 Global options (all commands)

| Option | Short | Description |
|--------|-------|-------------|
| `--version` | `-V` | Show version |
| `--verbose` | `-v` | Increase verbosity (`-v` = INFO, `-vv` = DEBUG) |

---

### 2.5 Deprecated / retired commands

| Old command | New equivalent | Behavior |
|-------------|---------------|----------|
| `cdxt iter ...` | `cdxt index query ...` | Runs as `index query` with deprecation warning. Old global flags `--cc`/`--ia` are mapped to `--source` |
| `cdxt warc ...` | `cdxt index query ... && cdxt repackage ...` | Prints error with migration instructions |

---

### 2.6 Error and help behavior

Error messages should be guided: explain what went wrong, show the closest valid command, and include a working example. This matters because first-run CLI UX is driven as much by recoverability as by the happy path.

**Examples of guided errors:**

```text
$ cdxt index
Error: 'index' requires a subcommand: query, filter, or sql.
Try: cdxt index query 'example.com/*' -l 10

$ cdxt index filter ./data/index.cdx.gz
Error: index filter requires at least one --match or --match-from.
Try: cdxt index filter ./data/index.cdx.gz --match 'example.com/*'

$ cdxt repackage - --prefix mydata < input.cdxj
Error: input format mismatch — expected cdx (default for stdin), got cdxj.
Try: cdxt repackage - --input-format cdxj --prefix mydata < input.cdxj

$ cdxt warc 'example.com/*'
Error: 'warc' has been removed. The workflow is now two steps:
  1. cdxt index query 'example.com/*' -o index.cdx.gz
  2. cdxt repackage index.cdx.gz --prefix ...
```

**`--help` per subcommand:** Each subcommand (`index query`, `index filter`, `index sql`, `repackage`) has its own `--help` showing only the relevant options for that command.

---

## 3. Usage Examples

### Querying a CDX API

```bash
# Query Common Crawl (default source), output to stdout as CDX
cdxt index query 'commoncrawl.org/*' -l 100

# Query Internet Archive with time range, save as JSONL
cdxt index query 'example.com/*' --source ia --from 2020 --to 2024 \
    -f jsonl -o results.jsonl

# Query Common Crawl, last 3 crawls, status 200 only
cdxt index query 'commoncrawl.org/*' -c 3 \
    --filter '=status:200' -o filtered.csv

# Query a custom CDX server
cdxt index query 'example.com/*' --source https://my-cdx-server.example.com \
    -o custom_results.csv
```

### Filtering local/remote CDX files

```bash
# Filter a single local CDX file by URL whitelist → single merged output
cdxt index filter ./data/index.cdx.gz \
    --match-from whitelist.txt \
    --match-type url \
    -o filtered.cdx.gz

# Filter with inline patterns (no file needed)
cdxt index filter ./data/index.cdx.gz \
    --match 'commoncrawl.org/*' --match 'example.com/*' \
    --match-type url \
    -o filtered.cdx.gz

# Filter multiple local CDX files using a relative glob
cdxt index filter ./indexes --glob '2024/*.gz' \
    --match 'example.com/*' \
    -o filtered.cdx.gz

# Filter many CDX files on S3 → single merged output
cdxt index filter s3://commoncrawl/cc-index/collections \
    --glob "/CC-MAIN-2024-30/indexes/*.gz" \
    --match-from s3://my-bucket/surt-whitelist.txt \
    --match-type surt \
    -o s3://my-bucket/filtered-index.cdx.gz --overwrite

# Filter many CDX files on S3 → mirrored directory (one output per input)
# Note: --glob implies --output-dir by default; use -o to merge into a single file instead
cdxt index filter s3://commoncrawl/cc-index/collections \
    --glob "/CC-MAIN-2024-30/indexes/*.gz" \
    --match-from surt-whitelist.txt \
    --match-type surt \
    --output-dir s3://my-bucket/filtered-cdx/ --overwrite
```

### Running SQL queries

```bash
# Athena query against the CC columnar index
cdxt index sql --engine athena \
    --connection 'athena://athena.us-east-1.amazonaws.com:443/ccindex?S3OutputLocation=s3://my-bucket/results/' \
    --sql query.sql -o s3://my-bucket/athena-results.csv

# DuckDB query
cdxt index sql --engine duckdb \
    --database ./cc-index.duckdb \
    --sql query.sql -o local-results.csv
```

### Repackaging into WARC files

```bash
# Basic repackage from a CDX index (uses default download prefix https://data.commoncrawl.org)
cdxt repackage filtered.cdx.gz \
    --prefix mydata

# Full EOT-style repackage with metadata
cdxt repackage s3://my-bucket/filtered-index/results.cdx.gz \
    --prefix s3://my-bucket/warcs/EOT-2024-REPACKAGE \
    --subprefix CC-MAIN-2024-30 \
    --max-warc-size 1073741824 \
    --download-prefix s3://commoncrawl \
    --creator "Common Crawl Foundation <https://commoncrawl.org>" \
    --operator "John Doe <mailto:john@example.org>" \
    --is-part-of "CC-MAIN-EOT-2024" \
    --description "Repackage of US federal government captures" \
    --add-metadata-record s3://my-bucket/url-list.txt \
    --add-metadata-record s3://my-bucket/filter-config.json

# Use aioboto3 backend for faster S3 throughput
cdxt repackage s3://my-bucket/index.cdx.gz \
    --prefix s3://my-bucket/warcs/output \
    --download-prefix s3://commoncrawl \
    --backend aioboto3
```

### Piping (no intermediate file)

```bash
# Pipe query output directly into repackage (cdx is the default for both sides)
cdxt index query 'example.com/*' | \
    cdxt repackage - --prefix mydata

# Pipe filtered CDX into repackage
cdxt index filter ./data/index.cdx.gz --match-from whitelist.txt | \
    cdxt repackage - --prefix filtered

# Pipe CDXJ output (explicit format on both sides)
cdxt index query 'example.com/*' -f cdxj | \
    cdxt repackage - --input-format cdxj --prefix mydata
```

### End-to-end workflow (index + repackage)

```bash
# Step 1: Filter CDX files from Common Crawl
cdxt -v index filter s3://commoncrawl/cc-index/collections \
    --glob "/CC-MAIN-2024-30/indexes/*.gz" \
    --match-from gov-urls.txt \
    --match-type url \
    -o s3://my-bucket/eot/filtered-index.cdx.gz

# Step 2: Repackage matching captures into WARC files
cdxt -v repackage s3://my-bucket/eot/filtered-index.cdx.gz \
    --prefix s3://my-bucket/eot/warcs/GOV-2024 \
    --max-warc-size 1073741824 \
    --download-prefix s3://commoncrawl \
    --creator "Common Crawl Foundation" \
    --add-metadata-record s3://my-bucket/eot/gov-urls.txt

# Step 1 (alt): Query the CDX API instead
cdxt -v index query 'gov.example.com/*' -c 3 \
    --filter '=status:200' -o gov-captures.cdx.gz

# Step 2 (alt): Repackage from the CDX API output
cdxt -v repackage gov-captures.cdx.gz \
    --prefix GOV-REPACKAGE --download-prefix s3://commoncrawl
```

### Size estimation

```bash
# Using the alias
cdxt size 'commoncrawl.org/*'
cdxt size 'example.com/*' --source ia --details

# Equivalent long form
cdxt index query 'commoncrawl.org/*' --count-only
cdxt index query 'example.com/*' --source ia --count-only --details
```

### Backward compatibility

```bash
# Old command (deprecated, still works with warning):
cdxt --cc iter 'commoncrawl.org/*'
# DeprecationWarning: 'iter' is deprecated. Use: cdxt index query 'commoncrawl.org/*'

# Old command (retired, shows error):
cdxt --cc warc 'commoncrawl.org/*'
# Error: 'warc' has been removed. Use 'cdxt index query <url> -o index.cdx.gz'
#        followed by 'cdxt repackage index.cdx.gz --prefix ...' instead.
```

---

## 4. Migration mapping

| Old CLI | New CLI |
|---------|---------|
| `cdxt --cc iter 'url'` | `cdxt index query 'url'` |
| `cdxt --ia iter 'url'` | `cdxt index query 'url' --source ia` |
| `cdxt --cc --jsonl iter 'url'` | `cdxt index query 'url' -f jsonl` |
| `cdxt --cc --cdxj iter 'url'` | `cdxt index query 'url' -f cdxj` |
| `cdxt --cc --csv iter 'url'` | `cdxt index query 'url' -f csv` |
| `cdxt --cc --fields 'url,status' iter 'url'` | `cdxt index query 'url' --output-columns url,status` |
| `cdxt --cc warc --prefix X 'url'` | `cdxt index query 'url' -o idx.cdx.gz && cdxt repackage idx.cdx.gz --prefix X` |
| `cdxt --cc size 'url'` | `cdxt size 'url'` |
| `cdxt --ia size 'url'` | `cdxt size 'url' --source ia` |
| `cdxt filter_cdx ...` (PR #54) | `cdxt index filter ...` |
| `cdxt warc_by_cdx ...` (PR #54) | `cdxt repackage ...` |

---

## 5. Design decisions and rationale

| Decision | Rationale |
|----------|-----------|
| `index` + `repackage` naming | Agreed in issue #582. "index" produces an index, "repackage" is archiving community terminology |
| Task-oriented subcommands (`query`, `filter`, `sql`) | Named after what the user is doing, not the backend. A new user can identify their workflow from the command name. Replaces backend-oriented names (`cdx`, `file`, `athena`, `duckdb`) |
| `athena` + `duckdb` collapsed into `index sql --engine` | Users see "run SQL against the columnar index" as one task. Two subcommands with near-identical structure were redundant |
| `--source` flag (not positional) for `cc`/`ia` | Reduces positional depth. `cc` is a sensible default; `ia` and custom URLs are opt-in |
| `--output` vs `--output-dir` | `--output` = single file or stdout (all `index` subcommands). `--output-dir` = mirrored per-file output for batch filtering (`index filter` only). They are mutually exclusive. Eliminates ambiguity of one flag meaning both |
| `repackage` accepts `-` for stdin | Enables `cdxt index query ... \| cdxt repackage -` without intermediate files. Standard Unix composability pattern |
| `repackage` accepts cdx, cdxj, csv, and jsonl | Per wumpus: repackage takes `index` output only, not raw CDX files (March 14 comment). Accepts all formats that `index --output-format` can emit |
| No `--index-glob` on `repackage` | Per wumpus: repackage takes a single file, not globs |
| `--sql <filename>` as required flag | Per wumpus: distinct from engine connection options. Marked **required** in help text |
| `--prefix` / `--subprefix` for WARC naming | WARC standard terminology, already established in current CLI |
| `--add-metadata-record` (not `--write-paths-as-resource-records`) | Per wumpus: generic feature, not path-specific |
| `size` is an alias for `index query --count-only` | Keeps backward compatibility while avoiding a separate command with duplicated options. `--count-only` flag on `index query` is the canonical form; `--details` is passed through |
| `--backend` defaults to `auto` | Auto-selects `aioboto3` when installed and output is S3, otherwise `default`. The chosen backend is logged in verbose mode (`-v`). Eliminates leaky abstraction while still allowing explicit override |
| `--output-dir` defaults when `--glob` is used | Batch filtering naturally produces per-file output. Users can still merge with explicit `--output` |
| `--output-columns` defaults to all fields | When omitted, all fields are emitted. No `--all-fields` flag needed — the default behavior is all fields |
| Global `--cc`/`--ia`/`--source` removed | Replaced by per-command `--source` flag; cleaner and unambiguous |
| `iter` deprecated, `warc` retired | `iter` is a trivial alias; `warc` combines two operations that are now separate |
| `--no-paginate` replaces `--get` | Describes user-visible behavior, not HTTP implementation |
| `--match-*` family replaces `--url-list`/`--filter-type` | Neutral between URL and SURT workflows. `--match`, `--match-from`, `--match-column`, `--match-type` are consistent and shorter than the originals |
| `--match-type` defaults to `url` | New users are far more likely to have a list of URLs than SURTs. Defaulting to `surt` causes silent partial results when users forget the flag — a bad failure mode. SURT users are experienced enough to specify `--match-type surt` explicitly |
| `--max-warc-size` replaces `--size` | Explicit about per-file rotation, avoids ambiguity |
| `--download-prefix` defaults to `https://data.commoncrawl.org` | Sensible default for Common Crawl users; override with `--download-prefix s3://commoncrawl` for direct S3 access |
| `--download-prefix` replaces `--warc-download-prefix` | Context (repackage) already implies WARC |
| `--input-format` replaces `--index-format` | Symmetric with `--output-format`; general-purpose naming |
| `--backend` replaces `--implementation` | Standard CLI term for swappable I/O layers |
| Short flags for common options | `-o`, `-f`, `-l`, `-c`, `-s` for the most frequently used flags |
| `--overwrite` on both `index` and `repackage` | Any command writing files may need it |
| `cdx` as default format | Native archival index format; pipes directly into `repackage`. Used as default for both `--output-format` (index) and `--input-format` fallback (repackage). Use `-f csv` or `-f jsonl` for human-readable output. Auto-detection from file extension when `-o` is used without `-f` |
| Both `cdx` and `cdxj` supported | CDX is the traditional space-delimited format with a header line defining field order. CDXJ is the modern format where each line is `SURT timestamp {JSON}`. Both are first-class formats for input and output |
| `--compression` with `auto` default | Auto-detects from `.gz` extension. Explicit `gzip` or `none` for override. Avoids needing a separate compression step |
| `--output-dir` restricted to `index filter` | Only `index filter` with `--glob` produces multiple output files. `index query` and `index sql` always produce a single stream |
| `--backend` on both `index filter` and `repackage` | Both commands read/write S3 and benefit from `aioboto3`. `index query` and `index sql` do not need it (API/engine handles I/O) |
| `--match` / `--match-from` required on `index filter` | Filtering without match criteria is undefined. Use `--match '*'` (quoted) for explicit pass-through. Prevents accidental full-file processing |
| `--input-format` defaults to `cdx` on stdin | Matches `index` default output format (`cdx`), so the basic pipe `index query ... \| repackage -` works without extra flags |

---

## 6. Resolved questions and revision history

See [new_cli_proposal_revision_history.md](new_cli_proposal_revision_history.md) for all resolved questions (8 total) and the full revision history (v1–v8).
