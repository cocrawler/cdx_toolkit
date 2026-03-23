# Review 3: `new_cli_proposal.md`

## Overall Assessment

This revision is in good shape structurally.

The command layout is now much easier to learn than the earlier versions:

- `index query`, `index filter`, and `index sql` are task-oriented
- `repackage` is cleanly separated from index generation
- the quick-start section gives a reasonable end-to-end path
- the pipe workflow is now internally consistent

The main remaining UX issues are no longer about command architecture. They are about first-run clarity: what a new user sees, what defaults they will infer, and where they are likely to make the wrong guess.

## Main Findings

### 1. The default `index query` output is still optimized for pipelines, not for people

The very first example is:

```bash
cdxt index query 'commoncrawl.org/*' -l 10
```

That prints raw `cdx`, which is domain-native but not beginner-friendly. A new user who has never worked with CDX before will not immediately know:

- what the columns mean
- whether the command succeeded
- why this format is preferable to `jsonl` or `csv`

This is a documentation UX problem even if the default format remains `cdx` for technical reasons.

Recommendation:

- Keep `cdx` as the default if pipeline compatibility matters.
- But change the first quick-start example to either:
  - explicitly say "prints raw CDX records", or
  - use `-f jsonl` or `-f csv` for the first human-facing example.
- Add one short sentence near `--output-format` explaining why `cdx` is the default:
  - good for piping into `repackage`
  - native archival index format

Right now the CLI is consistent, but the first visible output is still more legible to experienced users than to new ones.

### 2. `index filter` still has a major beginner footgun: default `--match-type surt`

The proposal says:

- `--match-type <type>` supports `url` or `surt`
- default is `surt`

That is risky for new users.

A new user is much more likely to have:

- a text file of URLs
- an intuition that matching will happen on normal URLs
- no idea what SURT is

If they forget `--match-type url`, the command may still run but produce confusingly empty or partial results. That is a bad failure mode because it looks like a data problem rather than a CLI-usage problem.

Recommendation:

- Prefer `url` as the default for new-user ergonomics, or
- require `--match-type` explicitly, or
- auto-detect based on the first pattern and warn when ambiguous

If the implementation strongly prefers SURT internally, that is still not a reason to expose SURT as the default user expectation.

### 3. The `<input_path>` + `--glob` model is still harder to understand than it should be

`index filter` currently uses:

```bash
cdxt index filter <input_path> --glob <pattern>
```

For experienced users this is fine, but for new users it creates avoidable questions:

- is `--glob` relative to `<input_path>` or absolute?
- why does the example use `s3://.../collections` plus a glob starting with `/CC-MAIN-...`?
- could I just pass one globbed path directly?

This is learnable, but it is not self-explanatory.

Recommendation:

- State explicitly that `--glob` is evaluated relative to `<input_path>`.
- Show one example with a local directory and a relative glob:

```bash
cdxt index filter ./indexes --glob '2024/*.gz' ...
```

- If implementation allows it, consider supporting a single globbed input path as the simpler mental model.

This is probably the least intuitive part of the current proposal.

### 4. `repackage` is correct domain language, but still needs a plain-language gloss everywhere it first appears

Archive practitioners will understand `repackage`. New users often will not.

The proposal already explains it once, but the term still appears early and often enough that the doc should keep translating it into plain English:

- "download the captured records referenced by the index"
- "write them into new WARC files"

Recommendation:

- In Quick Start step 3, change "Build WARCs from that index" to something slightly more explicit, such as:
  - "Download those captures and write new WARC files"
- In the command overview, keep the current term but pair it with that plain-language description consistently.

The command name itself is fine. The documentation just should not assume the term is self-evident.

### 5. The proposal should specify help and error-message behavior more explicitly

The document says each subcommand gets focused `--help`, which is good. For new users, the next important thing is guided failure.

The commands most likely to be mistyped or misunderstood are:

- `cdxt index`
- `cdxt index filter` without any `--match`
- `cdxt repackage -` with the wrong `--input-format`
- `cdxt warc ...`

Recommendation:

- Add a short section or note describing the intended error style:
  - explain what was wrong
  - show the closest valid command
  - include one working example

For example, `index filter` without matches should not just say "missing required argument". It should say something like:

```text
Error: index filter requires at least one --match or --match-from.
Try: cdxt index filter ./data/index.cdx.gz --match 'example.com/*'
```

That matters because first-run CLI UX is driven as much by recoverability as by the happy path.

## Suggested Edits

These would improve new-user UX the most:

1. Make the first `index query` example more human-readable, or explicitly label raw CDX as machine-oriented output.
2. Revisit the default `--match-type surt`; it is too expert-oriented as the default.
3. Clarify that `--glob` is relative to `<input_path>` and add a simpler local example.
4. Keep translating `repackage` into plain language in quick-start and overview text.
5. Specify guided error/help behavior, not just command grammar.

## Bottom Line

This proposal is now close to a strong CLI design.

The remaining weaknesses are mostly about onboarding. A new user can now find the right command family, but they may still stumble on raw CDX output, SURT-default matching, and the two-part `filter` path model. If those areas are clarified, the proposal should be easy to understand and much easier to adopt.
