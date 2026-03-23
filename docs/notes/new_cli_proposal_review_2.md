# Review 2: `new_cli_proposal.md`

## Overall Assessment

This revision is substantially better than the previous one.

The biggest structural problems are now fixed:

- task-oriented subcommands (`query`, `filter`, `sql`) are clearer than backend-oriented ones
- `--output` and `--output-dir` are separated
- the quick-start section is much easier for a new user to follow
- `size` is simplified by making it an alias
- the option names are more consistent overall

At this point, the proposal is close. The remaining issues are mostly about small but important UX traps, especially around defaults and shell behavior.

## Findings

### 1. The default pipe workflow is still a footgun

The proposal says:

- `index query` defaults to output format `cdx`
- `repackage -` defaults stdin input format to `cdxj`

That means the most natural Unix pipeline:

```bash
cdxt index query 'example.com/*' | cdxt repackage - --prefix mydata
```

does not work unless the user already knows to add either:

```bash
cdxt index query 'example.com/*' -f cdxj | cdxt repackage - --prefix mydata
```

or:

```bash
cdxt index query 'example.com/*' | cdxt repackage - --input-format cdx --prefix mydata
```

That is a bad first-run experience because the obvious command fails while the more expert command succeeds.

Recommendation:

- Make the default piped workflow work without extra flags.
- The cleanest options are:
  - default stdin for `repackage -` to `cdx` instead of `cdxj`, because that matches `index query`
  - or default stdout from `index query` to `cdxj`
  - or detect `cdx` vs `cdxj` from the first input line when reading stdin

If implementation simplicity matters most, stdin auto-detection is probably the best UX.

### 2. `--output-columns '*'` is shell-hostile

The proposal removes `--all-fields` and replaces it with:

```bash
--output-columns '*'
```

That is risky because `*` is shell syntax. In many working directories it will expand to filenames unless quoted correctly. The proposal mentions the replacement, but it does not call out the quoting requirement.

This is the kind of detail experienced shell users recover from quickly, but new users often do not.

Recommendation:

- Keep `--all-fields` as a convenience alias, even if `--output-columns` remains the canonical mechanism.
- If the alias is removed, the docs should explicitly show:

```bash
cdxt index query 'commoncrawl.org/*' --output-columns '*'
```

with a note that the `*` must be quoted.

The same issue applies to the suggested pass-through form:

```bash
--match '*'
```

That also needs explicit quoting in the docs.

### 3. `size` / `--count-only` / `--details` is still slightly inconsistent

The proposal says:

- `size` is an alias for `index query --count-only`
- `size` accepts `--details`
- the long-form example shows `cdxt index query ... --count-only --details`

But `--details` does not appear in the `index query` option table.

That creates a documentation mismatch between the canonical command and the alias.

Recommendation:

- Document `--details` in the `index query` section, marked as:
  - valid only with `--count-only`
- Or add a short subsection under `--count-only` describing its companion flags.

Right now the proposal is understandable, but the help model is not fully coherent.

### 4. The `--glob` default to `--output-dir` is convenient, but too implicit

This sentence is sensible:

> `--output-dir` is default when `--glob` is used

But it is also easy to miss. It changes the output mode from "single stream" to "mirrored directory tree" based on one option.

That is reasonable for power users, but it adds hidden behavior for new users.

Recommendation:

- Keep the default if desired, but surface it more aggressively.
- The first `index filter --glob` example should explicitly say:
  - "because `--glob` is present, output defaults to per-file directory mode unless `-o` is given"
- Consider making `--output-dir` explicit in the syntax block even when it is implied.

This is not a blocker, but it should not be easy to miss.

### 5. `--backend auto` needs one sentence about observability

`auto` is a good default, but it changes behavior based on environment and installed extras:

- S3 + `aioboto3` installed
- S3 + `aioboto3` not installed
- non-S3 paths

That is convenient, but support/debugging becomes harder if users cannot tell which backend actually ran.

Recommendation:

- State that the chosen backend should be logged at `-v` / `-vv`.
- Optionally mention this directly in the proposal:
  - "`auto` selects a backend automatically and logs the chosen backend in verbose mode."

This is a small doc issue, not a design problem.

## Suggested Edits

These are the highest-value edits before implementation:

1. Make `cdxt index query ... | cdxt repackage - ...` work by default.
2. Either keep `--all-fields` as an alias or explicitly document quoting for `'*'`.
3. Add `--details` to the `index query` documentation, scoped to `--count-only`.
4. Make the `--glob` => `--output-dir` default more visible.
5. Note that `--backend auto` should log its chosen backend in verbose mode.

## Bottom Line

This version is close to a good user-facing CLI design.

The remaining issues are not about command structure anymore. They are mostly about avoiding surprising defaults and avoiding shell-specific traps. If those are cleaned up, the proposal should be in good shape for implementation.
