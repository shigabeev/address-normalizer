# Good first issue proposals

These are issue drafts, not work already authorized. A maintainer should assign
an owner, confirm the acceptance criteria still match `main`, and create the
issue before adding `good first issue`.

All proposals are ready for discussion under the repository's GPL-3.0-only
contribution terms.

## Available now: minimal missing-marker failure set

**Why it matters:** Current external reports show that unmarked administrative
and street fields are weaker than numeric fields. Small public reproductions
help describe that boundary without sharing production data.

**Scope**

- Create three synthetic Russian address inputs covering distinct missing-marker
  patterns.
- For each, record `parse(...).as_dict()`, expected component values, exact
  `[start, end)` offsets, and why the expected interpretation is unambiguous.
- Submit them in one parsing-failure issue; do not change code or tests.

**Acceptance criteria**

- No real person or private address is used.
- Examples are not copied from committed sealed-test failure samples.
- Every expected `raw` value equals the source slice at its proposed span.
- The three examples represent different patterns, not spelling variants.

## Available now: resolver contract field review

**Why it matters:** The generic resolver example must be understandable to teams
that operate different FIAS/GAR indexes.

**Scope**

- Run `examples/fias_gar_http.py` in dry-run mode on two synthetic addresses,
  including one with an alternative.
- Review whether `raw`, `components`, `warnings`, and `alternatives` are enough
  to adapt at an application boundary.
- Open one documentation issue with confusing names or missing explanation; do
  not propose a universal FIAS/GAR API.

**Acceptance criteria**

- No network request or private endpoint is used.
- The issue distinguishes parser output from resolver verification.
- Suggestions do not add FIAS/GAR data, authentication policy, or a network
  dependency to the package.

## CLI stdin and JSONL contract tests

**Why it matters:** The CLI is the smallest integration surface for shell and
batch users, but its line-preservation behavior should be executable
documentation.

**Scope**

- Add subprocess tests for one positional address, single-address stdin, JSONL
  order, CRLF input, Unicode, and a blank JSONL line.
- Assert JSON structure and process exit status, not whitespace formatting
  except where the CLI contract requires it.

**Acceptance criteria**

- Each test fails for a demonstrated contract break, not only a fabricated
  internal change.
- Tests invoke the installed entry point or the documented module boundary.
- No network, temp data outside the test directory, or timing assertion.

## Public typed-dictionary example check

**Why it matters:** `as_dict()` is the JSON boundary and exports typed-dictionary
shapes. A small checked example can catch documentation drift.

**Scope**

- Add a type-check fixture assigning `parse(...).as_dict()` to
  `ParsedAddressDict`.
- Exercise one optional component, one `AddressPartDict`, warnings, and
  alternatives.
- Document the selected type checker and exact command.

**Acceptance criteria**

- No new runtime dependency.
- The type-check dependency remains development-only.
- The fixture checks public imports rather than private implementation types.
- The command is run in CI only after the maintainer agrees on the type-check
  policy.

## Span-verification recipe

**Why it matters:** Consumers need a safe way to prove every extracted `raw`
substring maps back to the original input.

**Scope**

- Add a compact example that iterates every populated part plus `unparsed` and
  asserts `result.raw[start:end] == part.raw`.
- Cover Unicode whitespace and `ё` without normalizing the source first.
- Link it from the README review policy.

**Acceptance criteria**

- Uses only the public API and standard library.
- Demonstrates original-text offsets, not offsets into `normalized`.
- Includes a runnable command and expected output.

## Tasks that are not good first issues

Do not label compact-model retraining, benchmark threshold changes, new runtime
dependencies, public API redesign, release workflows, license selection, or
FIAS/GAR resolution as starter tasks. They require maintainer decisions and
cross-domain review.
