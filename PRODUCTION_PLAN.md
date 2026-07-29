# Production plan for address-normalizer v2

Status: P0/P1 hardening implemented for `2.0.0a2` on
`codex/v2-parser-sprint`; GPL-3.0-only licensing and model provenance are
recorded, and the alpha release is in progress.

This document records the current evidence and the gates for a releasable v2.
It does not declare the package production-ready. Publication still requires a
tagged, reproducible build and protected TestPyPI/PyPI release checks.

## Current-state audit

### Product boundary

Version 2 is a small, deterministic, offline Russian address **parser**. It
extracts an unverified interpretation with original character offsets. It is
not a FIAS/GAR resolver, geocoder, spelling authority, address validator,
service, or database.

- Version: `2.0.0a2`.
- Supported Python declared in package metadata: 3.10 through 3.14.
- Runtime dependencies: none.
- Public entry points: `parse()`, `parse_many()`, lazy `parse_iter()`,
  conservative `detect_addresses()`, `ParsedAddress`, `DetectedAddress`,
  `AddressPart`, `Alternative`, and the matching serialized `TypedDict`
  schemas.
- CLI: one-address JSON and stdin JSONL modes.
- Runtime model: 37,130-byte JSON linear-chain tagger.
- `2.0.0a2` candidate wheel: 45,843 bytes.
- Large corpora are ignored under `.cache/external/`; none is package data.
- Historical v1 root files are retained but are outside the `src/` package.

### API and behavior

The parser combines offset-preserving tokenization, explicit marker and numeric
rules, a compact sequence tagger for residual text, and deterministic
post-processing. Results retain raw substrings, `[start, end)` offsets,
unparsed spans, warnings, alternatives, and bounded confidence values.
Confidence is decision strength, not a calibrated probability.

Closed API audit items:

- `parse()`, `parse_many()`, and `parse_iter()` now have a consistent,
  explicitly tested input-error contract.
- Empty input, Unicode whitespace, `ё/е`, long and malformed input, one-shot
  iterables, and concurrent calls have explicit tests and documentation.
- `as_dict()` has a documented JSON-compatible schema with public `TypedDict`
  types.
- Batch parsing remains eagerly list-based. The additive `parse_iter()` helper
  provides one-pass lazy iteration and documents consumption and error timing.
- Supported source values, warning codes, alternative reasons, and the review
  policy are part of the public alpha contract.

### Reliability evidence

The four evidence domains remain separate because their sources, schemas, and
metrics answer different questions:

| Domain | Size | Primary metric | `2.0.0a2` baseline |
| --- | ---: | --- | ---: |
| Historical bank-shaped reference | 500 rows | exact component micro F1 | 95.9% |
| RedMadRobot noisy address windows | 578 windows | same-label span-overlap F1 | 58.7% |
| Deepparse nationwide clean strings | 100,000 rows | character-overlap F1 | 66.2% |
| Moscow official clean buildings | 15,196 rows | exact component-value F1 | 85.4% |

Additional named baselines:

- Historical exact-address match: 80.4%; no residual word/number tokens: 76.8%.
- RedMadRobot street-and-house slice: 78.8% span-overlap F1.
- Deepparse binary span-overlap F1: 84.4%; exact token-boundary sequence:
  8.0%.
- Moscow exact full-address match: 64.9%; street F1: 66.2%; house F1:
  98.2%; корпус F1: 99.6%; строение F1: 97.0%.
- Compact tagger grouped test: 96.5% token accuracy, 94.3% complete-sequence
  accuracy, and 96.9% micro entity F1. The split has no meaningful district or
  settlement coverage.

The historical 500-row gate is small enough for normal CI. RedMadRobot and the
large corpora stay opt-in and must not be downloaded by routine test or build
jobs. No implementation work in this sprint may be tuned against those sealed
external test results.

### Packaging, release, and repository

- `setuptools` builds from `src/`; model JSON and `py.typed` are declared as
  package data.
- Project metadata names the repository, Python range, console script, and has
  no runtime dependencies.
- CI now tests every declared Python minor from 3.10 through 3.14, checks
  deterministic model regeneration, runs the legacy gate, and builds
  distributions.
- Release gates cover strict typing, metadata validation, exact wheel
  allowlisting/forbidden-content inspection, clean wheel installation, CLI
  smoke tests, explicit wheel/model budgets, sdist review, and a Trusted
  Publishing workflow with named GitHub environments.
- Contributor, security, CODEOWNERS, PR, issue, support, triage, starter-work,
  release, examples, and launch-draft assets are present.
- The worktree initially contained unrelated untracked notebook artifacts:
  `Untitled.ipynb` and `.ipynb_checkpoints/`. They are not part of this sprint
  and must remain untouched and uncommitted.

## Users and primary use cases

1. **Application developer** — install a tiny wheel, parse one user-supplied
   address, inspect ambiguity, then query the application's own FIAS/GAR
   resolver.
2. **Data/ETL engineer** — stream JSONL or an iterable of strings through an
   offline, dependency-free parser while retaining raw values and offsets for
   audit and correction.
3. **API developer** — expose the typed result from FastAPI or another service
   without hidden network, filesystem, or process requirements.
4. **Evaluator/data steward** — reproduce named gates, understand the exact
   scoring boundary and provenance, and keep independent domains separate.
5. **Contributor/maintainer** — reproduce a bug, add a fail-before/pass-after
   test, measure cross-domain and artifact impact, and release only through a
   reviewable workflow.

Non-users include anyone needing existence verification, FIAS/GAR identifiers,
geocoding, authoritative correction, fuzzy registry search, or an embedded
current registry. Those needs require a downstream resolver.

## Public API contract

For the v2 pre-release series:

- `parse(text: str) -> ParsedAddress` parses exactly one string and raises
  `TypeError` for non-strings.
- `parse_many(addresses: Iterable[str]) -> list[ParsedAddress]` consumes the
  iterable once, preserves order, returns an eager list, and uses the same
  per-item validation as `parse()`.
- `parse_iter(addresses: Iterable[str]) -> Iterator[ParsedAddress]` preserves
  order, consumes once, avoids preloading, and raises element errors when
  iteration reaches them.
- `detect_addresses(text: str) -> tuple[DetectedAddress, ...]` returns ordered,
  non-overlapping half-open spans in a free-form message. Detection is
  conservative and requires a street marker plus a building, or an explicit
  address cue plus a parseable street and building.
- Empty and whitespace-only strings return a valid empty `ParsedAddress`; they
  do not invent components.
- `AddressPart.raw == ParsedAddress.raw[start:end]`; offsets are half-open
  indices into the original Python string.
- `as_dict()` returns a JSON-compatible, stable v2 schema. Component keys remain
  present with `null` when absent; tuple fields serialize as arrays.
- `normalized` is a convenient rendering of the parser's unverified
  interpretation, not a canonical registry address.
- Confidence is a bounded ranking/review signal. No threshold may be described
  as a probability or guarantee.
- Warnings, alternatives, and unparsed content are public information, not
  debug output. Additive warning codes or alternative reasons may appear in
  pre-releases; removing result fields or changing their meaning requires
  migration notes.
- Parsing performs no network calls, downloads, service startup, or filesystem
  writes. A process-local immutable model may be cached and shared across
  threads.

## Release decisions

The maintainer selected GPL-3.0-only and authorized redistribution of the
historical workbook, its 500-row derivative, and the compact model. The complete
decision and reproducible provenance chain are recorded in `LICENSING.md`.
External Moscow, Deepparse, and RedMadRobot source corpora remain outside the
distribution under their separately recorded terms.

### Engineering blockers for a stable `2.0.0`

- Close all P0 gates below on every supported Python version.
- Independently review a documented sample of the legacy evaluation rows or
  create a replacement gold benchmark with a sealed final split.
- Improve or explicitly accept the weak administrative and exact-street
  behavior; do not hide it behind aggregate metrics.
- Freeze and document the v2 serialization, warning, and compatibility policy.
- Perform a TestPyPI rehearsal through the protected release environment.

## Prioritized work

### P0 — required before the next published pre-release

- Make API input/serialization/empty-input behavior typed, tested, and
  documented.
- Add adversarial regression tests for punctuation, casing, `ё/е`, Unicode
  whitespace, compound houses, корпус/строение, apartments, missing markers,
  reordered components, ambiguous numeric tails, one-shot iterables, and
  concurrency.
- Add a strict CI packaging job that builds sdist/wheel, validates metadata and
  contents, enforces wheel/model size budgets, installs the wheel into a clean
  environment, and smoke-tests import plus both CLI modes without network.
- Add an explicit type-checker configuration and gate the public package.
- Add a GitHub Actions release workflow using OIDC Trusted Publishing,
  immutable artifacts, protected environments, and an explicit version-tag
  check.
- Add a changelog and release checklist that put licensing/provenance before
  publication.
- Rewrite the README around the two-minute path, honest boundaries, named
  benchmarks, result review, FIAS/GAR handoff, API/CLI reference, migration,
  troubleshooting, and “Should I use this?” guidance.
- Add executable examples and issue/PR/security/support/triage assets.

Acceptance criteria:

- Unit tests pass on locally available interpreters and CI covers every
  declared Python minor.
- Static type checking passes with the selected, committed configuration.
- The historical 500-row gate meets every value in
  `evaluation/release_gates.json`; no named external baseline regresses as a
  side effect of code changes.
- Two consecutive deterministic model builds are byte-identical.
- Built metadata validates; a clean environment installs only the built wheel,
  imports from outside the checkout, parses an address, and runs single/JSONL
  CLI smoke tests.
- Wheel runtime content is allowlisted and excludes `.cache`, corpora,
  notebooks, training/evaluation data, legacy code, secrets, and forbidden
  dependencies.
- Model is at most 65,536 bytes; wheel is at most 262,144 bytes. Actual
  values are recorded in release evidence.
- README examples execute, README/package metadata render checks pass, and every
  numeric reliability claim names its domain and metric.

### P1 — high value for the beta

- Exercise and document the lazy batch iterator in streaming integrations.
- Add review-policy recipes for strict/manual/lenient queues based on warnings,
  alternatives, unparsed spans, and decision-strength confidence.
- Add reproducible FastAPI, ETL, JSONL, and customer-managed FIAS/GAR resolver
  examples without adding runtime dependencies.
- Make benchmark report schemas and metric names self-explanatory; add cheap
  fixture-level evaluator tests and provenance validation to routine CI.
- Publish a bounded set of useful starter-issue proposals and a release demo
  script/recording plan.
- Document alternative-comparison methodology without unverified competitor
  claims, plus maintainer-approved draft launch copy.

Acceptance criteria:

- Lazy parsing consumes a generator once and does not materialize it.
- Example source files are syntax-checked; dependency-bearing examples clearly
  separate optional dependencies from the core package.
- Fixture-level evaluation runs offline in normal CI, while large downloads
  require explicit commands.
- Contributor templates require a linked/reproduced problem, fail-before and
  pass-after evidence, cross-domain consideration, and human understanding.

### P2 — after beta evidence

- Curate training/validation data for street and administrative fields, with
  explicit rights and a new untouched final test.
- Add confidence calibration only if a representative labeled validation set
  supports it; otherwise retain decision-strength semantics.
- Report confidence intervals and regional/source-system slices.
- Rehearse TestPyPI, verify attestations and installation, then promote an
  unchanged artifact to PyPI with maintainer approval.
- Consider separately versioned integration adapters; keep the core runtime
  registry-independent.

## Decisions and unresolved maintainer questions

Recorded engineering decisions:

- Keep `parse_many()` eager and list-returning for compatibility.
- Prefer an additive lazy helper over changing `parse_many()` semantics.
- Keep runtime dependency-free and all data preparation dependencies separate.
- Keep the four benchmark domains and their metric families separate.
- Use PEP 440 progression `2.0.0aN` → `2.0.0bN` → `2.0.0rcN` → `2.0.0`;
  do not skip directly from this alpha to stable.
- Build once per release tag and publish that reviewed artifact through Trusted
  Publishing; never rebuild between test and publish.

Maintainer decisions still required:

1. Should historical v1 remain in the default branch for v2 stable, move to a
   named archival directory/branch, or be removed only in a future major
   cleanup?
2. Which GitHub environment will protect TestPyPI/PyPI publication, and who may
   approve it?
3. What administrative/street quality threshold is acceptable for beta, and
   who will perform the independent row review?
4. Should the eventual stable support policy include every Python minor
   3.10–3.14, or follow a rolling set once 3.10 reaches end of upstream support?

## Proposed release sequence

1. Complete and review P0.
2. Release `2.0.0a2` to TestPyPI from an approved tag; verify hashes,
   attestations, wheel contents, offline install, CLI, and rollback procedure.
3. Release `2.0.0a2` to PyPI only with explicit maintainer approval.
4. Expand human-reviewed validation coverage and close chosen quality targets;
   publish `2.0.0b1`.
5. Freeze API/serialization and documentation, run the sealed final evaluation
   once, and publish `2.0.0rc1`.
6. Promote the reviewed release-candidate code and evidence to `2.0.0`; rebuild
   only for a new version if any input changes.

## Evidence record for this sprint

Recorded on 2026-07-28 and rerun for the 2026-07-29 release candidate:

- Unit suite: 74 tests passed; fresh installed-wheel smoke tests passed on
  Python 3.10 and 3.14. CI covers 3.10 through 3.14.
- Strict `mypy==1.17.1`: success on all nine public package modules.
- Compact tagger: deterministic regeneration was byte-identical; the model is
  37,130 bytes with SHA-256
  `c23c4f3cf308b1f36f56d0679df278d66b3aeae41c70a2144d88606199f3eb36`.
- Historical 500-row gate: 80.4% exact-address match, 76.8% no-unparsed rate,
  and 95.8538% exact component micro F1; every committed gate passed.
- External baselines remained exactly unchanged: RedMadRobot span-overlap F1
  58.7462% (street-and-house slice 78.8392%); Deepparse span-overlap F1
  84.3948%, character-overlap F1 66.2289%, and token-label F1 66.4946%;
  Moscow exact component-value micro F1 85.3620% and exact-address match
  64.9316%.
- Two consecutive final builds were byte-identical. The wheel is 45,843 bytes
  (SHA-256
  `e98a49d0d7e8514485230bdccab043e6dedf0991c177a1c183fd561f734b1650`);
  the sdist is 73,601 bytes (SHA-256
  `e439ab16e0af50424d36c519c6fc309bafb3167af35ad3fb3b21f4f5b892bb1c`).
- The model remained 37,130 bytes, the wheel stayed below its 256 KiB budget,
  and the larger source archive includes both English and Russian
  documentation.
- Wheel metadata, archive safety, exact runtime allowlist, hashes, artifact
  policy rejection tests, Twine rendering, no-index/no-dependency installation,
  import, one-address CLI, and JSONL CLI checks all passed.
- GPL-3.0-only licensing and model provenance were subsequently approved on
  2026-07-29; `release-policy.toml` now enforces the publishable state.

Committed benchmark baselines must not be rewritten merely to make a change
look successful. Timing-only fields may vary when the reports are reproduced.

Detection and diagnostics addendum, recorded on 2026-07-29:

- `detect_addresses()` adds conservative, offset-preserving message-span
  detection without changing parser output on the historical regression.
- The detection behavior fixture contains 30 narrow messages: 18 positive and
  12 negative, with 20 exact address spans. All currently pass, but this
  authored fixture is not an independent accuracy benchmark.
- The historical diagnostic table contains all 500 rows and more than 75
  columns covering per-field outcomes, scenario dimensions, warnings,
  unparsed evidence, and heuristic triage causes. It records 402 exact and 98
  non-exact rows.
- Unit tests pass on locally available Python 3.10, 3.13, and 3.14: 74 passed
  per interpreter. Strict `mypy==1.17.1` passes all nine package modules.
- The historical parsing gate remains unchanged at 80.4% exact-address match,
  76.8% no-unparsed rate, and 95.8538% exact component micro F1.
- On complete RedMadRobot messages, the development-only detector diagnostic
  reports 98.0% any-overlap precision, 68.1% recall, 80.3% F1, and 100%
  negative-message specificity. Its failures were inspected, so it is not a
  sealed final-test result.
- The reproducible wheel is now 33,104 bytes with SHA-256
  `15f38aa4dde86da620d05d4e8e620624769b26430c7b365b9f62d2575c87c8ed`;
  the sdist is 52,762 bytes with SHA-256
  `e45303316773c560473e16e12fe2f574967d4593cf3d0b7c778d7fa9a3d7dda3`.
