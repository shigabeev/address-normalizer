# Changelog

All notable user-visible changes will be recorded here. This project follows
[Semantic Versioning](https://semver.org/) and uses
[PEP 440](https://peps.python.org/pep-0440/) version syntax.

The v2 package is not publishable until the maintainer resolves the licensing
and compact-model provenance blockers in `LICENSING.md`.

## Unreleased

### Added

- Typed, dependency-free v2 parsing API and JSON/JSONL command-line interface.
- Lazy `parse_iter()` batches, predictable batch input errors, and public
  typed-dictionary serialization schemas.
- Conservative `detect_addresses()` message-span detection with typed,
  offset-preserving results and a positive/negative behavior fixture.
- Compact bundled sequence model for residual unmarked text.
- Independent evaluation reports with explicit metric-family names and release
  regression gates; legacy report keys remain compatibility aliases.
- A committed results index, full historical benchmark report, and explicit
  documentation of the hybrid rules/structured-perceptron runtime stack.
- A complete 500-row diagnostic CSV with per-field outcomes, scenario columns,
  triage hypotheses, and a representative failure summary.
- Distribution inspection, isolated-wheel smoke tests, size limits, and
  artifact checksum manifests.

### Changed

- Source distributions now exclude evaluation corpora, training inputs, tests,
  notebooks, caches, and historical v1 assets.
- CI covers every declared Python minor version from 3.10 through 3.14.

## 2.0.0a1 - Unreleased

Initial v2 alpha. This version has not been published to PyPI.
