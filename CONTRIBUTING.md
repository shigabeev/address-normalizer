# Contributing

Thank you for helping make Russian address parsing easier to inspect and trust.
Useful contributions here are usually small: one clearly reproduced behavior,
one bounded change, and evidence a maintainer can rerun.

## License

The project is licensed under GNU GPL v3 (`GPL-3.0-only`). By submitting a
contribution, you confirm that you have the right to provide it under that
license. Code, examples, generated models, and data-derived artifacts need
clear provenance; see [`LICENSING.md`](LICENSING.md).

## Choose the right report

- [Bug report](https://github.com/shigabeev/address-normalizer/issues/new?template=bug-report.yml):
  installation, API, CLI, packaging, or deterministic runtime failures.
- [Parsing failure](https://github.com/shigabeev/address-normalizer/issues/new?template=parsing-failure.yml):
  one address whose extracted fields, spans, warning, or alternative are
  unexpected.
- [Feature request](https://github.com/shigabeev/address-normalizer/issues/new?template=feature-request.yml):
  a user problem, not a preselected implementation.
- [Data provenance](https://github.com/shigabeev/address-normalizer/issues/new?template=data-provenance.yml):
  source, permission, redistribution, or benchmark-integrity information.
- Security-sensitive reports follow [`SECURITY.md`](SECURITY.md), never a public
  issue with exploit or private-address details.

Remove or replace personal data before posting. A synthetic address that
reproduces the behavior is preferable.

## Before a pull request

1. link an existing issue or provide a complete, locally reproducible bug;
2. agree on scope before public API, model, data, dependency, or workflow work;
3. keep the change narrowly focused and preserve unrelated behavior;
4. add a test that fails before the fix and passes afterward;
5. explain the behavior in your own words, including maintenance implications;
6. report before/after output and relevant benchmark domains;
7. run the checks below and include exact results in the pull request.

A benchmark delta alone is not a product improvement. Parser or model changes
must not trade away another domain, field, original offsets, ambiguity, or
unparsed evidence to improve an aggregate score.

## Local setup and checks

Runtime development needs no third-party package dependency:

```bash
python -m pip install -e .
pytest
```

For a parsing change, show the focused failing test first, then run the complete
suite. For example:

```bash
pytest tests_v2/test_api.py -q
pytest
```

For documentation examples:

```bash
python examples/basic.py
printf '%s\n' 'Ополченская 5-30' | python examples/jsonl_etl.py
python -m compileall -q examples
```

Model, evaluation, build, and release work has additional checks and provenance
requirements. Read
[`training/README.md`](https://github.com/shigabeev/address-normalizer/blob/master/training/README.md)
and
[`evaluation/README.md`](https://github.com/shigabeev/address-normalizer/blob/master/evaluation/README.md)
before starting it. Large
external data and its preparation dependencies must stay outside the runtime
package.

## Parsing-change evidence

Include:

- the smallest synthetic or redacted input that reproduces the problem;
- expected fields and `[start, end)` offsets;
- output before and after the change;
- a regression test;
- an explanation of warnings, alternatives, and unparsed content affected;
- results for every relevant committed regression gate.

Do not copy examples from a sealed test set into tests or tune against that set
while continuing to call it untouched. Never commit private addresses or
unreviewed production logs.

## Model or benchmark changes

Discuss these in an issue before implementation. A proposal must identify:

- source, publisher, version/revision, URL, and retrieval date;
- license or terms and whether redistribution is allowed;
- transformations and filters;
- split and deduplication policy, including leakage prevention;
- exact reproduction command and checksums;
- before/after per-domain and per-field metrics;
- model and wheel size deltas;
- known regressions and rejected alternatives.

Keep historical, noisy-window, nationwide clean-address, and official-registry
scores separate. Do not select only the friendliest metric or average
incompatible domains.

## Changes requiring maintainer agreement

Ask before changing:

- public API or serialized result shape;
- supported Python versions;
- confidence semantics or review policy;
- runtime dependencies;
- model, training data, or data preparation;
- FIAS/GAR integration inside the core package;
- CI, permissions, release, or publishing workflows;
- large generated or binary artifacts.

The core package must remain small, dependency-free at runtime, offline, and
independent of any bundled FIAS/GAR database or service.

## Human accountability and automated assistance

AI-assisted contributions can be reviewed. The human author must be able to:

- explain every behavior change and why the approach is maintainable;
- identify the test that proves the bug and fix;
- reproduce claimed benchmark results;
- answer review questions and support follow-up repairs;
- confirm that submitted code and data may be contributed.

Unexplained generated changes, benchmark-only optimizations, bulk formatting,
and changes whose author cannot maintain them will be closed.

## Review and triage

Maintainers use [`docs/triage.md`](docs/triage.md) for labels, duplicate handling,
security routing, benchmark evidence, and review boundaries. Bounded starter
proposals are in [`docs/good-first-issues.md`](docs/good-first-issues.md).
