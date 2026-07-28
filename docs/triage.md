# Issue and pull-request triage

This guide makes review predictable without pretending maintainers have an SLA.
It is guidance for humans; no labels or repository settings are applied by
automation.

## First response

For each new issue:

1. remove public secrets or personal address data from view and follow the
   security/privacy route;
2. confirm it concerns v2 rather than the unsupported historical v1 service;
3. request the exact version, minimal command/input, current output, and expected
   result if missing;
4. reproduce before labeling a parser behavior as a confirmed bug;
5. identify the domain: synthetic, noisy/user-entered, clean registry,
   historical compatibility, or unknown;
6. check for a duplicate and link the canonical issue;
7. keep proposed implementation separate from the user problem.

Parsing failures should preserve punctuation, Unicode, output spans, warnings,
alternatives, and unparsed content. Prefer synthetic minimal cases; never ask
for a private corpus dump in a public issue.

## Suggested labels

| Label | Suggested color | Use |
| --- | --- | --- |
| `needs-triage` | `D4C5F9` | Reproduction or ownership has not been established |
| `bug` | `D73A4A` | Confirmed behavior contradicts the documented contract |
| `parsing` | `B60205` | Component, span, warning, or ambiguity behavior |
| `cli` | `1D76DB` | Command-line and JSONL behavior |
| `api` | `0052CC` | Public Python API or serialized result |
| `documentation` | `0075CA` | User or contributor documentation |
| `evaluation` | `5319E7` | Metrics, gates, adapters, or benchmark reports |
| `data` | `7057FF` | Dataset or generated-data concern |
| `provenance` | `8A2BE2` | Source, terms, attribution, or redistribution evidence |
| `packaging` | `0E8A16` | Build, wheel, install, metadata, or compatibility |
| `security` | `B60205` | Public tracking only after private disclosure is safe |
| `good first issue` | `7057FF` | Bounded task with exact acceptance criteria and mentor |
| `help wanted` | `008672` | Maintainer has defined scope and will review work |
| `blocked: licensing` | `000000` | Reusable contribution cannot proceed before license decision |
| `blocked: decision` | `FBCA04` | Explicit maintainer/product choice is required |
| `needs-reproduction` | `FEF2C0` | Report lacks a locally repeatable case |
| `needs-provenance` | `F9D0C4` | Data/model source or permission evidence is incomplete |
| `duplicate` | `CFD3D7` | Canonical issue is linked before closing |
| `wontfix` | `FFFFFF` | Intentionally outside scope, with reason documented |

Do not use `good first issue` on vague refactors, broad parser improvement,
model retraining, release workflows, or tasks blocked by an unstated design
decision.

## Parsing failure disposition

- **Confirmed regression:** add `bug` and `parsing`; record the last known good
  version if known.
- **Known limitation:** link the README limitation, retain the example if it
  adds a meaningful input class, and avoid promising a fix.
- **Resolver responsibility:** explain that extraction does not verify existence
  or choose a FIAS/GAR ID.
- **Ambiguous ground truth:** preserve alternatives; do not force a single label
  merely to close the issue.
- **Private or licensed data:** remove it from public view and ask for a
  synthetic reproduction.

A public failure example should become a regression test only after provenance,
privacy, expected spans, and the licensing/contribution policy allow it.

## Pull-request review

Close or defer substantive reusable pull requests while the licensing pause in
`LICENSING.md` remains. For eligible changes, require:

- a linked issue or complete reproduced bug;
- a test that demonstrates failure before and success afterward;
- the author's own explanation of behavior and maintenance implications;
- exact focused and full-suite results;
- before/after metrics for every relevant benchmark domain;
- model and wheel size deltas where relevant;
- preservation of offsets, ambiguity, warnings, alternatives, and unparsed
  evidence;
- no hidden network, registry, service, data, or runtime-dependency expansion.

An unexplained generated patch is not reviewable evidence. Ask the author to
reduce it and explain it; do not reverse-engineer a bulk submission on their
behalf.

## Benchmark-resistant review

Reject or redesign a change when it:

- tunes against a sealed test while continuing to describe it as untouched;
- improves only a lenient overlap metric while exact value or another field
  regresses;
- averages historical, noisy, nationwide-clean, and Moscow-clean domains;
- discards unparsed text or ambiguity to manufacture a cleaner score;
- moves data-preparation dependencies into the runtime;
- increases model/package size without a measured, justified tradeoff.

Require a validation-driven decision and report every measured domain
separately, including rejected regressions.

## Closing language

Be direct about scope. A useful close explains which documented boundary applies,
links the canonical issue or recipe, and states what new evidence would justify
reopening. “Not planned” without an explanation is not sufficient.
