# Contributing

Contributions should make the parser easier to trust, embed, or evaluate.

The v2 license has not yet been selected. Until it is, please open issues and
share minimal reproduction examples rather than submitting substantive reusable
code. This avoids creating additional ownership ambiguity.

## Before opening a pull request

- Link an existing issue or open one describing the behavior first.
- Keep the change narrowly scoped.
- Add a regression test for parsing changes.
- Preserve original character spans.
- Report before/after output for new examples.
- Do not add a runtime dependency without prior discussion.
- Run `pytest`.

AI-assisted contributions are welcome. The pull-request author must understand
the change, verify it, respond to review, and remain accountable for it.

## Changes requiring maintainer discussion

Discuss these before implementation:

- public API changes;
- model or training-data replacement;
- confidence semantics;
- runtime dependencies;
- release workflows;
- FIAS/GAR integration inside the core package;
- large generated or binary files.

Model changes must include the training command, data provenance, evaluation
delta, and model-size delta.

## Good first issues

Appropriate first contributions include:

- a synthetic failing address plus a regression test;
- malformed-input and numeric-grammar cases;
- typing and documentation improvements;
- packaging compatibility;
- integration recipes that do not affect the core runtime.

Generated busywork is not useful. A small, well-tested correction is.
