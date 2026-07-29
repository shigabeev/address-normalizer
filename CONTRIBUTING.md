# Contributing

Keep changes small and explain the user-visible problem they solve.

## Setup

```bash
python -m pip install -e .
python -m pip install pytest mypy
pytest
python -m mypy
```

Parser changes should include:

1. a minimal synthetic or redistributable failing input;
2. expected component values and source spans;
3. a regression test that fails before the fix;
4. the full test result and, when relevant, `python tools/benchmark.py --check`.

Do not hide ambiguity by discarding warnings, alternatives, or unparsed text.
Do not add runtime dependencies, network calls, or a registry database without
first discussing the product boundary in an issue.

Never publish private customer addresses in an issue or test. Contributors must
have the right to submit all code, data, and generated artifacts. Contributions
are licensed under GPL-3.0-only.
