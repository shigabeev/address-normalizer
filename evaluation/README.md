# Evaluation

This directory contains an extraction-only legacy regression benchmark.

`legacy_reference_500.jsonl` is a deterministic SHA-256 selection of 500 unique
rows from the `Good` worksheet in `ref/references.xlsx`. A row is eligible only
when:

- the street and house are present;
- supported administrative fields appear as exact token sequences in the source
  address;
- house, корпус/строение, and apartment values fit the parser's numeric schema;
- the legacy hierarchy can be represented by the v2 API.

This filtering prevents corrected FIAS values from being scored as if the
parse-only package were expected to correct or resolve them.

The workbook was already published in the historical repository as a reference
sample. The selected rows have **not** been independently re-reviewed during the
v2 work, are not bundled in the wheel, and are not model-training data. Describe
this as a legacy reference or silver benchmark—not a new gold dataset.

Run the alpha regression gate:

```bash
python evaluation/evaluate.py \
  --data evaluation/legacy_reference_500.jsonl \
  --gates evaluation/release_gates.json
```

The `2.0.0a1` baseline is:

- 500 rows;
- 78.8% exact-address match;
- 94.9% micro field F1;
- 77.4% with no residual word or number tokens.

The evaluator reports precision, recall, and F1 for every public field and keeps
a bounded failure sample. Gate thresholds are intentionally just below the
measured deterministic baseline: they prevent regressions but do not establish
production accuracy.

## Promoting this to a gold benchmark

Before making a production-quality claim:

1. confirm that the legacy workbook may be retained and used for evaluation;
2. have a person review at least 300 rows against the raw text and v2 schema;
3. record reviewer, decision, notes, and review date;
4. exclude corrected registry values that do not occur in the raw input;
5. group variations of one canonical address into the same data split;
6. keep a sealed test split that is never used to tune rules or the model;
7. publish field metrics, confidence intervals, slice failures, and limitations;
8. do not distribute address rows unless their provenance permits it.

The existing reference is valuable enough to drive engineering now, but the
remaining human review is a release-management task, not something automation
should silently pretend to have completed.
