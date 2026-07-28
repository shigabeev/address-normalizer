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
v2 work and are not bundled in the wheel. A canonical-grouped subset now trains
the compact tagger, while disjoint groups are reserved for validation and test.
Describe the data as a legacy reference or silver corpus—not a new gold dataset.

Run the alpha regression gate:

```bash
python evaluation/evaluate.py \
  --data evaluation/legacy_reference_500.jsonl \
  --gates evaluation/release_gates.json
```

The `2.0.0a1` baseline is:

- 500 rows;
- 80.4% exact-address match;
- 95.9% micro field F1;
- 76.8% with no residual word or number tokens.

The evaluator reports precision, recall, and F1 for every public field and keeps
a bounded failure sample. Gate thresholds are intentionally just below the
measured deterministic baseline: they prevent regressions but do not establish
production accuracy.

## Independent external benchmark

The repository also includes an adapter for
[RedMadRobot's MIT-licensed Russian PII NER benchmark](https://huggingface.co/datasets/redmadrobot-rnd/pii_benchmark).
Its 2,841 manually BIO-annotated sentences combine
production-log-shaped inputs (with real personal values replaced), synthetic
document-style examples, and manually filtered hard negatives. The external
data is pinned by Git revision and SHA-256 but is not committed or used for
training.

Run the external evaluation once with:

```bash
python evaluation/evaluate_redmadrobot.py --download \
  --output evaluation/redmadrobot_report.json
```

Subsequent runs can omit `--download`. The adapter extracts minimal address
windows from the gold BIO annotations and scores one-to-one, same-label span
overlap for `REGION`, `DISTRICT`, `CITY`, `STREET`, and `HOUSE`. This measures
address parsing after an address window has already been identified; it is not
an address-in-arbitrary-text detection score. `COUNTRY` is retained as context
but is not scored because it is not currently a public parser field.

Treat this set as sealed evaluation data: do not train on it, tune thresholds
against it, or turn its failures into model features without replacing it with a
new untouched final test.

The first untuned baseline covers 1,010 address spans in 578 snippets from 493
source rows:

| Slice | Snippets | Micro span F1 |
| --- | ---: | ---: |
| All address windows | 578 | 58.7% |
| Two or more distinct fields | 217 | 73.6% |
| Contains both street and house | 144 | 78.8% |
| Administrative fields only | 403 | 41.7% |

Per-field F1 on all windows is 52.4% region, 44.5% district, 59.8% city, 49.1%
street, and 90.2% house. The large difference from the legacy regression is
important evidence: the parser is useful for conventional street-and-house
inputs, but it currently defaults too readily to `STREET` on isolated
administrative names and has weak administrative recall.

## Promoting this to a gold benchmark

Before making a production-quality claim:

1. confirm that the legacy workbook may be retained and used for evaluation;
2. have a person review at least 300 rows against the raw text and v2 schema;
3. record reviewer, decision, notes, and review date;
4. exclude corrected registry values that do not occur in the raw input;
5. group variations of one canonical address into the same data split;
6. keep a final test split that is never used to tune rules or the model;
7. publish field metrics, confidence intervals, slice failures, and limitations;
8. do not distribute address rows unless their provenance permits it.

The existing reference is valuable enough to drive engineering now, but the
remaining human review is a release-management task, not something automation
should silently pretend to have completed.
