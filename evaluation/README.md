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

## Large external corpora

Install the data-only tools in a separate environment:

```bash
python -m venv .venv-evaluation
.venv-evaluation/bin/python -m pip install -r requirements-evaluation.txt
```

The runtime wheel remains dependency-free. Raw and derived files are written to
the ignored `.cache/external/` directory.

### Nationwide clean addresses: Deepparse

Prepare the complete pinned Russian shard:

```bash
.venv-evaluation/bin/python evaluation/prepare_deepparse.py \
  --download --overwrite
```

The streaming pipeline verifies the 371,595,309-byte source by SHA-256, checks
all 13,152,918 token/tag sequences, filters administrative-only strings,
deduplicates normalized text, maps the external tags to package fields, and
assigns canonical building groups to deterministic 90/5/5 splits. The result is:

- 6,314,158 unique usable rows;
- 5,681,842 train, 316,586 validation, and 315,730 test rows;
- 5,293,689 street-and-house rows, including 679,076 with a unit;
- a 557,321,339-byte labeled Parquet corpus;
- a deterministic 100,000-row compressed JSONL sample from test only.

Run the large test:

```bash
python evaluation/evaluate_deepparse.py \
  --output evaluation/deepparse_report.json
```

The initial untuned 100,000-row result is:

| Measure | Result |
| --- | ---: |
| Binary same-label span-overlap F1 | 84.4% |
| Character-overlap F1 | 66.2% |
| Token-label F1 | 66.5% |
| Exact token-boundary sequence | 8.0% |
| Throughput | 7,245 rows/s |

Binary span overlap is intentionally lenient: any overlapping same-label span is
a match. Character and token metrics expose partial values and merged spans, but
also penalize schema-boundary differences such as the source labeling `дом 12`
as one entity while the package returns the value `12`. Publish all three, not
only the largest number.

Character-overlap F1 by field is 99.9% postcode, 22.3% region, 22.6% district,
52.2% city, 76.7% street, 59.0% house, and 47.1% apartment. The set is national
in scale but consists of curated open-geographic addresses rather than noisy
user input.

### Official clean addresses: Moscow registry

Prepare the pinned October 2021 city snapshot:

```bash
.venv-evaluation/bin/python evaluation/prepare_datamos.py \
  --download --overwrite
```

The filter retains only addresses that are on Moscow territory, official,
registered in the address registry, present in GKN, have a valid FIAS UUID, and
contain structured street and house values. It removes normalized duplicates
and groups street/house/корпус/строение identities before splitting.

The result contains 307,274 unique active official addresses: 276,368 train,
15,710 validation, and 15,196 test. The portable filtered artifact is
26,050,795 bytes compressed.

Run exact-value evaluation:

```bash
python evaluation/evaluate_datamos.py \
  --output evaluation/datamos_report.json
```

| Measure | Result |
| --- | ---: |
| Exact component-value micro F1 | 85.4% |
| Exact full-address match | 64.9% |
| Street F1 | 66.2% |
| House F1 | 98.2% |
| Корпус F1 | 99.6% |
| Строение F1 | 97.0% |

This is the strongest current clean-building benchmark because it scores exact
structured values rather than mere span overlap. It is still not a production
claim: the snapshot is old, Moscow-only, and legally formatted.

The archive embeds the original portal dataset ID, publisher, version, source
URL, and Russian government open-data terms. The mirror describes the package
as CC-BY-SA. Keep both records and confirm redistribution terms before
publishing derived rows.

## Current evidence, kept separate

| Domain | Test size | Primary measure | Baseline |
| --- | ---: | --- | ---: |
| Historical bank-shaped reference | 500 | exact field micro F1 | 95.9% |
| RedMadRobot noisy address windows | 578 | binary span-overlap F1 | 58.7% |
| Deepparse nationwide clean strings | 100,000 | character-overlap F1 | 66.2% |
| Moscow official clean buildings | 15,196 | exact component-value F1 | 85.4% |

These numbers answer different questions and must not be averaged into one
“accuracy” claim.

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
