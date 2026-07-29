# Evaluation

This directory measures two separate tasks:

1. **address parsing** after an address string or oracle-cropped window is
   already available;
2. **address detection** of half-open address spans inside a free-form message.

Do not use parsing scores as evidence that the package can find addresses in
arbitrary prose. Do not use the small detection fixture as a production
accuracy claim.

## What parsing accuracy currently means

The historical regression requires case-insensitive exact component values
after whitespace and `ё/е` folding. It reports:

- exact-address rate: every public component value matches on one row;
- no-unparsed rate: no residual word or number spans remain;
- exact component-value micro precision, recall, and F1;
- the same exact-value metrics per public field.

RedMadRobot instead uses one-to-one same-label span overlap on gold-cropped
address windows. Deepparse reports binary span overlap, character overlap,
token labels, and exact complete sequences. Moscow uses exact component values.
These metrics and domains are intentionally not interchangeable.

## Historical 500-row regression

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

This is the portable release gate: it uses only the committed 500-row fixture,
requires no download, and runs as part of the normal test suite. The report
names its exact-value aggregate `exact_component_value_micro`; the legacy
`micro` key remains as a compatibility alias for existing gate files.

The `2.0.0a1` baseline is:

- 500 rows;
- 80.4% exact-address match;
- 95.9% micro field F1;
- 76.8% with no residual word or number tokens.

The evaluator reports precision, recall, and F1 for every public field and keeps
a bounded failure sample. Gate thresholds are intentionally just below the
measured deterministic baseline: they prevent regressions but do not establish
production accuracy.

### Row-level failure diagnostics

Generate the complete 500-row diagnostic table and summary:

```bash
python evaluation/analyze_failures.py
```

[`legacy_reference_500_diagnostics.csv`](legacy_reference_500_diagnostics.csv)
contains one row per test example and explicit columns for:

- expected, actual, and match/missing/extra/wrong status for every field;
- mismatch, missing, extra, and wrong-value field lists;
- parser confidence, warnings, alternatives, and unparsed spans;
- punctuation, Unicode whitespace, marker position, administrative, unit,
  compound-number, numeric-sequence, ordinal, and repeated-city scenarios;
- triage priority, failure types, likely causes, and a readable summary.

The likely-cause fields are deterministic hypotheses for triage. They have not
been independently human-verified and must not be presented as causal ground
truth. The current summary contains 402 exact rows and 98 non-exact rows.
Among those failures, 61 involve `street_type`, 25 `house_num`, 21 `apartment`,
and 20 `street`; one row can contribute to several counts.

The primary heuristic cause partitions all 98 rows:

| Primary likely cause | Rows | Interpretation |
| --- | ---: | --- |
| Conflicting street markers | 22 | More than one type marker competes |
| Reference infers absent street type | 20 | Expected type is not explicit in raw text |
| Ambiguous/unsupported abbreviation | 15 | `пр.`, `с.`, `ком.`, or a typo needs review |
| Unmarked numeric-role ambiguity | 14 | Bare numbers can be house/corpus/unit |
| Compound or letter-number boundary | 7 | Slash, hyphen, or letter suffix is split |
| Administrative label/boundary | 6 | Adjacent administrative values merge or shift |
| Street-type recognition | 4 | A visible supported-looking marker is missed |
| Reference conflicts with numeric marker | 3 | Raw `стр.` conflicts with expected `house_num` |
| Numeric component not recognized | 3 | House/unit evidence is missed |
| Four single-row causes | 4 | Label confusion, reference conflict, or span/extra field |

Additional likely-cause tags intentionally overlap—for example, an unmarked
numeric row can also contain label confusion and a missing apartment. Both the
primary partition and all secondary tags are retained in the CSV.

[`FAILURE_ANALYSIS.md`](FAILURE_ANALYSIS.md) walks through ten representative
rows and explains why at least 24 failures require reference adjudication before
parser optimization.

## Free-form message detection

`detection_reference.jsonl` contains 30 deliberately narrow positive and
negative messages. Every row records the message, exact expected substrings,
scenario family, context style, address style, boundary style, polarity,
ambiguity, and notes.

Run:

```bash
python evaluation/evaluate_detection.py
```

The current conservative detector exactly matches all 20 annotated address
spans and returns no span for all 12 negative messages. That is 100% on this
small authored regression fixture only. It is not independent or large enough
for an accuracy claim. The next meaningful detector benchmark should annotate
complete, representative messages—including hard negatives—without
oracle-cropping.

An additional diagnostic runs the detector on all 2,841 complete reconstructed
RedMadRobot messages rather than gold-cropped snippets:

```bash
python evaluation/evaluate_redmadrobot_detection.py \
  --data .cache/external/redmadrobot-pii-benchmark-f77ea831.csv
```

Only gold clusters containing both `STREET` and `HOUSE` are address positives.
The current development snapshot has 144 such gold spans in 135 messages:

| Detection metric | Result |
| --- | ---: |
| Any-overlap precision | 98.0% |
| Any-overlap recall | 68.1% |
| Any-overlap F1 | 80.3% |
| Exact-boundary F1 | 23.8% |
| Negative-message specificity | 100.0% |

Exact-boundary scoring is much lower because the BIO gold span and detector
have different boundary policies—for example, one may include a city or
country while the other returns the parseable street/building/unit substring.
The complete diagnostic contains 107 non-exact messages: 44 missed-address,
33 context-inclusion, 33 dropped-gold-text, and one spurious-address tag.
Tags overlap.

These failures were inspected while developing the detector, so this
RedMadRobot detection report is now a development diagnostic, not a sealed
final test. A production claim needs a new untouched message-level benchmark
whose annotation policy explicitly defines optional city, postcode, country,
person-name, and trailing-unit boundaries.

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

The report uses explicit metric-family keys:

- `span_overlap_micro`: binary same-label span matching with any overlap;
- `character_overlap_micro`: overlapping-character precision, recall, and F1;
- `token_label_micro`: aligned source-token label precision, recall, and F1;
- `exact_address_rate`: exact labels and exact span boundaries for a full row;
- `exact_token_sequence_rate`: exact complete token-label sequence.

The original `micro`, `character_micro`, and `token_micro` names remain
compatibility aliases. Each generated report includes `metric_definitions`;
do not compare or average values from different metric families.

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

The exact-value aggregate is named `exact_component_value_micro`; `micro`
remains a compatibility alias for the first published report schema.

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
