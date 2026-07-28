# address-normalizer

A small, offline parser for unstructured Russian addresses.

```python
from address_normalizer import parse

address = parse("ополченская 5-30")

assert address.street.value == "Ополченская"
assert address.house_num.value == "5"
assert address.apartment.value == "30"
```

Version 2 is a parser, not a FIAS/GAR resolver. It extracts components so an
application can query its own registry, database, search engine, or service.

## Why version 2

The original project was developed for a bank in 2017 and published with
permission. It standardized addresses by materializing FIAS paths in
Elasticsearch. That prototype worked, but required tens of gigabytes of source
data and potentially more than 100 GB of Elasticsearch storage.

Version 2 draws a smaller product boundary:

- no Elasticsearch;
- no bundled FIAS/GAR database;
- no network calls or first-run downloads;
- no service process;
- no pandas or scientific Python runtime;
- an offset-preserving typed API;
- deterministic numeric rules plus a tiny structured sequence model.

The historical v1 files remain in the repository as an architectural record but
are not imported by the `address_normalizer` package.

## Installation

From a checkout:

```bash
python -m pip install .
```

The first PyPI alpha will use:

```bash
python -m pip install address-normalizer
```

## API

```python
from address_normalizer import parse, parse_many

result = parse("г. Москва, ул. Тверская, д.4, кв.12")

print(result.normalized)
# Москва, ул Тверская, д 4, кв 12

print(result.as_dict())
```

Each extracted `AddressPart` contains:

- `value`: lightly normalized value;
- `raw`: exact source substring;
- `span`: original `[start, end)` character offsets;
- `confidence`: bounded decision-strength score;
- `source`: `rule`, `model`, `postprocessor`, or `unparsed`.

The result can contain:

```text
postal_code, region, district, city, settlement,
street, street_type, house_num, corpus, structure, apartment,
unparsed, warnings, alternatives, confidence
```

Confidence values in the alpha are decision-strength scores, not calibrated
probabilities.

### Ambiguous numeric tails

Legacy address data often uses `house-apartment` notation:

```python
result = parse("Ополченская 5-30")

result.house_num.value        # "5"
result.apartment.value        # "30"
result.warnings               # ("ambiguous_numeric_tail",)
result.alternatives[0]        # compound house interpretation: "5-30"
```

The parser preserves the alternative because a customer-managed FIAS/GAR lookup
is better positioned to disambiguate it.

## Command line

```bash
address-normalizer "СПб Невский проспект 10 корп 2 кв 15"
```

For batch pipelines:

```bash
printf '%s\n' \
  'Ополченская 5-30' \
  'Самара Авроры 7 12' |
  address-normalizer --jsonl
```

## Architecture

```text
source text
  -> offset-preserving tokenizer
  -> deterministic postcode and numeric grammar
  -> explicit address-marker extraction
  -> compact dependency-free linear-chain tagger
  -> deterministic postprocessing and alternatives
  -> ParsedAddress
```

The bundled model is deliberately small and only classifies residual unmarked
text. High-confidence numeric syntax remains deterministic.

The current 37 KB model is trained from source-verifiable legacy reference rows
using canonical-address-grouped train, validation, and test splits. On the
untuned sequence test it reaches 96.5% token accuracy and 94.3% complete-sequence
accuracy. The test lacks meaningful district and settlement coverage and the
source provenance still requires confirmation, so this is not evidence of
nationwide production accuracy. See `training/README.md`.

## Evaluation

The v2 alpha includes a fixed 500-row extraction-only regression benchmark
derived from the historical `Good` reference worksheet:

```bash
python evaluation/evaluate.py \
  --data evaluation/legacy_reference_500.jsonl \
  --gates evaluation/release_gates.json
```

The current baseline is 80.4% exact-address match and 95.9% micro field F1. The
source rows have not been independently re-reviewed for v2, so these are
engineering regression numbers—not a production or nationwide accuracy claim.

On the independent RedMadRobot Russian PII benchmark, the first untuned result
is 58.7% micro span F1 across every address window, 73.6% on windows with at
least two distinct fields, and 78.8% when both street and house are present.
This external result is the more useful estimate of present generalization:
house parsing is strong, while administrative recall and street precision need
work. See `evaluation/README.md` for the pinned data source, scoring boundary,
per-field metrics, and failure sample.

Two much larger clean-address benchmarks now make the reliability boundary more
concrete:

- 100,000 group-disjoint Russian test addresses from Deepparse produce 66.2%
  character-overlap F1 (84.4% binary span-overlap F1). Street detection is
  strong, while administrative fields remain weak.
- 15,196 group-disjoint active official Moscow registry addresses produce 85.4%
  exact component-value F1 and 64.9% exact full-address match. House, корпус,
  and строение exceed 96.9% field F1; exact street extraction is 66.2%.

These scores are deliberately not averaged. Deepparse is nationwide but
registry-derived and uses a different token schema; the Moscow snapshot is
official and exact-value scored but clean, Moscow-only, and from October 2021.
The full external data stays in `.cache/external/`, never in the wheel. See
`evaluation/README.md` for reproducible downloads, checksums, filters, and
reports.

## What this package does not do

It does not:

- verify that an address exists;
- return a FIAS/GAR identifier;
- geocode;
- correct a street to its official spelling;
- bundle or update a registry;
- guarantee that every ambiguous number was interpreted correctly.

Use the structured output to query a customer-managed FIAS/GAR index. Integration
recipes can live independently from the lightweight parser.

## Development

```bash
python -m pip install -e .
pytest
python training/train_compact_tagger.py
python training/evaluate_compact_tagger.py
```

Large external data preparation has separate, pinned tooling:

```bash
python -m pip install -r requirements-evaluation.txt
python evaluation/prepare_deepparse.py --download
python evaluation/prepare_datamos.py --download
```

These dependencies and datasets are not installed with the runtime package.

Build artifacts:

```bash
python -m pip install build
python -m build
```

The sprint quality gates include golden parsing examples, original-span checks,
model loading, deterministic training, forbidden-dependency scanning, and a
package-data size limit.

## Legacy v1

The following root files belong to the original Elasticsearch implementation:

- `api.py`
- `parsing.py`
- `upload_fias.py`
- `docker-compose.yaml`
- `requirements-legacy.txt`

They are preserved for historical reference and are not part of the v2 wheel.

## Contributing

Bug reports and narrowly scoped evaluation examples are welcome. Read
[CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request. Substantive
reusable code contributions should wait until the v2 license is selected.
AI-assisted contributions are acceptable after that point, but the human author
remains responsible for understanding, testing, and supporting the change.

## License

A license has not yet been selected for version 2. The repository has historical
third-party contributions, while permission to publish does not necessarily
grant authority to relicense them. Read `LICENSING.md` and confirm the scope of
the original publication permission before publishing the PyPI release or
accepting reusable third-party contributions.
