# External data sources

These sources complement the historical workbook. None should be added to model
training until its role, license, and split policy are recorded.

## 1. RedMadRobot Russian PII NER benchmark

- Source: <https://huggingface.co/datasets/redmadrobot-rnd/pii_benchmark>
- License: MIT
- Size: 2,841 sentences, including 1,252 location/address entity spans
- Useful subset: 493 rows, 578 address windows, 1,010 fields supported by this
  parser
- Strength: manually BIO-annotated, production-log-shaped inputs, hard negatives
- Limitation: personal values are replaced; some examples are synthetic
  document templates; the adapter uses gold annotations to crop address windows
- Status: integrated as an independent external regression benchmark

The exact revision and file SHA-256 are pinned in
`evaluate_redmadrobot.py`. The source CSV is downloaded into `.cache/` and is
not committed or used for training.

## 2. Deepparse worldwide addresses, Russia configuration

- Source: <https://huggingface.co/datasets/deepparse/worldwide-addresses>
- License: CC BY 4.0
- Pinned candidate revision: `cb61e5e49db87f8c3586b5494149f612460f8992`
- Russian shard: 13,152,918 annotated addresses; 371,595,309-byte Parquet file
- Fields: street number/name, unit, municipality, district, county, province,
  postal code, and country
- Strength: nationwide scale and an independently defined token schema
- Limitation: curated from libpostal/open geographic data rather than raw user
  input; punctuation is removed; there is no predefined sealed split
- Integrated filter: retain street/house/unit-bearing rows, validate all token
  labels, deduplicate normalized text, map labels to package fields, and group
  building identities before deterministic 90/5/5 splitting
- Result: 6,314,158 unique usable rows and a 100,000-row sealed test sample
- Status: integrated as a reproducible external corpus and clean-address
  benchmark; the current compact model has not been trained on it

The committed manifest pins the source and generated artifact checksums. Report
binary span, character-overlap, and token metrics separately from the
RedMadRobot real-input-shape benchmark.

## 3. Moscow official address registry

- Source package:
  <https://data2.apicrafter.ru/packages/datamos-addressreestr>
- Published origin: Moscow open-data portal, dataset ID 60562, Department of
  City Property
- Pinned snapshot: version 3.630, released 15 October 2021
- Size: 440,399 records; 121,581,813-byte download archive
- Fields include full and simplified address strings plus region, city,
  settlement, street/road element, house, корпус, строение, room, and FIAS ID
- Strength: official structured truth and building-level identifiers
- Integrated filter: retain active official Moscow/GKN records with valid FIAS
  UUID, street, and house; deduplicate normalized simplified addresses; group
  building identities before deterministic 90/5/5 splitting
- Result: 307,274 unique usable rows and 15,196 exact-value test rows
- Limitation: Moscow-only, clean legal formatting, and a stale 2021 snapshot
- Terms: the archive embeds Russian government open-data terms; the mirror
  describes the package as CC-BY-SA. Confirm redistribution before publishing
  derived rows.
- Status: integrated as a reproducible historical official-address benchmark

## Evaluation policy

Keep the three domains separate:

1. historical bank-shaped regression data for compatibility;
2. RedMadRobot address windows for external noisy-input generalization;
3. Deepparse or official registries for broad clean-address coverage.

Never average them into one headline accuracy number. Publish per-field metrics
and domain-specific slices, and reserve a new untouched dataset before changing
rules or features in response to observed external failures.

## Local artifacts

All raw and derived data stays under `.cache/external/` and is ignored by Git.
The runtime wheel contains none of it. The committed `deepparse_manifest.json`
and `datamos_manifest.json` record exact filters, counts, revisions, checksums,
split policies, and limitations; the matching report JSON files record the
first untuned scores.
