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
- Proposed use: a deterministic 10,000-row evaluation sample, never training

This is the best next scale benchmark. It should report token/span metrics
separately from the RedMadRobot real-input-shape benchmark rather than combining
the two scores.

## 3. Moscow official address registry

- Source documentation:
  <https://data.apicrafter.ru/tables/datamos/addressreestr/docs>
- Published origin: Moscow open-data portal
- Size reported by the catalog: 440,399 records, approximately 766 MB
- Fields include full and simplified address strings plus region, city,
  settlement, street/road element, house, корпус, строение, room, and FIAS ID
- Strength: official structured truth and building-level identifiers
- Limitation: Moscow-only, clean legal formatting, and bulk access/redistribution
  terms must be verified from the original publisher before integration
- Proposed use: a 10,000-row clean official-address slice after provenance review

## Evaluation policy

Keep the three domains separate:

1. historical bank-shaped regression data for compatibility;
2. RedMadRobot address windows for external noisy-input generalization;
3. Deepparse or official registries for broad clean-address coverage.

Never average them into one headline accuracy number. Publish per-field metrics
and domain-specific slices, and reserve a new untouched dataset before changing
rules or features in response to observed external failures.
