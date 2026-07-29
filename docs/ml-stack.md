# ML stack and runtime boundaries

## Short answer

`address-normalizer` is a hybrid rules-and-ML extractor. It is not a
transformer, neural network, LLM, or registry-backed parser.

The only learned runtime component is a 37,130-byte sparse linear-chain
sequence tagger. It labels residual word tokens after deterministic syntax has
already extracted explicit address components. Message-level address detection
is currently rule-based.

The package has no runtime dependencies outside the Python standard library and
makes no network calls.

## Runtime pipeline

| Stage | Implementation | Learned? | Responsibility |
| --- | --- | --- | --- |
| Message detection | Regular expressions, clause boundaries, evidence scoring, parser validation | No | Find conservative street-and-building candidate spans in prose |
| Tokenization | Offset-preserving regular-expression tokenizer | No | Split words, numbers, and punctuation without losing source offsets |
| Explicit extraction | Marker dictionaries and numeric grammars | No | Extract postcode, region/city/street markers, house, корпус, строение, and apartment |
| Residual labeling | Sparse linear-chain sequence tagger with Viterbi decoding | Yes | Label remaining unmarked words as region, district, city, settlement, street, or other |
| Post-processing | Deterministic boundary and numeric-tail heuristics | No | Fill plausible implicit street/house fields and retain warnings or alternatives |
| Registry resolution | Not part of this package | N/A | Verify existence, resolve abbreviations in context, choose a FIAS/GAR object, and return canonical values |

Direct address strings enter at tokenization. Free-form messages first pass
through detection; every accepted detection is then parsed by the same address
parser.

## The learned model

The runtime model is an epoch-averaged structured perceptron:

- labels: `O`, `REGION`, `DISTRICT`, `CITY`, `SETTLEMENT`, `STREET`;
- representation: sparse emission and transition weights serialized as JSON;
- decoding: first-order Viterbi sequence decoding;
- token features: lowercased word, word/number/punctuation kind, one- and
  two-character prefixes, one- to three-character suffixes, sequence position,
  neighboring token text and kind, and digit length;
- artifact: `src/address_normalizer/data/model.json`;
- artifact size: 37,130 bytes;
- stored parameters: 1,240 emission weights and 39 transition weights;
- training algorithm and inference: Python standard library only.

This is closest in spirit to a small CRF-style linear sequence model, but it is
trained with a structured perceptron objective. It does not calculate neural
embeddings, use pretrained language representations, or produce calibrated
probabilities.

The model only sees residual word tokens that rules have not consumed. It
therefore does not learn house-number grammar, address detection in prose, or
registry identity.

## Training data and split

The current model corpus is derived from the 500-row historical reference:

- 429 sequence examples from 108 canonical address groups;
- deterministic 70/15/15 group split using SHA-256;
- 301 training, 58 validation, and 70 test sequence examples;
- epoch count selected on validation only;
- final artifact retrained on the 359 train-plus-validation examples;
- 10 selected epochs and seed `2017`.

The group split prevents variants of the same canonical administrative/street
identity from appearing across train and test. The final 70-example sequence
test has 115 tokens, so its 96.5% token accuracy and 96.9% entity F1 are useful
regression evidence but not a production-scale claim. It contains no
independently measured district or settlement tokens.

The much larger Deepparse and Moscow datasets are evaluation sources and
potential future training material. They are not used by the current bundled
model.

## What FIAS/GAR changes

Text extraction and registry resolution are different tasks.

The text `пр. Ленина` is locally ambiguous because `пр.` can abbreviate more
than one street type. A full location and building can nevertheless identify
one registry object. Likewise, whether `с. 1` or `4А` is a building/unit role
may be resolved by the set of valid objects at the rest of the address.

The intended production boundary is therefore:

1. detect an inclusive address span in a message;
2. extract source-faithful component candidates and offsets;
3. query a current FIAS/GAR index with all available context;
4. rank registry candidates and return the canonical object;
5. retain the original text and parser warnings for auditability.

The parser should not be trained to fabricate a registry-backed value that is
absent from the text merely to match a historical reference. The resolver can
enrich or correct the extracted candidate because it has the missing registry
knowledge.

## Current production limitation

The message detector is deliberately high-precision and rule-based. Its
complete-message RedMadRobot diagnostic has 98.0% overlap precision but only
68.1% recall. That makes it suitable when false positives are expensive, but
not yet sufficient when production requires finding most implicit, malformed,
or markerless addresses.

A higher-recall production system should add a separately trained
message-level address-span model on representative messages, while keeping
parsing and FIAS/GAR resolution as distinct measured stages:

```text
message -> address-span detector -> component parser -> FIAS/GAR resolver
```

Each stage should have its own metric: span recall/precision, conditional
component accuracy, resolver top-k recall, and end-to-end business success.

All committed benchmark outputs and their precise scopes are indexed in
[`evaluation/RESULTS.md`](../evaluation/RESULTS.md).
