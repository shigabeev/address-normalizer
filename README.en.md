# address-normalizer

A small offline parser for Russian addresses. It extracts typed fields while
preserving source substrings and offsets, supports Python 3.10+, and has no
runtime dependencies.

[Русская версия](https://github.com/shigabeev/address-normalizer#readme)

## Install

```bash
python -m pip install --pre address-normalizer
```

## Use

```python
from address_normalizer import parse

result = parse("г. Москва, ул. Тверская, д. 4, кв. 12")

print(result.city.value)       # Москва
print(result.street.value)     # Тверская
print(result.house_num.value)  # 4
print(result.apartment.value)  # 12
print(result.normalized)       # Москва, ул Тверская, д 4, кв 12
```

Every component includes its normalized `value`, exact source substring `raw`,
half-open `start:end` offsets, decision source, and `confidence`.
`result.as_dict()` returns a JSON-compatible dictionary.

Ambiguity remains visible:

```python
result = parse("Ополченская 5-30")

print(result.normalized)     # Ополченская, д 5, кв 30
print(result.warnings)       # ("ambiguous_numeric_tail",)
print(result.alternatives)   # includes compound house 5-30
```

`confidence` is internal decision strength, not the probability that an address
exists. Review results containing `warnings`, `alternatives`, or `unparsed`.

### Detect an address in a message

```python
from address_normalizer import detect_addresses

message = "Доставить по адресу: Москва, ул. Тверская, д. 13. Позвоните."

for item in detect_addresses(message):
    print(item.text)     # Москва, ул. Тверская, д. 13
    print(item.span)     # offsets in the original message
    print(item.parsed)   # ParsedAddress
```

Detection is conservative: weak candidates are intentionally rejected to avoid
treating dates or order numbers as addresses.

### Batches and CLI

```python
from address_normalizer import parse_many

results = parse_many(["Тверская 1", "Невский проспект 10"])
```

```bash
address-normalizer "СПб, Невский проспект 10, корп. 2"
printf '%s\n' "Тверская 1" "Ополченская 5-30" |
  address-normalizer --jsonl
```

## Scope

The package extracts postal and administrative fields, street and street type,
building/unit fields, source offsets, warnings, alternatives, and unparsed
content.

It does not validate addresses against FIAS/GAR, return registry IDs, correct
official spelling, or geocode. Pass extracted candidates to a current registry
resolver owned by your application.

## Implementation

The runtime is a straightforward hybrid: offset-preserving tokenization,
explicit address and numeric rules, a compact linear sequence tagger for
unmarked words, and deterministic post-processing. It is not an LLM or neural
network. The model is 37 KB; there are no network calls or hidden downloads.

## Quality

One headline “accuracy” would mix incompatible tasks, so benchmarks stay
separate:

| Dataset | Size | Metric | Result |
| --- | ---: | --- | ---: |
| Historical address sample | 500 | exact component micro F1 | 95.9% |
| Noisy address snippets | 578 | span-overlap F1 | 58.7% |
| Clean nationwide addresses | 100,000 | character-overlap F1 | 66.2% |
| Moscow buildings | 15,196 | exact component micro F1 | 85.4% |

See
[`benchmarks/README.md`](https://github.com/shigabeev/address-normalizer/blob/master/benchmarks/README.md)
for definitions, per-field results, and
the complete 500-row diagnostic table.

Building numbers, корпус, and строение are strongest. Administrative levels,
exact street boundaries, rare abbreviations, and unmarked numeric tails are
weaker.

## Development

```bash
python -m pip install -e .
python -m pip install pytest
pytest
python tools/benchmark.py --check
```

Runtime code lives in `src/address_normalizer`, core tests in `tests`, and the
reproducible failure set in `benchmarks`.

## License

GNU GPL v3.0 only. See
[`LICENSE`](https://github.com/shigabeev/address-normalizer/blob/master/LICENSE).
