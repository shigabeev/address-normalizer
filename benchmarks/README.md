# Benchmarks and known failures

The parser extracts text; it does not resolve an address against FIAS/GAR.
Results from different datasets use different annotation and matching rules and
must not be averaged into one “accuracy” number.

## Release results

| Dataset | Rows/windows | Metric | Precision | Recall | F1 / rate |
| --- | ---: | --- | ---: | ---: | ---: |
| Historical address sample | 500 | exact component values, micro | 97.0% | 94.8% | **95.9%** |
| Historical address sample | 500 | every field exact | — | — | **80.4%** |
| Noisy address snippets | 578 | same-label span overlap | 60.0% | 57.5% | **58.7%** |
| Noisy street+house slice | 144 | same-label span overlap | 81.7% | 76.2% | **78.8%** |
| Clean nationwide addresses | 100,000 | character overlap | 72.8% | 60.8% | **66.2%** |
| Moscow buildings | 15,196 | exact component values, micro | 85.4% | 85.3% | **85.4%** |
| Free-message detector | 144 positives | span overlap | 98.0% | 68.1% | **80.3%** |
| Free-message detector | negative messages | specificity | — | — | **100%** |

The detector’s specificity result belongs to its evaluated negative slice, not
to arbitrary production traffic.

## Historical 500-row sample

This is the only benchmark run in normal CI:

```bash
python tools/benchmark.py --check
```

The command reads [`legacy_500.jsonl`](legacy_500.jsonl).
It passes when exact-address rate is at least 80%, exact-component micro F1 is
at least 95%, and at least 75% of rows have no residual word/number spans.

Per-field exact-value results:

| Field | Support | Precision | Recall | F1 |
| --- | ---: | ---: | ---: | ---: |
| postal code | 498 | 100.0% | 100.0% | 100.0% |
| region | 58 | 89.8% | 91.4% | 90.6% |
| district | 11 | 90.9% | 90.9% | 90.9% |
| city | 496 | 99.0% | 99.0% | 99.0% |
| settlement | 9 | 80.0% | 44.4% | 57.1% |
| street | 500 | 96.0% | 96.0% | 96.0% |
| street type | 500 | 94.8% | 87.8% | 91.2% |
| house | 500 | 95.8% | 95.0% | 95.4% |
| корпус | 75 | 100.0% | 92.0% | 95.8% |
| строение | 137 | 97.1% | 97.1% | 97.1% |
| apartment/unit | 95 | 96.2% | 80.0% | 87.4% |

The sample contains 402 exact rows and 98 failures. The most common primary
failure hypotheses are:

| Cause | Rows |
| --- | ---: |
| conflicting street markers | 22 |
| reference infers an absent street type | 20 |
| ambiguous or unsupported abbreviation | 15 |
| unmarked numeric roles | 14 |
| compound or letter-suffixed number boundary | 7 |
| administrative label/boundary | 6 |

[`legacy_500_results.csv`](legacy_500_results.csv)
contains every row,
expected and actual values for every field, scenario flags, mismatch category,
unparsed spans, warnings, and a narrow failure hypothesis. These diagnoses are
triage aids, not independently adjudicated ground truth.

## Interpreting the other datasets

- The noisy snippet benchmark uses oracle-cropped address windows and
  same-label span overlap. It is useful for messy input, not end-to-end message
  detection.
- The nationwide set is large and clean but uses a schema adapter and lenient
  character overlap.
- The Moscow set is official clean building data. House F1 is 98.2%, while
  exact street F1 is 66.2%; this gap is more useful than its aggregate.
- The free-message benchmark measures conservative span detection. Exact-span
  F1 is much lower than overlap F1, so callers must inspect returned offsets.

The bundled 37 KB sequence model was trained with a deterministic,
group-disjoint 70/15/15 split of the historical rows. Its held-out token
accuracy is 96.5%, complete-sequence accuracy 94.3%, and micro entity F1 96.9%.
The split has little district and settlement coverage.

The maintainer authored and authorized redistribution of the historical source
rows and derived model under GPL-3.0-only. External benchmark source corpora are
not included in this repository.
