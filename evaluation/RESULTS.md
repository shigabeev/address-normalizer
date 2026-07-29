# Committed evaluation results

This file is the index of durable benchmark results stored in the repository.
It separates message detection, address parsing, learned-model evaluation, and
software tests because they answer different questions.

## Result snapshot

| Task and domain | Rows or examples | Primary result | Committed report |
| --- | ---: | ---: | --- |
| Historical component parsing | 500 addresses | 95.85% exact component-value micro F1; 80.4% exact full address | [`legacy_reference_500_report.json`](legacy_reference_500_report.json) |
| Historical failure analysis | 500 addresses, 98 non-exact | Complete row-level diagnoses and scenario flags | [`legacy_reference_500_diagnostics.csv`](legacy_reference_500_diagnostics.csv), [`legacy_reference_500_failure_summary.json`](legacy_reference_500_failure_summary.json) |
| Compact learned tagger | 70 group-disjoint sequence examples | 96.52% token accuracy; 96.88% micro entity F1 | [`../training/model_evaluation.json`](../training/model_evaluation.json) |
| Authored message detection fixture | 30 messages, 20 spans | 100% exact span F1; regression fixture only | [`detection_report.json`](detection_report.json) |
| Complete-message detection diagnostic | 2,841 messages, 144 gold spans | 98.0% overlap precision; 68.1% recall; 80.3% F1 | [`redmadrobot_detection_report.json`](redmadrobot_detection_report.json) |
| RedMadRobot oracle-cropped parsing | 578 address windows | 58.75% same-label span-overlap F1 | [`redmadrobot_report.json`](redmadrobot_report.json) |
| Deepparse nationwide clean parsing | 100,000 addresses | 84.39% binary span-overlap F1; 66.23% character-overlap F1 | [`deepparse_report.json`](deepparse_report.json) |
| Official Moscow clean-building parsing | 15,196 addresses | 85.36% exact component-value F1; 64.93% exact full address | [`datamos_report.json`](datamos_report.json) |

These numbers are not interchangeable and must not be averaged. Open each
report's `scope`, `limitations`, and `metric_definitions` before using a result.

## Source fixtures and manifests

The reports are accompanied by the exact portable inputs or source metadata
needed to interpret or reproduce them:

| Artifact | Purpose |
| --- | --- |
| [`legacy_reference_500.jsonl`](legacy_reference_500.jsonl) | Portable historical parser fixture |
| [`release_gates.json`](release_gates.json) | Minimum non-regression thresholds for that fixture |
| [`detection_reference.jsonl`](detection_reference.jsonl) | Narrow positive and negative detector scenarios |
| [`deepparse_manifest.json`](deepparse_manifest.json) | Pinned source, preparation, and split metadata |
| [`datamos_manifest.json`](datamos_manifest.json) | Pinned Moscow source, filtering, and split metadata |
| [`DATA_SOURCES.md`](DATA_SOURCES.md) | Provenance and licensing notes |

Large external raw/test corpora are intentionally not committed. Their reports
contain pinned source revisions and checksums; preparation writes external data
under the ignored `.cache/external/` directory.

## Reproduction commands

Historical parsing and gates:

```bash
python evaluation/evaluate.py \
  --data evaluation/legacy_reference_500.jsonl \
  --gates evaluation/release_gates.json \
  --output evaluation/legacy_reference_500_report.json
```

Historical failure diagnostics:

```bash
python evaluation/analyze_failures.py
```

Learned tagger:

```bash
python training/train_compact_tagger.py
python training/evaluate_compact_tagger.py
```

Detection:

```bash
python evaluation/evaluate_detection.py
python evaluation/evaluate_redmadrobot_detection.py \
  --data .cache/external/redmadrobot-pii-benchmark-f77ea831.csv
```

External parsing:

```bash
python evaluation/evaluate_redmadrobot.py \
  --data .cache/external/redmadrobot-pii-benchmark-f77ea831.csv
python evaluation/evaluate_deepparse.py
python evaluation/evaluate_datamos.py
```

See [`README.md`](README.md) for the exact data preparation commands and
benchmark boundaries.

## Software tests

`pytest` verifies API, offsets, behavior, evaluation adapters, failure
diagnostics, artifact loading, and regression gates. A passing test count is an
execution/CI result rather than a model-quality metric, so transient console
logs are not committed as benchmark evidence. The benchmark JSON/CSV artifacts
above are the durable results.
