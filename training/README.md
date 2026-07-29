# Training the compact tagger

The runtime model is a sparse linear-chain sequence tagger with Viterbi
inference. Training uses an epoch-averaged structured perceptron. Training and
inference require only the Python standard library.

The real-address corpus is derived from the 500 source-verifiable rows in
`evaluation/legacy_reference_500.jsonl`:

1. the deterministic parser records the exact residual word tokens that reach
   the model;
2. source-verifiable address fields are aligned to those token offsets;
3. marker-free views are derived from the same real component names;
4. examples are grouped by canonical administrative/street identity;
5. SHA-256 assigns whole groups to train, validation, or test (70/15/15);
6. epoch count is chosen on validation only;
7. the final candidate is evaluated once on the untouched test groups.

Regenerate the model and committed evaluation report:

```bash
python training/train_compact_tagger.py
```

Verify the bundled artifact against fixed test gates:

```bash
python training/evaluate_compact_tagger.py
```

The first real model is 37 KB. Compared with the preserved 20 KB synthetic
starter on the group-disjoint sequence test:

| Metric | Synthetic starter | Real model |
| --- | ---: | ---: |
| Token accuracy | 71.3% | 96.5% |
| Complete sequence accuracy | 61.4% | 94.3% |
| Micro entity F1 | 78.1% | 96.9% |

On the 21 corresponding end-to-end holdout rows, micro field F1 improves from
87.4% to 91.3%. The test split contains no independently measured
`DISTRICT` or `SETTLEMENT` tokens, so those classes must not be claimed as
validated by this result. See `model_evaluation.json` for the exact split,
tuning runs, class coverage, failures, and end-to-end comparison.

`training/baselines/synthetic_model.json` preserves the pre-real-data model so
that regeneration remains reproducible and comparisons do not silently change
after the bundled model is replaced.

## Provenance

The maintainer authorized redistribution of the historical source workbook,
its committed 500-row derivative, and the compact model under GPL-3.0-only.
The decision, source commit, deterministic generation path, and external-data
boundary are recorded in `LICENSING.md`.

The external preparation tools now expose 5,681,842 Deepparse training rows and
276,368 historical Moscow-registry training rows in group-disjoint splits.
They are not used by the current 37 KB model. Before training on them, define a
sampling policy so repeated clean formatting does not overwhelm the smaller
noisy-input corpus, keep the committed test groups sealed, and record the
derived-model rights for CC BY 4.0 and the Moscow source terms.

For a production training release:

1. independently review at least 300 aligned rows;
2. add substantially more district and settlement examples;
3. preserve canonical-address grouping across every split;
4. reserve a final dataset not used for feature or rule changes;
5. report confidence intervals and field metrics by region and source system;
6. document the right to redistribute examples and the derived model.
