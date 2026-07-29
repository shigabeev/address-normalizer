# Licensing and model provenance

## Maintainer decision

Effective 2026-07-29, the repository maintainer selected the GNU General
Public License version 3 for this project. The repository is distributed under
the `GPL-3.0-only` SPDX expression. [`LICENSE`](LICENSE) is the unmodified
`gpl-3.0` template returned by GitHub's license API so GitHub and package tools
can identify it consistently.

The maintainer also authorizes redistribution of the historical
`ref/references.xlsx` workbook, the deterministic
`evaluation/legacy_reference_500.jsonl` derivative, and the compact model
derived from those rows as repository and package artifacts under
`GPL-3.0-only`.

## Provenance record

- The workbook first appears in repository commit
  `4a72605b0204e2ba4c21f09d74c249b066c41021`, authored by Ilya Shigabeev
  (`beat@live.ru`).
- Repository history for the workbook and v2 model contains only the
  maintainer identities `Ilya Shigabeev` and `frappuccino`, using the same
  `beat@live.ru` email address.
- The source-verifiable 500-row derivative is committed as
  `evaluation/legacy_reference_500.jsonl`.
- `training/train_compact_tagger.py` deterministically regenerates the bundled
  `src/address_normalizer/data/model.json`.
- `training/model_evaluation.json` records the source dataset digest, split,
  training algorithm, tuning, test results, and artifact size.
- CI regenerates the model and requires a byte-for-byte match with the bundled
  artifact.

This record covers artifacts distributed by this repository and package. Large
external Deepparse, RedMadRobot, and Moscow source corpora are not included in
the wheel or source distribution; their separate source and license records are
documented in `evaluation/DATA_SOURCES.md`.

## Contributions

Contributions are accepted under the repository's `GPL-3.0-only` license.
Contributors must have the right to submit their code, data, and generated
artifacts and must record data/model provenance as described in
[`CONTRIBUTING.md`](CONTRIBUTING.md).
