# Launch drafts and evidence plan

**Status: release facts verified; destination-specific posts remain drafts.**

The maintainer approved GPL-3.0-only licensing, the recorded reference-data and
model provenance, and publication of `2.0.0a2` on 2026-07-29. The drafts below
are ready for final editorial review after the public package and release URLs
resolve. Nothing in this file authorizes automated posting to third-party
channels.

## One-line problem and solution

> Extract typed fields and original offsets from messy Russian addresses in a
> small offline Python package—then verify them against the FIAS/GAR resolver
> you already control.

This line deliberately says “extract,” not “validate,” “resolve,” or “geocode.”

## Facts to verify immediately before launch

- release tag and package version;
- public license and exact repository/path scope;
- PyPI and GitHub release URLs;
- Python support matrix;
- clean wheel install and CLI/API output;
- wheel and bundled-model byte sizes;
- runtime dependency list;
- all committed report checksums and baseline values;
- current limitations and unresolved data terms;
- release notes and migration boundary.

Never copy an approximate size or score from an older draft into a release.

## Draft: GitHub release

### address-normalizer 2.0.0a2: a small offline Russian address parser

`address-normalizer` v2 extracts typed address components while preserving raw
substrings, character offsets, warnings, unparsed text, and alternative
interpretations.

```python
from address_normalizer import parse

result = parse("Ополченская 5-30")
print(result.normalized)
# Ополченская, д 5, кв 30
print(result.warnings)
# ("ambiguous_numeric_tail",)
```

What is intentionally outside the package:

- no bundled FIAS/GAR database;
- no address verification, identifier lookup, or geocoding;
- no Elasticsearch or service process;
- no hidden downloads or network calls;
- no runtime dependencies.

The wheel is `45,843` bytes and its bundled model is `37,130` bytes in this
release. Independent benchmark results are published by domain rather than
averaged in the
[README reliability table](https://github.com/shigabeev/address-normalizer#reliability).
Numeric building fields are currently stronger than administrative fields and
exact street extraction.

Install: `python -m pip install --pre address-normalizer==2.0.0a2`

Documentation: [English README](https://github.com/shigabeev/address-normalizer#readme)
and [Russian README](https://github.com/shigabeev/address-normalizer/blob/master/README.ru.md)

Migration notes: [Changelog](https://github.com/shigabeev/address-normalizer/blob/master/CHANGELOG.md)

License: GPL-3.0-only for the repository and distributed package.

## Draft: Show HN

**Title**

> Show HN: address-normalizer – small offline parsing for messy Russian addresses

**Text**

I rebuilt an old Russian address project around a narrower boundary. The new
Python package extracts components and preserves source offsets, warnings,
unparsed text, and alternatives. It does not bundle FIAS/GAR, run
Elasticsearch, verify existence, or make network calls.

The motivating example is `Ополченская 5-30`: the parser selects house `5` and
apartment `30`, but retains compound house `5-30` as an alternative for a
downstream resolver.

The runtime has no dependencies; verified artifact sizes for this release are
`45,843` and `37,130` bytes. The README publishes four non-comparable
benchmark domains and their limitations instead of one headline “accuracy”
number. Current weaknesses are administrative recall and exact street
boundaries.

I would value feedback on the typed result contract, ambiguity handling, and
the boundary between extraction and a customer-managed FIAS/GAR resolver:
https://github.com/shigabeev/address-normalizer

## Draft: Habr

**Заголовок**

> Маленький офлайн-парсер российских адресов без встроенного ФИАС и Elasticsearch

**Лид**

Старый `address-normalizer` решал сразу две задачи: разбирал строку и искал
адрес в заранее загруженном ФИАС. В версии 2 граница уже: библиотека только
извлекает компоненты, сохраняет исходные смещения, неоднозначности и
неразобранный остаток. Проверка существования и выбор идентификатора остаются
за актуальным справочником пользователя.

**План текста**

1. Почему парсинг и разрешение адреса — разные задачи.
2. Разбор `Ополченская 5-30`: выбранный вариант и сохранённая альтернатива.
3. Конвейер: токенизация → правила → компактная модель → постобработка.
4. Типизированный API и JSONL без runtime-зависимостей.
5. Как передать поля в собственный ФИАС/ГАР-сервис.
6. Четыре отдельных бенчмарка и почему их нельзя усреднять.
7. Слабые места: административные поля, точные границы улиц, неоднозначные
   числовые хвосты.
8. Размеры артефактов, воспроизводимая команда и планы после alpha.

**Финал**

Исходники и методика: https://github.com/shigabeev/address-normalizer.
Особенно полезны синтетические
примеры ошибок с ожидаемыми полями и смещениями; реальные частные адреса
публиковать не нужно.

## Draft: Reddit / r/Python

**Title**

> address-normalizer v2: dependency-free, offline Russian address field extraction

**Body**

I released `2.0.0a2` of a small Python library that extracts Russian
address components and preserves original character spans. It is intentionally
not a FIAS/GAR database, validator, geocoder, or service.

The runtime has no dependencies and makes no network calls. `parse`,
`parse_iter`, and `parse_many` return frozen typed results with warnings,
alternatives, and JSON-compatible serialization. The README includes a FastAPI
wrapper, streaming JSONL ETL, and a generic boundary for a resolver you operate.

I have kept the evidence separated across historical regression, noisy address
windows, nationwide clean strings, and an official Moscow snapshot. The weakest
current areas are administrative fields and exact street extraction.

Repository and reproducible numbers: https://github.com/shigabeev/address-normalizer

Feedback on API ergonomics and failure reporting is welcome. Please use
synthetic or redacted addresses.

## Draft: Telegram / LinkedIn

> `Ополченская 5-30` — это дом 5, квартира 30 или дом 5-30?
>
> `address-normalizer` v2 разбирает российские адресные строки офлайн, сохраняет
> исходные смещения и не скрывает неоднозначность. Внутри нет ФИАС/ГАР,
> Elasticsearch, сетевых запросов и runtime-зависимостей: библиотека извлекает
> кандидатов, а актуальный справочник пользователя их проверяет.
>
> В README есть типизированный API, JSONL, FastAPI-пример, интеграционная граница
> с собственным resolver и четыре раздельных бенчмарка с ограничениями.
>
> https://github.com/shigabeev/address-normalizer/releases/tag/v2.0.0a2

Before using this short post, add the verified license and release status in the
linked page; do not let brevity conceal them.

## Reproducible benchmark command

The small committed regression requires no external dataset download:

```bash
python evaluation/evaluate.py \
  --data evaluation/legacy_reference_500.jsonl \
  --gates evaluation/release_gates.json
```

Record the commit SHA, Python version, full JSON output, wall-clock environment,
and whether the gate passed. Describe it as a historical silver regression, not
an independent nationwide production score.

The large and external benchmark commands live in `evaluation/README.md`. Run
them only in a separate environment with pinned data artifacts. Report all
defined metrics and per-field results; do not rerun a sealed set repeatedly
while tuning.

## Alternative-comparison methodology

A fair comparison starts by aligning product boundaries.

1. **Classify the alternative.** Is it an extractor, address validator,
   FIAS/GAR resolver, geocoder, tokenizer/model, or complete service? Do not
   rank different tasks on one “accuracy” axis.
2. **Pin public versions.** Record package/service version, source revision,
   configuration, model/data revision, date, Python/platform, and exact command.
3. **Use permitted, task-matched data.** Separate noisy user input, clean
   registry strings, administrative-only text, and historical compatibility.
   Prevent entity/building groups from leaking across splits.
4. **Map schemas in writing.** Publish every field mapping and unsupported
   field. Do not score corrected registry values as extractor truth when those
   values do not occur in the input.
5. **Report multiple exact metrics.** At minimum publish exact component values,
   exact full-address rows, character/span behavior, per-field precision/recall/
   F1, abstentions/unparsed evidence, and failure slices when supported.
6. **Measure operations separately.** Report artifact/download size, runtime
   dependencies, cold/warm latency or throughput, peak memory, required service
   or registry, network behavior, and hardware. Never infer an unmeasured value.
7. **Preserve failure evidence.** Count extra fields, missing fields,
   alternatives, and discarded text. Do not award a cleaner score for hiding
   ambiguity.
8. **Invite correction.** Publish the harness, raw aggregate report, license/
   provenance notes, and a contact path. Label unavailable results
   “not measured,” not zero.

Before naming a competitor in public, verify its current official documentation
and reproduce the claim. Do not repeat marketing copy, old architecture sizes,
or license assumptions as fact.

## Release demo / terminal recording plan

Target length: 75–100 seconds, one continuous recording, no edits that hide
installation or network activity.

1. Start in a new virtual environment with network disabled after the wheel is
   already available locally.
2. Show the wheel filename and exact byte size.
3. Install the wheel from its local path and show that no runtime dependency is
   installed.
4. Run `Ополченская 5-30`; point to spans, warning, and compound-house
   alternative rather than only normalized text.
5. Pipe two lines through `address-normalizer --jsonl`.
6. Run the FIAS/GAR HTTP example without `--send` and explain that it prints an
   application-owned resolver request but performs no network call.
7. Show the four-domain benchmark table and the administrative/street
   limitations.
8. End on the install command, repository URL, license, and alpha status.

Save the command transcript beside the recording. Do not show tokens, internal
resolver URLs, shell history, private addresses, local usernames, or unpublished
benchmark data.

## Launch checklist

- [x] The licensing and provenance blockers are resolved in writing.
- [ ] A release actually exists at every linked URL.
- [x] Placeholder markers are gone.
- [ ] Install, API, CLI, wheel, and benchmark commands were rerun from the tag.
- [x] Sizes and metrics match the candidate artifacts.
- [x] Limitations remain adjacent to the claims they qualify.
- [x] No post implies validation, FIAS ID lookup, geocoding, or calibrated
      confidence.
- [ ] Maintainer approved each destination-specific draft.
- [x] Nothing has been posted by automation.
