"""Evaluate the parser on a prepared Deepparse Russian test sample."""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import asdict, dataclass
import gzip
import json
from pathlib import Path
import sys
import time
from typing import Any, Callable, Iterable, Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from address_normalizer import parse
from address_normalizer.types import ParsedAddress

from deepparse_data import (
    DATASET_REVISION,
    DEFAULT_SAMPLE,
    SCORED_LABELS,
)


@dataclass(frozen=True, slots=True)
class Span:
    label: str
    start: int
    end: int


PREDICTED_FIELDS = {
    "postal_code": "POSTAL_CODE",
    "region": "REGION",
    "district": "DISTRICT",
    "city": "CITY",
    "settlement": "CITY",
    "street": "STREET",
    "street_type": "STREET",
    "house_num": "HOUSE",
    "corpus": "HOUSE",
    "structure": "HOUSE",
    "apartment": "APARTMENT",
}


def _offsets(text: str, tokens: Sequence[str]) -> tuple[tuple[int, int], ...]:
    offsets: list[tuple[int, int]] = []
    cursor = 0
    for token in tokens:
        start = text.find(token, cursor)
        if start < 0:
            raise ValueError(f"token {token!r} cannot be aligned in {text!r}")
        end = start + len(token)
        offsets.append((start, end))
        cursor = end
    return tuple(offsets)


def gold_spans(row: dict[str, Any]) -> list[Span]:
    tokens = row["tokens"]
    labels = row["labels"]
    if len(tokens) != len(labels):
        raise ValueError("prepared row has mismatched tokens and labels")
    offsets = _offsets(row["raw"], tokens)
    spans: list[Span] = []
    active_label: str | None = None
    active_start = active_end = 0
    for label, (start, end) in zip(labels, offsets):
        if label != active_label:
            if active_label in SCORED_LABELS:
                spans.append(Span(active_label, active_start, active_end))
            active_label = label if label in SCORED_LABELS else None
            active_start = start
        if active_label is not None:
            active_end = end
    if active_label in SCORED_LABELS:
        spans.append(Span(active_label, active_start, active_end))
    return spans


def predicted_spans(result: ParsedAddress) -> list[Span]:
    grouped: dict[str, list[tuple[int, int]]] = {}
    for field, label in PREDICTED_FIELDS.items():
        part = getattr(result, field)
        if part is not None:
            grouped.setdefault(label, []).append((part.start, part.end))
    return [
        Span(
            label,
            min(start for start, _ in field_offsets),
            max(end for _, end in field_offsets),
        )
        for label, field_offsets in sorted(grouped.items())
    ]


def _overlap(left: Span, right: Span) -> int:
    return max(0, min(left.end, right.end) - max(left.start, right.start))


def _match(
    gold: Sequence[Span],
    predicted: Sequence[Span],
) -> tuple[list[tuple[int, int]], set[int], set[int]]:
    candidates = sorted(
        (
            (_overlap(wanted, actual), gold_index, predicted_index)
            for gold_index, wanted in enumerate(gold)
            for predicted_index, actual in enumerate(predicted)
            if wanted.label == actual.label and _overlap(wanted, actual)
        ),
        reverse=True,
    )
    matched_gold: set[int] = set()
    matched_predicted: set[int] = set()
    matches: list[tuple[int, int]] = []
    for _, gold_index, predicted_index in candidates:
        if gold_index in matched_gold or predicted_index in matched_predicted:
            continue
        matched_gold.add(gold_index)
        matched_predicted.add(predicted_index)
        matches.append((gold_index, predicted_index))
    return matches, matched_gold, matched_predicted


def _metrics(counts: dict[str, int]) -> dict[str, int | float]:
    precision = (
        counts["tp"] / (counts["tp"] + counts["fp"])
        if counts["tp"] + counts["fp"]
        else 0.0
    )
    recall = (
        counts["tp"] / (counts["tp"] + counts["fn"])
        if counts["tp"] + counts["fn"]
        else 0.0
    )
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall
        else 0.0
    )
    return {
        **counts,
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
    }


def _score(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    counts = {
        label: {"tp": 0, "fp": 0, "fn": 0, "support": 0}
        for label in SCORED_LABELS
    }
    token_counts = {
        label: {"tp": 0, "fp": 0, "fn": 0, "support": 0}
        for label in SCORED_LABELS
    }
    character_counts = {
        label: {"tp": 0, "fp": 0, "fn": 0, "support": 0}
        for label in SCORED_LABELS
    }
    failures: list[dict[str, Any]] = []
    row_count = exact_rows = exact_token_rows = exact_spans = overlap_spans = 0
    tiers: Counter[str] = Counter()
    started = time.monotonic()
    for row in rows:
        row_count += 1
        tiers[row["tier"]] += 1
        gold = gold_spans(row)
        predicted = predicted_spans(parse(row["raw"]))
        matches, matched_gold, matched_predicted = _match(gold, predicted)
        overlap_spans += len(matches)
        exact_spans += sum(
            gold[gold_index] == predicted[predicted_index]
            for gold_index, predicted_index in matches
        )
        exact_row = (
            len(matches) == len(gold) == len(predicted)
            and all(
                gold[gold_index] == predicted[predicted_index]
                for gold_index, predicted_index in matches
            )
        )
        if exact_row:
            exact_rows += 1

        token_offsets = _offsets(row["raw"], row["tokens"])
        actual_token_labels: list[str] = []
        for start, end in token_offsets:
            token_span = Span("", start, end)
            candidates = [
                (_overlap(token_span, span), span.label)
                for span in predicted
                if _overlap(token_span, span)
            ]
            actual_token_labels.append(
                max(candidates)[1] if candidates else "O"
            )
        wanted_token_labels = [
            label if label in SCORED_LABELS else "O"
            for label in row["labels"]
        ]
        if actual_token_labels == wanted_token_labels:
            exact_token_rows += 1
        for wanted, actual in zip(wanted_token_labels, actual_token_labels):
            if wanted in SCORED_LABELS:
                token_counts[wanted]["support"] += 1
            if wanted == actual:
                if wanted in SCORED_LABELS:
                    token_counts[wanted]["tp"] += 1
                continue
            if actual in SCORED_LABELS:
                token_counts[actual]["fp"] += 1
            if wanted in SCORED_LABELS:
                token_counts[wanted]["fn"] += 1

        for label in SCORED_LABELS:
            gold_indices = {
                index for index, span in enumerate(gold) if span.label == label
            }
            predicted_indices = {
                index
                for index, span in enumerate(predicted)
                if span.label == label
            }
            label_matches = sum(
                gold_index in gold_indices
                and predicted_index in predicted_indices
                for gold_index, predicted_index in matches
            )
            counts[label]["tp"] += label_matches
            counts[label]["fp"] += len(predicted_indices - matched_predicted)
            counts[label]["fn"] += len(gold_indices - matched_gold)
            counts[label]["support"] += len(gold_indices)
            gold_characters = sum(
                gold[index].end - gold[index].start for index in gold_indices
            )
            predicted_characters = sum(
                predicted[index].end - predicted[index].start
                for index in predicted_indices
            )
            overlapping_characters = sum(
                _overlap(gold[gold_index], predicted[predicted_index])
                for gold_index, predicted_index in matches
                if gold_index in gold_indices
                and predicted_index in predicted_indices
            )
            character_counts[label]["tp"] += overlapping_characters
            character_counts[label]["fp"] += (
                predicted_characters - overlapping_characters
            )
            character_counts[label]["fn"] += (
                gold_characters - overlapping_characters
            )
            character_counts[label]["support"] += gold_characters

        if not exact_row and len(failures) < 50:
            failures.append(
                {
                    "source_row": row["source_row"],
                    "example_id": row["example_id"],
                    "tier": row["tier"],
                    "raw": row["raw"],
                    "gold": [asdict(span) for span in gold],
                    "predicted": [asdict(span) for span in predicted],
                }
            )

    fields = {label: _metrics(values) for label, values in counts.items()}
    token_fields = {
        label: _metrics(values) for label, values in token_counts.items()
    }
    character_fields = {
        label: _metrics(values) for label, values in character_counts.items()
    }
    totals = {
        key: sum(values[key] for values in counts.values())
        for key in ("tp", "fp", "fn", "support")
    }
    token_totals = {
        key: sum(values[key] for values in token_counts.values())
        for key in ("tp", "fp", "fn", "support")
    }
    character_totals = {
        key: sum(values[key] for values in character_counts.values())
        for key in ("tp", "fp", "fn", "support")
    }
    elapsed = time.monotonic() - started
    return {
        "rows": row_count,
        "tiers": dict(sorted(tiers.items())),
        "matching": "one-to-one same-label character-span overlap",
        "micro": _metrics(totals),
        "character_micro": _metrics(character_totals),
        "token_micro": _metrics(token_totals),
        "macro_field_f1": round(
            sum(float(value["f1"]) for value in fields.values())
            / len(fields),
            6,
        ),
        "token_macro_field_f1": round(
            sum(float(value["f1"]) for value in token_fields.values())
            / len(token_fields),
            6,
        ),
        "character_macro_field_f1": round(
            sum(float(value["f1"]) for value in character_fields.values())
            / len(character_fields),
            6,
        ),
        "exact_address_rate": round(
            exact_rows / row_count if row_count else 0.0,
            6,
        ),
        "exact_token_sequence_rate": round(
            exact_token_rows / row_count if row_count else 0.0,
            6,
        ),
        "exact_span_recall": round(
            exact_spans / totals["support"] if totals["support"] else 0.0,
            6,
        ),
        "overlap_spans": overlap_spans,
        "exact_spans": exact_spans,
        "fields": fields,
        "character_fields": character_fields,
        "token_fields": token_fields,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_second": round(row_count / elapsed, 1) if elapsed else None,
        "failure_sample": failures,
    }


def load_rows(
    path: Path,
    *,
    limit: int | None = None,
    tier: str | None = None,
) -> Iterable[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as source:
        yielded = 0
        for line in source:
            row = json.loads(line)
            if row.get("split") != "test":
                raise ValueError("prepared benchmark contains a non-test row")
            if tier is not None and row.get("tier") != tier:
                continue
            yield row
            yielded += 1
            if limit is not None and yielded >= limit:
                break


def _summary(report: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in report.items()
        if key
        not in {
            "failure_sample",
            "fields",
            "character_fields",
            "token_fields",
        }
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=DEFAULT_SAMPLE)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if not args.data.exists():
        parser.error(
            f"{args.data} does not exist; run prepare_deepparse.py first"
        )
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")

    score = _score(load_rows(args.data, limit=args.limit))
    report = {
        "scope": (
            "untuned external evaluation on clean Russian open-geographic "
            "address strings from the sealed Deepparse test split"
        ),
        "source": {
            "repository": "deepparse/worldwide-addresses",
            "configuration": "ru",
            "license": "CC BY 4.0",
            "revision": DATASET_REVISION,
        },
        "limitations": [
            "clean registry-derived strings are easier than user-entered text",
            "Country and Suburb source tags are retained as context but unscored",
            "span overlap gives partial credit and is not exact-value accuracy",
        ],
        **score,
        "slices": {
            tier: _summary(
                _score(load_rows(args.data, limit=args.limit, tier=tier))
            )
            for tier in (
                "street_house_unit",
                "street_house",
                "street_only",
                "number_or_unit_only",
            )
        },
    }
    rendered = f"{json.dumps(report, ensure_ascii=False, indent=2)}\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
