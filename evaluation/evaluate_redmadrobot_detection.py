#!/usr/bin/env python3
"""Evaluate message-level address detection on complete RedMadRobot rows."""

from __future__ import annotations

import argparse
from collections import Counter
import csv
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import sys
from typing import Any, Callable, Iterable, Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from address_normalizer import detect_addresses
from address_normalizer.types import DetectedAddress

from evaluate_redmadrobot import (
    DATASET_REVISION,
    DATASET_SHA256,
    _base_label,
    _clusters,
    _reconstruct,
    _sha256,
)


DEFAULT_DATA = (
    ROOT
    / ".cache"
    / "external"
    / f"redmadrobot-pii-benchmark-{DATASET_REVISION[:8]}.csv"
)


@dataclass(frozen=True, slots=True)
class Span:
    start: int
    end: int


@dataclass(frozen=True, slots=True)
class Message:
    source_row: int
    text: str
    gold: tuple[Span, ...]


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _prf(tp: int, fp: int, fn: int) -> dict[str, int | float]:
    precision = _ratio(tp, tp + fp)
    recall = _ratio(tp, tp + fn)
    f1 = _ratio(2 * precision * recall, precision + recall)
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
    }


def load_messages(path: Path, *, max_gap: int = 3) -> list[Message]:
    """Reconstruct complete messages and STREET+HOUSE gold windows."""

    messages: list[Message] = []
    with path.open(encoding="utf-8", newline="") as source:
        for row_number, row in enumerate(csv.DictReader(source)):
            tokens = tuple(json.loads(row["tokens"]))
            tags = tuple(json.loads(row["ner_tags"]))
            if len(tokens) != len(tags):
                raise ValueError(f"row {row_number}: token/tag length mismatch")
            text, offsets = _reconstruct(tokens)
            spans: list[Span] = []
            for start, end in _clusters(tags, max_gap):
                labels = {
                    _base_label(tag)
                    for tag in tags[start:end]
                }
                if not {"STREET", "HOUSE"} <= labels:
                    continue
                spans.append(
                    Span(
                        start=offsets[start][0],
                        end=offsets[end - 1][1],
                    )
                )
            messages.append(
                Message(
                    source_row=row_number,
                    text=text,
                    gold=tuple(spans),
                )
            )
    return messages


def _overlap(left: Span, right: Span) -> int:
    return max(0, min(left.end, right.end) - max(left.start, right.start))


def _match(
    expected: Sequence[Span],
    actual: Sequence[Span],
    *,
    exact: bool,
) -> tuple[list[tuple[int, int]], set[int], set[int]]:
    candidates = sorted(
        (
            (
                _overlap(wanted, observed),
                expected_index,
                actual_index,
            )
            for expected_index, wanted in enumerate(expected)
            for actual_index, observed in enumerate(actual)
            if (
                wanted == observed
                if exact
                else _overlap(wanted, observed) > 0
            )
        ),
        reverse=True,
    )
    matched_expected: set[int] = set()
    matched_actual: set[int] = set()
    matches: list[tuple[int, int]] = []
    for _, expected_index, actual_index in candidates:
        if (
            expected_index in matched_expected
            or actual_index in matched_actual
        ):
            continue
        matched_expected.add(expected_index)
        matched_actual.add(actual_index)
        matches.append((expected_index, actual_index))
    return matches, matched_expected, matched_actual


def _failure_reasons(
    expected: Sequence[Span],
    actual: Sequence[Span],
) -> list[str]:
    matches, matched_expected, matched_actual = _match(
        expected,
        actual,
        exact=False,
    )
    reasons: set[str] = set()
    for expected_index, actual_index in matches:
        wanted = expected[expected_index]
        observed = actual[actual_index]
        if observed.start < wanted.start or observed.end > wanted.end:
            reasons.add("span_includes_context")
        if observed.start > wanted.start or observed.end < wanted.end:
            reasons.add("span_drops_gold_text")
    if len(matched_expected) != len(expected):
        reasons.add("missed_address")
    if len(matched_actual) != len(actual):
        reasons.add("spurious_address")
    return sorted(reasons)


def evaluate(
    messages: Iterable[Message],
    detector: Callable[[str], Sequence[DetectedAddress]] = detect_addresses,
) -> dict[str, Any]:
    """Score complete messages without oracle-cropping detector input."""

    rows = positive_rows = negative_rows = exact_messages = correctly_empty = 0
    exact_tp = exact_fp = exact_fn = 0
    overlap_tp = overlap_fp = overlap_fn = 0
    failures: list[dict[str, Any]] = []
    failure_reasons: Counter[str] = Counter()

    for message in messages:
        rows += 1
        if message.gold:
            positive_rows += 1
        else:
            negative_rows += 1
        detected = tuple(detector(message.text))
        actual = tuple(Span(item.start, item.end) for item in detected)
        if not message.gold and not actual:
            correctly_empty += 1

        exact_matches, _, _ = _match(message.gold, actual, exact=True)
        overlap_matches, _, _ = _match(message.gold, actual, exact=False)
        exact_tp += len(exact_matches)
        exact_fp += len(actual) - len(exact_matches)
        exact_fn += len(message.gold) - len(exact_matches)
        overlap_tp += len(overlap_matches)
        overlap_fp += len(actual) - len(overlap_matches)
        overlap_fn += len(message.gold) - len(overlap_matches)

        message_exact = list(message.gold) == list(actual)
        exact_messages += int(message_exact)
        if not message_exact:
            reasons = _failure_reasons(message.gold, actual)
            failure_reasons.update(reasons)
            failures.append(
                {
                    "source_row": message.source_row,
                    "text": message.text,
                    "gold": [
                        {
                            **asdict(span),
                            "text": message.text[span.start : span.end],
                        }
                        for span in message.gold
                    ],
                    "predicted": [
                        {
                            "start": item.start,
                            "end": item.end,
                            "text": item.text,
                            "confidence": round(item.confidence, 4),
                            "signals": list(item.signals),
                        }
                        for item in detected
                    ],
                    "failure_reasons": reasons,
                }
            )

    return {
        "rows": rows,
        "positive_rows": positive_rows,
        "negative_rows": negative_rows,
        "scope": (
            "complete reconstructed benchmark messages; gold address windows "
            "must contain both STREET and HOUSE labels"
        ),
        "limitations": [
            (
                "The source is a PII NER benchmark, not a detector-specific "
                "Russian message sample."
            ),
            (
                "Gold spans follow BIO annotation boundaries while predicted "
                "spans intentionally include parseable markers and units."
            ),
            (
                "Rows without a STREET+HOUSE cluster are treated as detection "
                "negatives even when they contain isolated location entities."
            ),
        ],
        "metric_definitions": {
            "span_overlap_micro": (
                "one-to-one precision, recall, and F1 for any character "
                "overlap between a detected span and a STREET+HOUSE gold window"
            ),
            "exact_span_micro": (
                "one-to-one precision, recall, and F1 requiring identical "
                "half-open boundaries"
            ),
            "exact_message_rate": (
                "fraction of complete messages whose ordered span lists match"
            ),
            "negative_message_specificity": (
                "fraction of messages without a STREET+HOUSE gold window where "
                "the detector returns no span"
            ),
        },
        "span_overlap_micro": _prf(overlap_tp, overlap_fp, overlap_fn),
        "exact_span_micro": _prf(exact_tp, exact_fp, exact_fn),
        "exact_message_rate": round(_ratio(exact_messages, rows), 6),
        "negative_message_specificity": round(
            _ratio(correctly_empty, negative_rows),
            6,
        ),
        "failure_case_count": len(failures),
        "failure_rows_by_reason": dict(failure_reasons.most_common()),
        "failure_cases": failures,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument("--max-gap", type=int, default=3)
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "evaluation/redmadrobot_detection_report.json",
    )
    args = parser.parse_args(argv)
    if not args.data.exists():
        raise SystemExit(
            f"dataset not found: {args.data}; run the RedMadRobot download "
            "command documented in evaluation/README.md"
        )
    if _sha256(args.data) != DATASET_SHA256:
        raise SystemExit("RedMadRobot dataset checksum mismatch")

    report = evaluate(load_messages(args.data, max_gap=args.max_gap))
    report["source"] = {
        "dataset": "redmadrobot-rnd/pii_benchmark",
        "revision": DATASET_REVISION,
        "sha256": DATASET_SHA256,
    }
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(f"{rendered}\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
