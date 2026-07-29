#!/usr/bin/env python3
"""Evaluate free-form message address detection on a transparent fixture."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Callable, Iterable, Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from address_normalizer import detect_addresses
from address_normalizer.types import DetectedAddress


Span = tuple[int, int]


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


def load_rows(path: Path) -> list[dict[str, Any]]:
    """Load and validate the annotated JSONL fixture."""

    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict) or not isinstance(row.get("message"), str):
            raise ValueError(f"{path}:{line_number}: message must be a string")
        expected = row.get("expected")
        if not isinstance(expected, list) or not all(
            isinstance(value, str) for value in expected
        ):
            raise ValueError(f"{path}:{line_number}: expected must be strings")
        rows.append(row)
    if not rows:
        raise ValueError(f"{path}: no rows")
    return rows


def _expected_spans(message: str, expected: Sequence[str]) -> list[Span]:
    spans: list[Span] = []
    search_start = 0
    for value in expected:
        start = message.find(value, search_start)
        if start < 0:
            raise ValueError(f"expected text is absent from message: {value!r}")
        spans.append((start, start + len(value)))
        search_start = start + len(value)
    return spans


def _overlap(left: Span, right: Span) -> int:
    return max(0, min(left[1], right[1]) - max(left[0], right[0]))


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
            if (wanted == observed if exact else _overlap(wanted, observed) > 0)
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
    _, matched_expected, matched_actual = _match(expected, actual, exact=False)
    reasons: set[str] = set()
    for expected_index, wanted in enumerate(expected):
        if expected_index not in matched_expected:
            reasons.add("missed_address")
            continue
        overlaps = [
            observed for observed in actual if _overlap(wanted, observed)
        ]
        for observed in overlaps:
            if observed[0] < wanted[0] or observed[1] > wanted[1]:
                reasons.add("span_includes_context")
            if observed[0] > wanted[0] or observed[1] < wanted[1]:
                reasons.add("span_drops_address_text")
    if len(matched_actual) != len(actual):
        reasons.add("spurious_address")
    return sorted(reasons)


def evaluate(
    rows: Iterable[dict[str, Any]],
    detector: Callable[[str], Sequence[DetectedAddress]] = detect_addresses,
) -> dict[str, Any]:
    """Score exact and overlap spans without treating overlap as exact."""

    row_count = positive_rows = negative_rows = exact_messages = 0
    correctly_empty = 0
    exact_tp = exact_fp = exact_fn = 0
    overlap_tp = overlap_fp = overlap_fn = 0
    failures: list[dict[str, Any]] = []
    scenario_counts: dict[str, dict[str, int]] = {}

    for row in rows:
        row_count += 1
        message = str(row["message"])
        expected = _expected_spans(message, row["expected"])
        detected = tuple(detector(message))
        actual = [item.span for item in detected]
        if expected:
            positive_rows += 1
        else:
            negative_rows += 1
            if not actual:
                correctly_empty += 1

        exact_matches, _, _ = _match(expected, actual, exact=True)
        overlap_matches, _, _ = _match(expected, actual, exact=False)
        exact_tp += len(exact_matches)
        exact_fp += len(actual) - len(exact_matches)
        exact_fn += len(expected) - len(exact_matches)
        overlap_tp += len(overlap_matches)
        overlap_fp += len(actual) - len(overlap_matches)
        overlap_fn += len(expected) - len(overlap_matches)
        message_exact = expected == actual
        exact_messages += int(message_exact)

        scenario = str(row.get("scenario_family", "unspecified"))
        scenario_result = scenario_counts.setdefault(
            scenario,
            {"rows": 0, "exact_messages": 0},
        )
        scenario_result["rows"] += 1
        scenario_result["exact_messages"] += int(message_exact)

        if not message_exact:
            failures.append(
                {
                    "id": row.get("id"),
                    "message": message,
                    "scenario_family": scenario,
                    "context_style": row.get("context_style"),
                    "address_style": row.get("address_style"),
                    "boundary_style": row.get("boundary_style"),
                    "ambiguity": row.get("ambiguity"),
                    "expected": [
                        {"text": message[start:end], "span": [start, end]}
                        for start, end in expected
                    ],
                    "actual": [
                        {
                            "text": item.text,
                            "span": [item.start, item.end],
                            "confidence": round(item.confidence, 4),
                            "signals": list(item.signals),
                        }
                        for item in detected
                    ],
                    "failure_reasons": _failure_reasons(expected, actual),
                    "notes": row.get("notes"),
                }
            )

    scenario_report = {
        name: {
            **values,
            "exact_message_rate": round(
                _ratio(values["exact_messages"], values["rows"]),
                6,
            ),
        }
        for name, values in sorted(scenario_counts.items())
    }
    return {
        "rows": row_count,
        "positive_rows": positive_rows,
        "negative_rows": negative_rows,
        "scope": (
            "small, manually authored detection-behavior fixture; suitable for "
            "regression, not a production accuracy claim"
        ),
        "metric_definitions": {
            "exact_span_micro": (
                "one-to-one precision, recall, and F1 requiring exact message "
                "boundaries"
            ),
            "overlap_span_micro": (
                "one-to-one precision, recall, and F1 requiring any character "
                "overlap; reported separately because it is lenient"
            ),
            "exact_message_rate": (
                "fraction of messages where the complete ordered span list is "
                "exact"
            ),
            "negative_message_specificity": (
                "fraction of annotated negative messages returning no spans"
            ),
        },
        "exact_span_micro": _prf(exact_tp, exact_fp, exact_fn),
        "overlap_span_micro": _prf(overlap_tp, overlap_fp, overlap_fn),
        "exact_message_rate": round(_ratio(exact_messages, row_count), 6),
        "negative_message_specificity": round(
            _ratio(correctly_empty, negative_rows),
            6,
        ),
        "scenarios": scenario_report,
        "failure_sample": failures,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data",
        type=Path,
        default=ROOT / "evaluation/detection_reference.jsonl",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "evaluation/detection_report.json",
    )
    args = parser.parse_args(argv)

    report = evaluate(load_rows(args.data))
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(f"{rendered}\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
