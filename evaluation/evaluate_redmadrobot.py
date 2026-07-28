"""Evaluate address extraction on an independent Russian NER benchmark."""

from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Callable, Iterable, Sequence
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from address_normalizer import parse
from address_normalizer.types import ParsedAddress


DATASET_REVISION = "f77ea831274daf980cc45c61a93c226be9d978d6"
DATASET_SHA256 = "6bf544a380a3ee5bec94b946124bea3afaecce49e734679ad0f0c0e7c12977bb"
DATASET_URL = (
    "https://huggingface.co/datasets/redmadrobot-rnd/pii_benchmark/resolve/"
    f"{DATASET_REVISION}/test.csv"
)
DEFAULT_DATA = (
    ROOT
    / ".cache"
    / f"redmadrobot-pii-benchmark-{DATASET_REVISION[:8]}.csv"
)
LOCATION_LABELS = {
    "COUNTRY",
    "REGION",
    "DISTRICT",
    "CITY",
    "STREET",
    "HOUSE",
}
SCORED_LABELS = ("REGION", "DISTRICT", "CITY", "STREET", "HOUSE")
PART_LABELS = {
    "region": "REGION",
    "district": "DISTRICT",
    "city": "CITY",
    "settlement": "CITY",
    "street": "STREET",
    "street_type": "STREET",
    "house_num": "HOUSE",
    "corpus": "HOUSE",
    "structure": "HOUSE",
    "apartment": "HOUSE",
}


@dataclass(frozen=True, slots=True)
class Span:
    label: str
    start: int
    end: int


@dataclass(frozen=True, slots=True)
class AddressSnippet:
    source_row: int
    text: str
    tokens: tuple[str, ...]
    tags: tuple[str, ...]
    offsets: tuple[tuple[int, int], ...]


def _base_label(tag: str) -> str:
    return tag[2:] if tag.startswith(("B-", "I-")) else tag


def _reconstruct(tokens: Sequence[str]) -> tuple[str, tuple[tuple[int, int], ...]]:
    text_parts: list[str] = []
    offsets: list[tuple[int, int]] = []
    cursor = 0
    for index, token in enumerate(tokens):
        if index:
            text_parts.append(" ")
            cursor += 1
        start = cursor
        text_parts.append(token)
        cursor += len(token)
        offsets.append((start, cursor))
    return "".join(text_parts), tuple(offsets)


def _clusters(tags: Sequence[str], max_gap: int) -> list[tuple[int, int]]:
    location_indices = [
        index
        for index, tag in enumerate(tags)
        if _base_label(tag) in LOCATION_LABELS
    ]
    if not location_indices:
        return []
    clusters: list[tuple[int, int]] = []
    start = previous = location_indices[0]
    for index in location_indices[1:]:
        if index - previous - 1 > max_gap:
            clusters.append((start, previous + 1))
            start = index
        previous = index
    clusters.append((start, previous + 1))
    return clusters


def load_snippets(path: Path, *, max_gap: int = 3) -> list[AddressSnippet]:
    snippets: list[AddressSnippet] = []
    with path.open(encoding="utf-8", newline="") as source:
        for row_number, row in enumerate(csv.DictReader(source)):
            tokens = tuple(json.loads(row["tokens"]))
            tags = tuple(json.loads(row["ner_tags"]))
            if len(tokens) != len(tags):
                raise ValueError(f"row {row_number}: token/tag length mismatch")
            for start, end in _clusters(tags, max_gap):
                selected_tags = tags[start:end]
                if not any(
                    _base_label(tag) in SCORED_LABELS
                    for tag in selected_tags
                ):
                    continue
                selected_tokens = tokens[start:end]
                text, offsets = _reconstruct(selected_tokens)
                snippets.append(
                    AddressSnippet(
                        source_row=row_number,
                        text=text,
                        tokens=selected_tokens,
                        tags=selected_tags,
                        offsets=offsets,
                    )
                )
    return snippets


def _gold_spans(snippet: AddressSnippet) -> list[Span]:
    spans: list[Span] = []
    active_label: str | None = None
    active_start = 0
    active_end = 0
    for tag, (start, end) in zip(snippet.tags, snippet.offsets):
        label = _base_label(tag)
        continues = tag.startswith("I-") and label == active_label
        if active_label is not None and not continues:
            if active_label in SCORED_LABELS:
                spans.append(Span(active_label, active_start, active_end))
            active_label = None
        if label not in LOCATION_LABELS:
            continue
        if active_label is None:
            active_label = label
            active_start = start
        active_end = end
    if active_label in SCORED_LABELS:
        spans.append(Span(active_label, active_start, active_end))
    return spans


def _predicted_spans(result: ParsedAddress) -> list[Span]:
    grouped: dict[str, list[tuple[int, int]]] = {}
    for field, label in PART_LABELS.items():
        part = getattr(result, field)
        if part is not None:
            grouped.setdefault(label, []).append((part.start, part.end))
    return [
        Span(
            label=label,
            start=min(start for start, _ in offsets),
            end=max(end for _, end in offsets),
        )
        for label, offsets in sorted(grouped.items())
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


def _prf(values: dict[str, int]) -> dict[str, int | float]:
    precision = (
        values["tp"] / (values["tp"] + values["fp"])
        if values["tp"] + values["fp"]
        else 0.0
    )
    recall = (
        values["tp"] / (values["tp"] + values["fn"])
        if values["tp"] + values["fn"]
        else 0.0
    )
    f1 = (
        2 * precision * recall / (precision + recall)
        if precision + recall
        else 0.0
    )
    return {
        **values,
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "f1": round(f1, 6),
    }


def evaluate(
    snippets: Iterable[AddressSnippet],
    parse_address: Callable[[str], ParsedAddress] = parse,
) -> dict[str, Any]:
    counts = {
        label: {"tp": 0, "fp": 0, "fn": 0, "support": 0}
        for label in SCORED_LABELS
    }
    failures: list[dict[str, Any]] = []
    snippet_count = exact_span_matches = overlap_matches = 0
    source_rows: set[int] = set()

    for snippet in snippets:
        snippet_count += 1
        source_rows.add(snippet.source_row)
        gold = _gold_spans(snippet)
        predicted = _predicted_spans(parse_address(snippet.text))
        matches, matched_gold, matched_predicted = _match(gold, predicted)
        overlap_matches += len(matches)
        exact_span_matches += sum(
            gold[gold_index] == predicted[predicted_index]
            for gold_index, predicted_index in matches
        )

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

        if (
            len(matched_gold) != len(gold)
            or len(matched_predicted) != len(predicted)
        ) and len(failures) < 50:
            failures.append(
                {
                    "source_row": snippet.source_row,
                    "text": snippet.text,
                    "gold": [asdict(span) for span in gold],
                    "predicted": [asdict(span) for span in predicted],
                }
            )

    fields = {label: _prf(values) for label, values in counts.items()}
    totals = {
        name: sum(values[name] for values in counts.values())
        for name in ("tp", "fp", "fn", "support")
    }
    supported_f1 = [
        float(values["f1"])
        for values in fields.values()
        if values["support"]
    ]
    span_overlap_micro = _prf(totals)
    return {
        "source_rows": len(source_rows),
        "address_snippets": snippet_count,
        "matching": (
            "one-to-one same-label span overlap; address windows are oracle-"
            "cropped from the benchmark's BIO annotations"
        ),
        "metric_definitions": {
            "span_overlap_micro": (
                "micro precision, recall, and F1 for one-to-one same-label "
                "spans with any character overlap"
            ),
            "exact_span_recall": (
                "exact-boundary same-label matches divided by gold span count"
            ),
            "fields": "per-field span-overlap precision, recall, and F1",
        },
        "span_overlap_micro": span_overlap_micro,
        # Retained for compatibility with the first published report.
        "micro": span_overlap_micro,
        "macro_field_f1": round(
            sum(supported_f1) / len(supported_f1) if supported_f1 else 0.0,
            6,
        ),
        "overlap_matches": overlap_matches,
        "exact_span_matches": exact_span_matches,
        "exact_span_recall": round(
            exact_span_matches / totals["support"] if totals["support"] else 0.0,
            6,
        ),
        "fields": fields,
        "failure_sample": failures,
    }


def _without_failures(report: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in report.items()
        if key
        not in {
            "failure_sample",
            "metric_definitions",
            "span_overlap_micro",
        }
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_dataset(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.part")
    request = Request(DATASET_URL, headers={"User-Agent": "address-normalizer/2"})
    with urlopen(request, timeout=60) as response, temporary.open("wb") as output:
        while chunk := response.read(1024 * 1024):
            output.write(chunk)
    actual_sha256 = _sha256(temporary)
    if actual_sha256 != DATASET_SHA256:
        temporary.unlink(missing_ok=True)
        raise ValueError(
            "external benchmark checksum mismatch: "
            f"expected {DATASET_SHA256}, got {actual_sha256}"
        )
    temporary.replace(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA)
    parser.add_argument(
        "--download",
        action="store_true",
        help="download the pinned benchmark revision before evaluation",
    )
    parser.add_argument("--max-gap", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    if args.download:
        download_dataset(args.data)
    if not args.data.exists():
        parser.error(
            f"{args.data} does not exist; provide --data or run with --download"
        )
    actual_sha256 = _sha256(args.data)
    if actual_sha256 != DATASET_SHA256:
        parser.error(
            "benchmark checksum mismatch: "
            f"expected {DATASET_SHA256}, got {actual_sha256}"
        )
    if args.max_gap < 0:
        parser.error("--max-gap must be non-negative")

    snippets = load_snippets(args.data, max_gap=args.max_gap)
    multi_field = [
        snippet
        for snippet in snippets
        if len({span.label for span in _gold_spans(snippet)}) >= 2
    ]
    street_and_house = [
        snippet
        for snippet in snippets
        if {"STREET", "HOUSE"}
        <= {span.label for span in _gold_spans(snippet)}
    ]
    administrative_only = [
        snippet
        for snippet in snippets
        if {span.label for span in _gold_spans(snippet)}
        <= {"REGION", "DISTRICT", "CITY"}
    ]
    report = {
        "scope": (
            "independent, untuned external evaluation on address snippets from "
            "the RedMadRobot Russian PII NER benchmark"
        ),
        "source": {
            "repository": "redmadrobot-rnd/pii_benchmark",
            "license": "MIT",
            "revision": DATASET_REVISION,
            "sha256": actual_sha256,
            "url": DATASET_URL,
            "limitations": (
                "production-log-shaped and manually annotated, with real "
                "personal values replaced; includes synthetic document-style "
                "examples and hard negatives"
            ),
        },
        "windowing": {
            "max_non_location_tokens_between_spans": args.max_gap,
            "country_is_context_only": True,
        },
        **evaluate(snippets),
        "slices": {
            "multi_field": _without_failures(evaluate(multi_field)),
            "street_and_house": _without_failures(evaluate(street_and_house)),
            "administrative_only": _without_failures(
                evaluate(administrative_only)
            ),
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
