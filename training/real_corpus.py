"""Build deterministic real-address sequence examples without data leakage."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Sequence

from address_normalizer.parser import AddressParser
from address_normalizer.tokenizer import Token, tokenize


FIELD_LABELS = {
    "region": "REGION",
    "district": "DISTRICT",
    "city": "CITY",
    "settlement": "SETTLEMENT",
    "street": "STREET",
}
GROUP_FIELDS = (
    "region",
    "district",
    "city",
    "settlement",
    "street",
    "street_type",
)


@dataclass(frozen=True, slots=True)
class SequenceExample:
    id: str
    group: str
    split: str
    view: str
    tokens: tuple[Token, ...]
    labels: tuple[str, ...]
    row: dict[str, Any]


class _ResidualRecorder:
    def __init__(self) -> None:
        self.tokens: tuple[Token, ...] = ()

    def tag(self, tokens: Sequence[Token]) -> tuple[list[str], list[float]]:
        self.tokens = tuple(tokens)
        return ["O"] * len(tokens), [0.0] * len(tokens)


def load_reference_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        1,
    ):
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict) or not isinstance(row.get("expected"), dict):
            raise ValueError(f"{path}:{line_number}: invalid reference row")
        rows.append(row)
    return rows


def _words(value: str) -> tuple[str, ...]:
    return tuple(token.lower for token in tokenize(value) if token.is_word)


def _group_key(row: dict[str, Any]) -> str:
    expected = row["expected"]
    parts = [
        f"{field}={' '.join(_words(str(expected.get(field, ''))))}"
        for field in GROUP_FIELDS
    ]
    return "|".join(parts)


def _split(group: str) -> str:
    digest = hashlib.sha256(f"address-normalizer-model-v1\t{group}".encode()).digest()
    bucket = int.from_bytes(digest[:8], "big") % 100
    if bucket < 70:
        return "train"
    if bucket < 85:
        return "validation"
    return "test"


def _residual_tokens(raw: str) -> tuple[Token, ...]:
    recorder = _ResidualRecorder()
    AddressParser(tagger=recorder).parse(raw)
    return recorder.tokens


def _matching_spans(raw: str, value: str) -> list[tuple[tuple[int, int], ...]]:
    source = tuple(token for token in tokenize(raw) if token.is_word)
    wanted = _words(value)
    if not wanted:
        return []
    matches: list[tuple[tuple[int, int], ...]] = []
    for start in range(len(source) - len(wanted) + 1):
        candidate = source[start : start + len(wanted)]
        if tuple(token.lower for token in candidate) == wanted:
            matches.append(tuple((token.start, token.end) for token in candidate))
    return matches


def build_examples(rows: Sequence[dict[str, Any]]) -> list[SequenceExample]:
    examples: list[SequenceExample] = []
    for row in rows:
        raw = row["raw"]
        residual = _residual_tokens(raw)
        if not residual:
            continue
        residual_spans = {(token.start, token.end) for token in residual}
        labels_by_span: dict[tuple[int, int], str] = {}
        conflict = False

        for field, label in FIELD_LABELS.items():
            value = row["expected"].get(field)
            if value is None:
                continue
            candidates = [
                spans
                for spans in _matching_spans(raw, str(value))
                if all(span in residual_spans for span in spans)
            ]
            if not candidates:
                continue
            spans = candidates[0]
            for span in spans:
                previous = labels_by_span.get(span)
                if previous is not None and previous != label:
                    conflict = True
                    break
                labels_by_span[span] = label
            if conflict:
                break

        if conflict:
            continue
        labels = tuple(
            labels_by_span.get((token.start, token.end), "O")
            for token in residual
        )
        group = _group_key(row)
        examples.append(
            SequenceExample(
                id=str(row["id"]),
                group=group,
                split=_split(group),
                view="observed_residual",
                tokens=residual,
                labels=labels,
                row=row,
            )
        )

        ordered_fields = [
            field
            for field in FIELD_LABELS
            if row["expected"].get(field) is not None
        ]
        deepest_locality = next(
            (
                field
                for field in ("settlement", "city", "district", "region")
                if field in ordered_fields
            ),
            None,
        )
        views: list[tuple[str, list[str]]] = [
            ("hierarchy_without_markers", ordered_fields),
        ]
        if deepest_locality and "street" in ordered_fields:
            views.append(("locality_and_street", [deepest_locality, "street"]))
        if "street" in ordered_fields:
            views.append(("street_only", ["street"]))
        if deepest_locality:
            views.append(("locality_only", [deepest_locality]))

        for view, fields in views:
            text_parts: list[str] = []
            derived_labels: list[str] = []
            for field in fields:
                value = str(row["expected"][field])
                text_parts.append(value)
                derived_labels.extend(
                    [FIELD_LABELS[field]]
                    * len([token for token in tokenize(value) if token.is_word])
                )
            derived_tokens = tuple(
                token for token in tokenize(" ".join(text_parts)) if token.is_word
            )
            if derived_tokens and len(derived_tokens) == len(derived_labels):
                examples.append(
                    SequenceExample(
                        id=f"{row['id']}:{view}",
                        group=group,
                        split=_split(group),
                        view=view,
                        tokens=derived_tokens,
                        labels=tuple(derived_labels),
                        row=row,
                    )
                )

    unique: dict[tuple[str, tuple[str, ...], tuple[str, ...]], SequenceExample] = {}
    for example in examples:
        key = (
            example.group,
            tuple(token.lower for token in example.tokens),
            example.labels,
        )
        unique.setdefault(key, example)
    return list(unique.values())


def corpus_summary(examples: Sequence[SequenceExample]) -> dict[str, Any]:
    splits: dict[str, dict[str, Any]] = {}
    for split in ("train", "validation", "test"):
        selected = [example for example in examples if example.split == split]
        label_counts: dict[str, int] = {}
        for example in selected:
            for label in example.labels:
                label_counts[label] = label_counts.get(label, 0) + 1
        splits[split] = {
            "examples": len(selected),
            "groups": len({example.group for example in selected}),
            "tokens": sum(len(example.tokens) for example in selected),
            "positive_sequences": sum(
                any(label != "O" for label in example.labels)
                for example in selected
            ),
            "views": dict(
                sorted(
                    {
                        view: sum(example.view == view for example in selected)
                        for view in {example.view for example in selected}
                    }.items()
                )
            ),
            "labels": dict(sorted(label_counts.items())),
        }
    group_splits: dict[str, set[str]] = {}
    for example in examples:
        group_splits.setdefault(example.group, set()).add(example.split)
    leaking = sorted(
        group
        for group, splits_for_group in group_splits.items()
        if len(splits_for_group) > 1
    )
    return {
        "examples": len(examples),
        "groups": len(group_splits),
        "splits": splits,
        "leaking_groups": leaking,
    }
