"""Score exact building components on the filtered Moscow registry test split."""

from __future__ import annotations

import argparse
from collections import Counter
import gzip
import json
from pathlib import Path
import sys
import time
from typing import Any, Callable, Iterable


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from address_normalizer import parse
from address_normalizer.types import ParsedAddress

from datamos_data import (
    DATASET_DATE,
    DATASET_ID,
    DATASET_VERSION,
    DEFAULT_FILTERED,
    FIELDS,
    fold,
)


def _predicted_values(result: ParsedAddress) -> dict[str, str | None]:
    street_parts = [
        part
        for part in (result.street, result.street_type)
        if part is not None
    ]
    street = (
        result.raw[
            min(part.start for part in street_parts) :
            max(part.end for part in street_parts)
        ]
        if street_parts
        else None
    )
    return {
        "street": street,
        "house_num": result.house_num.value if result.house_num else None,
        "corpus": result.corpus.value if result.corpus else None,
        "structure": result.structure.value if result.structure else None,
    }


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


def score(
    rows: Iterable[dict[str, Any]],
    parse_address: Callable[[str], ParsedAddress] = parse,
) -> dict[str, Any]:
    counts = {
        field: {"tp": 0, "fp": 0, "fn": 0, "support": 0}
        for field in FIELDS
    }
    failures: list[dict[str, Any]] = []
    tiers: Counter[str] = Counter()
    row_count = exact_rows = no_unparsed_rows = 0
    started = time.monotonic()
    for row in rows:
        row_count += 1
        tiers[row["tier"]] += 1
        result = parse_address(row["raw"])
        predicted = _predicted_values(result)
        mismatches: dict[str, dict[str, str | None]] = {}
        for field in FIELDS:
            wanted_value = row["expected"].get(field)
            actual_value = predicted[field]
            wanted = fold(wanted_value) if wanted_value is not None else None
            actual = fold(actual_value) if actual_value is not None else None
            field_counts = counts[field]
            if wanted is not None:
                field_counts["support"] += 1
            if wanted == actual:
                if wanted is not None:
                    field_counts["tp"] += 1
            else:
                if actual is not None:
                    field_counts["fp"] += 1
                if wanted is not None:
                    field_counts["fn"] += 1
                mismatches[field] = {
                    "expected": wanted_value,
                    "actual": actual_value,
                }
        if not mismatches:
            exact_rows += 1
        elif len(failures) < 50:
            failures.append(
                {
                    "source_row": row["source_row"],
                    "fias_id": row["fias_id"],
                    "tier": row["tier"],
                    "raw": row["raw"],
                    "mismatches": mismatches,
                }
            )
        if not result.unparsed:
            no_unparsed_rows += 1

    fields = {field: _metrics(values) for field, values in counts.items()}
    totals = {
        key: sum(values[key] for values in counts.values())
        for key in ("tp", "fp", "fn", "support")
    }
    elapsed = time.monotonic() - started
    exact_component_value_micro = _metrics(totals)
    return {
        "rows": row_count,
        "tiers": dict(sorted(tiers.items())),
        "matching": (
            "case-insensitive exact component value after whitespace and ё/е "
            "folding; street includes its source type marker"
        ),
        "metric_definitions": {
            "exact_component_value_micro": (
                "micro precision, recall, and F1 over case-insensitive exact "
                "component values after whitespace and ё/е folding"
            ),
            "exact_address_rate": (
                "fraction of rows where every scored component value matches"
            ),
            "no_unparsed_rate": (
                "fraction of rows with no residual word or number spans"
            ),
            "fields": "per-field exact component-value metrics",
        },
        "exact_component_value_micro": exact_component_value_micro,
        # Retained for compatibility with the first published report.
        "micro": exact_component_value_micro,
        "macro_field_f1": round(
            sum(float(value["f1"]) for value in fields.values())
            / len(fields),
            6,
        ),
        "exact_address_rate": round(
            exact_rows / row_count if row_count else 0.0,
            6,
        ),
        "no_unparsed_rate": round(
            no_unparsed_rows / row_count if row_count else 0.0,
            6,
        ),
        "fields": fields,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_second": round(row_count / elapsed, 1) if elapsed else None,
        "failure_sample": failures,
    }


def load_rows(
    path: Path,
    *,
    split: str = "test",
    tier: str | None = None,
    limit: int | None = None,
) -> Iterable[dict[str, Any]]:
    yielded = 0
    with gzip.open(path, "rt", encoding="utf-8") as source:
        for line in source:
            row = json.loads(line)
            if row.get("split") != split:
                continue
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
            "metric_definitions",
            "exact_component_value_micro",
        }
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=DEFAULT_FILTERED)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if not args.data.exists():
        parser.error(f"{args.data} does not exist; run prepare_datamos.py first")
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit must be positive")

    report = {
        "scope": (
            "untuned exact-value evaluation on a group-disjoint test split of "
            "active official Moscow registry building addresses"
        ),
        "source": {
            "dataset_id": DATASET_ID,
            "version": DATASET_VERSION,
            "release_date": DATASET_DATE,
        },
        "limitations": [
            "October 2021 snapshot; not current FIAS/GAR truth",
            "Moscow-only clean legal/simplified address formatting",
            "administrative fields and address existence resolution are unscored",
        ],
        **score(load_rows(args.data, limit=args.limit)),
        "slices": {
            tier: _summary(
                score(load_rows(args.data, tier=tier, limit=args.limit))
            )
            for tier in (
                "house_only",
                "house_corpus",
                "house_structure",
                "house_corpus_structure",
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
