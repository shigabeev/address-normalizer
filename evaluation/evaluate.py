"""Evaluate parse-only field extraction against a JSONL reference set."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import sys
from typing import Any, Callable, Iterable

from address_normalizer import parse
from address_normalizer.types import ParsedAddress


FIELDS = (
    "postal_code",
    "region",
    "district",
    "city",
    "settlement",
    "street",
    "street_type",
    "house_num",
    "corpus",
    "structure",
    "apartment",
)


def _fold(value: str | None) -> str | None:
    if value is None:
        return None
    return " ".join(value.casefold().replace("ё", "е").split())


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict) or not isinstance(row.get("raw"), str):
            raise ValueError(f"{path}:{line_number}: expected an object with raw text")
        expected = row.get("expected")
        if not isinstance(expected, dict):
            raise ValueError(f"{path}:{line_number}: expected must be an object")
        unknown = set(expected) - set(FIELDS)
        if unknown:
            raise ValueError(
                f"{path}:{line_number}: unsupported expected fields: {sorted(unknown)}"
            )
        rows.append(row)
    if not rows:
        raise ValueError(f"{path}: no evaluation rows")
    return rows


def _safe_ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def score_rows(
    rows: Iterable[dict[str, Any]],
    parse_address: Callable[[str], ParsedAddress] = parse,
) -> dict[str, Any]:
    field_counts = {
        field: {"tp": 0, "fp": 0, "fn": 0, "support": 0}
        for field in FIELDS
    }
    failures: list[dict[str, Any]] = []
    review_statuses: Counter[str] = Counter()
    total = 0
    exact = 0
    no_unparsed = 0

    for row in rows:
        total += 1
        review_statuses[str(row.get("review_status", "unspecified"))] += 1
        result = parse_address(row["raw"])
        expected = row["expected"]
        row_mismatches: dict[str, dict[str, str | None]] = {}

        for field in FIELDS:
            expected_value = expected.get(field)
            part = getattr(result, field)
            actual_value = part.value if part else None
            wanted = _fold(str(expected_value)) if expected_value is not None else None
            actual = _fold(actual_value)
            counts = field_counts[field]

            if wanted is not None:
                counts["support"] += 1
            if actual == wanted:
                if wanted is not None:
                    counts["tp"] += 1
            else:
                if actual is not None:
                    counts["fp"] += 1
                if wanted is not None:
                    counts["fn"] += 1
                row_mismatches[field] = {
                    "expected": str(expected_value)
                    if expected_value is not None
                    else None,
                    "actual": actual_value,
                }

        if not row_mismatches:
            exact += 1
        elif len(failures) < 50:
            failures.append(
                {
                    "id": row.get("id"),
                    "raw": row["raw"],
                    "mismatches": row_mismatches,
                    "unparsed": [part.raw for part in result.unparsed],
                }
            )
        if not result.unparsed:
            no_unparsed += 1

    fields: dict[str, dict[str, float | int]] = {}
    micro_tp = micro_fp = micro_fn = 0
    for field, counts in field_counts.items():
        tp, fp, fn = counts["tp"], counts["fp"], counts["fn"]
        precision = _safe_ratio(tp, tp + fp)
        recall = _safe_ratio(tp, tp + fn)
        f1 = _safe_ratio(2 * precision * recall, precision + recall)
        fields[field] = {
            **counts,
            "precision": round(precision, 6),
            "recall": round(recall, 6),
            "f1": round(f1, 6),
        }
        micro_tp += tp
        micro_fp += fp
        micro_fn += fn

    micro_precision = _safe_ratio(micro_tp, micro_tp + micro_fp)
    micro_recall = _safe_ratio(micro_tp, micro_tp + micro_fn)
    micro_f1 = _safe_ratio(
        2 * micro_precision * micro_recall,
        micro_precision + micro_recall,
    )
    return {
        "rows": total,
        "review_statuses": dict(review_statuses),
        "exact_address_rate": round(_safe_ratio(exact, total), 6),
        "no_unparsed_rate": round(_safe_ratio(no_unparsed, total), 6),
        "micro": {
            "tp": micro_tp,
            "fp": micro_fp,
            "fn": micro_fn,
            "precision": round(micro_precision, 6),
            "recall": round(micro_recall, 6),
            "f1": round(micro_f1, 6),
        },
        "fields": fields,
        "failure_sample": failures,
    }


def _lookup(report: dict[str, Any], path: str) -> float | int:
    value: Any = report
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            raise KeyError(f"metric path not found: {path}")
        value = value[part]
    if not isinstance(value, (int, float)):
        raise TypeError(f"metric is not numeric: {path}")
    return value


def _check_gates(
    report: dict[str, Any],
    gates: dict[str, Any],
) -> list[dict[str, Any]]:
    outcomes: list[dict[str, Any]] = []
    minimum_rows = int(gates.get("minimum_rows", 0))
    outcomes.append(
        {
            "metric": "rows",
            "actual": report["rows"],
            "minimum": minimum_rows,
            "passed": report["rows"] >= minimum_rows,
        }
    )
    for path, minimum in gates.get("minimum_metrics", {}).items():
        actual = _lookup(report, path)
        outcomes.append(
            {
                "metric": path,
                "actual": actual,
                "minimum": minimum,
                "passed": actual >= minimum,
            }
        )
    return outcomes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data",
        type=Path,
        default=Path(__file__).with_name("legacy_reference_500.jsonl"),
    )
    parser.add_argument("--gates", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)

    report = score_rows(_load_jsonl(args.data))
    if args.gates:
        gates = json.loads(args.gates.read_text(encoding="utf-8"))
        report["gates"] = _check_gates(report, gates)
        report["release_gate_passed"] = all(
            outcome["passed"] for outcome in report["gates"]
        )

    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(f"{rendered}\n", encoding="utf-8")

    return 0 if report.get("release_gate_passed", True) else 1


if __name__ == "__main__":
    sys.exit(main())
