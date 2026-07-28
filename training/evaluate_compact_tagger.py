"""Evaluate the bundled model on the untouched canonical-group test split."""

from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from address_normalizer.tagger import CompactSequenceTagger
from training.real_corpus import (
    build_examples,
    corpus_summary,
    load_reference_rows,
)
from training.train_compact_tagger import score_sequences


MINIMUMS = {
    "token_accuracy": 0.95,
    "sequence_accuracy": 0.90,
    "macro_entity_f1": 0.95,
    "micro_entity_f1": 0.95,
}
MAXIMUM_MODEL_BYTES = 250_000


def main() -> int:
    data_path = ROOT / "evaluation/legacy_reference_500.jsonl"
    model_path = ROOT / "src/address_normalizer/data/model.json"
    examples = build_examples(load_reference_rows(data_path))
    test_examples = [example for example in examples if example.split == "test"]
    summary = corpus_summary(examples)
    metrics = score_sequences(
        CompactSequenceTagger.from_package(),
        test_examples,
    )
    supported_labels = [
        label
        for label, values in metrics["labels"].items()
        if label != "O" and values["support"]
    ]
    unsupported_labels = [
        label
        for label, values in metrics["labels"].items()
        if label != "O" and not values["support"]
    ]
    gates = [
        {
            "metric": name,
            "actual": metrics[name],
            "minimum": minimum,
            "passed": metrics[name] >= minimum,
        }
        for name, minimum in MINIMUMS.items()
    ]
    gates.extend(
        (
            {
                "metric": "model_bytes",
                "actual": model_path.stat().st_size,
                "maximum": MAXIMUM_MODEL_BYTES,
                "passed": model_path.stat().st_size <= MAXIMUM_MODEL_BYTES,
            },
            {
                "metric": "leaking_groups",
                "actual": len(summary["leaking_groups"]),
                "maximum": 0,
                "passed": not summary["leaking_groups"],
            },
        )
    )
    report = {
        "scope": (
            "untuned test split grouped by canonical address; source provenance "
            "must be resolved before redistribution"
        ),
        "test_groups": summary["splits"]["test"]["groups"],
        "test_examples": len(test_examples),
        "supported_test_labels": supported_labels,
        "unsupported_test_labels": unsupported_labels,
        "metrics": metrics,
        "gates": gates,
        "passed": all(gate["passed"] for gate in gates),
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
