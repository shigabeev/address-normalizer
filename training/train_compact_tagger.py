"""Train the dependency-free sequence tagger on real address components."""

from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import random
import sys
from typing import Any, Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from address_normalizer.parser import AddressParser
from address_normalizer.tagger import (
    CompactSequenceTagger,
    END,
    START,
    token_features,
)
from evaluation.evaluate import score_rows
from training.real_corpus import (
    SequenceExample,
    build_examples,
    corpus_summary,
    load_reference_rows,
)


LABELS = ("O", "REGION", "DISTRICT", "CITY", "SETTLEMENT", "STREET")


def update_sequence(
    emissions: defaultdict[str, float],
    transitions: defaultdict[str, float],
    example: SequenceExample,
    predicted: Sequence[str],
) -> None:
    for index, (gold_label, predicted_label) in enumerate(
        zip(example.labels, predicted)
    ):
        if gold_label == predicted_label:
            continue
        for feature in token_features(example.tokens, index):
            emissions[f"{gold_label}\t{feature}"] += 1.0
            emissions[f"{predicted_label}\t{feature}"] -= 1.0

    for labels, direction in ((example.labels, 1.0), (predicted, -1.0)):
        previous = START
        for label in labels:
            transitions[f"{previous}\t{label}"] += direction
            previous = label
        transitions[f"{previous}\t{END}"] += direction


def _averaged_weights(
    totals: defaultdict[str, float],
    epochs: int,
) -> dict[str, float]:
    return {
        key: round(value / epochs, 6)
        for key, value in sorted(totals.items())
        if round(value / epochs, 6)
    }


def fit(
    examples: Sequence[SequenceExample],
    *,
    seed: int,
    epochs: int,
) -> tuple[CompactSequenceTagger, dict[str, float], dict[str, float]]:
    randomizer = random.Random(seed)
    order = list(examples)
    emissions: defaultdict[str, float] = defaultdict(float)
    transitions: defaultdict[str, float] = defaultdict(float)
    emission_totals: defaultdict[str, float] = defaultdict(float)
    transition_totals: defaultdict[str, float] = defaultdict(float)

    for _ in range(epochs):
        randomizer.shuffle(order)
        for example in order:
            model = CompactSequenceTagger(LABELS, emissions, transitions)
            predicted, _ = model.tag(example.tokens)
            if tuple(predicted) != example.labels:
                update_sequence(emissions, transitions, example, predicted)
        for key, value in emissions.items():
            emission_totals[key] += value
        for key, value in transitions.items():
            transition_totals[key] += value

    averaged_emissions = _averaged_weights(emission_totals, epochs)
    averaged_transitions = _averaged_weights(transition_totals, epochs)
    return (
        CompactSequenceTagger(
            LABELS,
            averaged_emissions,
            averaged_transitions,
        ),
        averaged_emissions,
        averaged_transitions,
    )


def score_sequences(
    model: CompactSequenceTagger,
    examples: Sequence[SequenceExample],
) -> dict[str, Any]:
    counts = {
        label: {"tp": 0, "fp": 0, "fn": 0, "support": 0}
        for label in LABELS
    }
    token_total = token_correct = sequence_correct = 0
    failures: list[dict[str, Any]] = []

    for example in examples:
        predicted, _ = model.tag(example.tokens)
        token_total += len(example.labels)
        token_correct += sum(
            actual == wanted
            for actual, wanted in zip(predicted, example.labels)
        )
        is_correct = tuple(predicted) == example.labels
        sequence_correct += is_correct
        if not is_correct and len(failures) < 30:
            failures.append(
                {
                    "id": example.id,
                    "view": example.view,
                    "tokens": [token.text for token in example.tokens],
                    "expected": list(example.labels),
                    "actual": predicted,
                }
            )
        for wanted, actual in zip(example.labels, predicted):
            counts[wanted]["support"] += 1
            if wanted == actual:
                counts[wanted]["tp"] += 1
            else:
                counts[actual]["fp"] += 1
                counts[wanted]["fn"] += 1

    fields: dict[str, dict[str, float | int]] = {}
    entity_f1: list[float] = []
    micro_tp = micro_fp = micro_fn = 0
    for label, values in counts.items():
        tp, fp, fn = values["tp"], values["fp"], values["fn"]
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1 = (
            2 * precision * recall / (precision + recall)
            if precision + recall
            else 0.0
        )
        fields[label] = {
            **values,
            "precision": round(precision, 6),
            "recall": round(recall, 6),
            "f1": round(f1, 6),
        }
        if label != "O":
            micro_tp += tp
            micro_fp += fp
            micro_fn += fn
            if values["support"]:
                entity_f1.append(f1)

    micro_precision = (
        micro_tp / (micro_tp + micro_fp) if micro_tp + micro_fp else 0.0
    )
    micro_recall = (
        micro_tp / (micro_tp + micro_fn) if micro_tp + micro_fn else 0.0
    )
    micro_f1 = (
        2 * micro_precision * micro_recall / (micro_precision + micro_recall)
        if micro_precision + micro_recall
        else 0.0
    )
    return {
        "examples": len(examples),
        "tokens": token_total,
        "token_accuracy": round(
            token_correct / token_total if token_total else 0.0,
            6,
        ),
        "sequence_accuracy": round(
            sequence_correct / len(examples) if examples else 0.0,
            6,
        ),
        "macro_entity_f1": round(
            sum(entity_f1) / len(entity_f1) if entity_f1 else 0.0,
            6,
        ),
        "micro_entity_f1": round(micro_f1, 6),
        "labels": fields,
        "failure_sample": failures,
    }


def _model_payload(
    *,
    emissions: dict[str, float],
    transitions: dict[str, float],
    seed: int,
    epochs: int,
    examples: int,
    dataset_sha256: str,
) -> dict[str, Any]:
    return {
        "format": "address-normalizer-compact-sequence-v1",
        "training": {
            "algorithm": "epoch-averaged structured perceptron",
            "seed": seed,
            "epochs": epochs,
            "examples": examples,
            "source": (
                "source-verifiable legacy reference rows with deterministic "
                "marker-free views"
            ),
            "dataset": "evaluation/legacy_reference_500.jsonl",
            "dataset_sha256": dataset_sha256,
            "split": "SHA-256 by canonical address group: 70/15/15",
        },
        "labels": list(LABELS),
        "emissions": emissions,
        "transitions": transitions,
    }


def _serialized_size(payload: dict[str, Any]) -> int:
    return len(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def _without_large_failures(report: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in report.items()
        if key != "failure_sample"
    }


def train(
    *,
    data_path: Path,
    baseline_path: Path,
    seed: int = 2017,
    epoch_grid: Sequence[int] = (5, 10, 20, 40, 80),
) -> tuple[dict[str, Any], dict[str, Any]]:
    rows = load_reference_rows(data_path)
    examples = build_examples(rows)
    train_examples = [example for example in examples if example.split == "train"]
    validation_examples = [
        example for example in examples if example.split == "validation"
    ]
    test_examples = [example for example in examples if example.split == "test"]
    if not train_examples or not validation_examples or not test_examples:
        raise ValueError("real corpus must have non-empty train/validation/test splits")

    baseline = CompactSequenceTagger.from_path(baseline_path)
    tuning: list[dict[str, Any]] = []
    candidates: list[
        tuple[
            tuple[float, float, int],
            int,
            CompactSequenceTagger,
            dict[str, float],
            dict[str, float],
        ]
    ] = []
    dataset_sha256 = hashlib.sha256(data_path.read_bytes()).hexdigest()

    for epochs in epoch_grid:
        model, emissions, transitions = fit(
            train_examples,
            seed=seed,
            epochs=epochs,
        )
        metrics = score_sequences(model, validation_examples)
        provisional = _model_payload(
            emissions=emissions,
            transitions=transitions,
            seed=seed,
            epochs=epochs,
            examples=len(train_examples),
            dataset_sha256=dataset_sha256,
        )
        model_bytes = _serialized_size(provisional)
        tuning.append(
            {
                "epochs": epochs,
                "model_bytes": model_bytes,
                "validation": _without_large_failures(metrics),
            }
        )
        objective = (
            float(metrics["macro_entity_f1"]),
            float(metrics["sequence_accuracy"]),
            -model_bytes,
        )
        candidates.append(
            (objective, epochs, model, emissions, transitions)
        )

    _, selected_epochs, _, _, _ = max(candidates, key=lambda item: item[0])
    final_training = [*train_examples, *validation_examples]
    candidate, emissions, transitions = fit(
        final_training,
        seed=seed,
        epochs=selected_epochs,
    )
    payload = _model_payload(
        emissions=emissions,
        transitions=transitions,
        seed=seed,
        epochs=selected_epochs,
        examples=len(final_training),
        dataset_sha256=dataset_sha256,
    )

    test_rows_by_id = {
        str(example.row["id"]): example.row
        for example in test_examples
    }
    test_rows = list(test_rows_by_id.values())
    baseline_parser = AddressParser(tagger=baseline)
    candidate_parser = AddressParser(tagger=candidate)
    baseline_end_to_end = score_rows(test_rows, baseline_parser.parse)
    candidate_end_to_end = score_rows(test_rows, candidate_parser.parse)
    try:
        dataset_name = str(data_path.relative_to(ROOT))
    except ValueError:
        dataset_name = str(data_path)

    report = {
        "scope": (
            "group-disjoint real-address model evaluation; source and "
            "derived-model provenance are recorded in LICENSING.md"
        ),
        "dataset": dataset_name,
        "dataset_sha256": dataset_sha256,
        "corpus": corpus_summary(examples),
        "tuning": tuning,
        "selected_epochs": selected_epochs,
        "test_sequence": {
            "synthetic_starter": score_sequences(baseline, test_examples),
            "real_model": score_sequences(candidate, test_examples),
        },
        "test_end_to_end": {
            "rows": len(test_rows),
            "synthetic_starter": _without_large_failures(baseline_end_to_end),
            "real_model": _without_large_failures(candidate_end_to_end),
        },
        "model_bytes": {
            "synthetic_starter": len(
                json.dumps(
                    json.loads(baseline_path.read_text(encoding="utf-8")),
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            ),
            "real_model": _serialized_size(payload),
        },
    }
    return payload, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data",
        type=Path,
        default=ROOT / "evaluation/legacy_reference_500.jsonl",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "src/address_normalizer/data/model.json",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=ROOT / "training/model_evaluation.json",
    )
    parser.add_argument(
        "--baseline-model",
        type=Path,
        default=ROOT / "training/baselines/synthetic_model.json",
    )
    parser.add_argument("--seed", type=int, default=2017)
    parser.add_argument("--epoch-grid", default="5,10,20,40,80")
    args = parser.parse_args(argv)
    epoch_grid = tuple(
        int(value)
        for value in args.epoch_grid.split(",")
        if value.strip()
    )
    if not epoch_grid or any(value <= 0 for value in epoch_grid):
        raise ValueError("epoch grid must contain positive integers")

    payload, report = train(
        data_path=args.data,
        baseline_path=args.baseline_model,
        seed=args.seed,
        epoch_grid=epoch_grid,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        f"{json.dumps(report, ensure_ascii=False, indent=2)}\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "output": str(args.output),
                "bytes": args.output.stat().st_size,
                "report": str(args.report),
                "selected_epochs": report["selected_epochs"],
                "test_sequence": {
                    name: {
                        key: metrics[key]
                        for key in (
                            "token_accuracy",
                            "sequence_accuracy",
                            "macro_entity_f1",
                            "micro_entity_f1",
                        )
                    }
                    for name, metrics in report["test_sequence"].items()
                },
                "test_end_to_end": {
                    name: {
                        key: metrics[key]
                        for key in (
                            "exact_address_rate",
                            "no_unparsed_rate",
                        )
                    }
                    for name, metrics in report["test_end_to_end"].items()
                    if name != "rows"
                },
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
