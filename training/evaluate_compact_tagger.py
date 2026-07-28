"""Honest smoke evaluation on place names absent from the starter corpus."""

from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from address_normalizer.tagger import CompactSequenceTagger
from address_normalizer.tokenizer import tokenize


HOLDOUT = (
    ("Курск Советская", ("CITY", "STREET")),
    ("Иркутск Молодежная", ("CITY", "STREET")),
    ("Уфа Красная", ("CITY", "STREET")),
    ("Сочи Северная", ("CITY", "STREET")),
    ("Волгоград Победы", ("CITY", "STREET")),
    ("Орёл Кирова", ("CITY", "STREET")),
)


def main() -> int:
    model = CompactSequenceTagger.from_package()
    token_total = 0
    token_correct = 0
    sequences_correct = 0
    failures: list[dict[str, object]] = []

    for text, expected in HOLDOUT:
        tokens = [token for token in tokenize(text) if token.is_word]
        predicted, _ = model.tag(tokens)
        token_total += len(expected)
        token_correct += sum(
            actual == wanted for actual, wanted in zip(predicted, expected)
        )
        sequences_correct += tuple(predicted) == expected
        if tuple(predicted) != expected:
            failures.append(
                {
                    "text": text,
                    "expected": expected,
                    "predicted": predicted,
                }
            )

    report = {
        "scope": "unseen-name smoke corpus; not a production benchmark",
        "examples": len(HOLDOUT),
        "token_accuracy": token_correct / token_total,
        "full_sequence_accuracy": sequences_correct / len(HOLDOUT),
        "failures": failures,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
