"""Train the bundled dependency-free linear-chain address tagger.

This starter corpus is intentionally compact and contains synthetic public place
names only. Real deployments should extend the generator with reviewed examples
while preserving an address-grouped evaluation split.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
from pathlib import Path
import random
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from address_normalizer.tagger import CompactSequenceTagger, END, START, token_features
from address_normalizer.tokenizer import Token, tokenize


LABELS = ("O", "REGION", "DISTRICT", "CITY", "SETTLEMENT", "STREET")

CITIES = (
    "Москва",
    "Санкт Петербург",
    "Самара",
    "Казань",
    "Тула",
    "Омск",
    "Пермь",
    "Владимир",
    "Видное",
    "Новошахтинск",
    "Красный Сулин",
    "Ростов на Дону",
    "Екатеринбург",
    "Новосибирск",
    "Саратов",
    "Тверь",
    "Калуга",
    "Ярославль",
)
STREETS = (
    "Ополченская",
    "Тверская",
    "Большая Садовая",
    "Невский",
    "Авроры",
    "Ленина",
    "Школьная",
    "Кузьминская",
    "Харьковская",
    "Октябрьская",
    "Больничная",
    "Сельская",
    "Коммунистический",
    "Рязанский",
    "Юрьевский",
    "Денисовский",
    "Московская",
    "Центральная",
    "Мира",
    "Гагарина",
    "Пушкина",
)
REGIONS = (
    "Московская",
    "Ростовская",
    "Тверская",
    "Тульская",
    "Калужская",
    "Самарская",
    "Саратовская",
    "Омская",
    "Новосибирская",
)
DISTRICTS = (
    "Ленинский",
    "Азовский",
    "Неклиновский",
    "Октябрьский",
    "Красносулинский",
    "Семикаракорский",
)
SETTLEMENTS = (
    "Красный Десант",
    "Самарское",
    "Вершинный",
    "Новосветловский",
    "Московский",
    "Котельники",
)


def labelled_words(phrase: str, label: str) -> tuple[list[Token], list[str]]:
    words = [token for token in tokenize(phrase) if token.is_word]
    return words, [label] * len(words)


def build_examples() -> list[tuple[list[Token], list[str]]]:
    examples: list[tuple[list[Token], list[str]]] = []
    for city in CITIES:
        examples.append(labelled_words(city, "CITY"))
    for street in STREETS:
        examples.append(labelled_words(street, "STREET"))
    for region in REGIONS:
        examples.append(labelled_words(region, "REGION"))
    for district in DISTRICTS:
        examples.append(labelled_words(district, "DISTRICT"))
    for settlement in SETTLEMENTS:
        examples.append(labelled_words(settlement, "SETTLEMENT"))

    for city_index, city in enumerate(CITIES):
        for offset in range(3):
            street = STREETS[(city_index * 3 + offset) % len(STREETS)]
            text = f"{city} {street}"
            city_tokens = [token for token in tokenize(city) if token.is_word]
            street_tokens = [
                token
                for token in tokenize(text[len(city) + 1 :])
                if token.is_word
            ]
            # Re-tokenize the complete example so offsets and context are correct.
            tokens = [token for token in tokenize(text) if token.is_word]
            labels = ["CITY"] * len(city_tokens) + ["STREET"] * len(street_tokens)
            examples.append((tokens, labels))

    for region_index, region in enumerate(REGIONS):
        district = DISTRICTS[region_index % len(DISTRICTS)]
        city = CITIES[region_index % len(CITIES)]
        street = STREETS[(region_index + 4) % len(STREETS)]
        text = f"{region} {district} {city} {street}"
        tokens = [token for token in tokenize(text) if token.is_word]
        labels = (
            ["REGION"]
            + ["DISTRICT"]
            + ["CITY"] * len([t for t in tokenize(city) if t.is_word])
            + ["STREET"] * len([t for t in tokenize(street) if t.is_word])
        )
        examples.append((tokens, labels))
    return examples


def update_sequence(
    emissions: defaultdict[str, float],
    transitions: defaultdict[str, float],
    tokens: list[Token],
    gold: list[str],
    predicted: list[str],
) -> None:
    for index, (gold_label, predicted_label) in enumerate(zip(gold, predicted)):
        if gold_label == predicted_label:
            continue
        for feature in token_features(tokens, index):
            emissions[f"{gold_label}\t{feature}"] += 1.0
            emissions[f"{predicted_label}\t{feature}"] -= 1.0

    for labels, direction in ((gold, 1.0), (predicted, -1.0)):
        previous = START
        for label in labels:
            transitions[f"{previous}\t{label}"] += direction
            previous = label
        transitions[f"{previous}\t{END}"] += direction


def train(seed: int = 2017, epochs: int = 40) -> dict[str, object]:
    randomizer = random.Random(seed)
    examples = build_examples()
    emissions: defaultdict[str, float] = defaultdict(float)
    transitions: defaultdict[str, float] = defaultdict(float)

    for _ in range(epochs):
        randomizer.shuffle(examples)
        for tokens, gold in examples:
            model = CompactSequenceTagger(LABELS, emissions, transitions)
            predicted, _ = model.tag(tokens)
            if predicted != gold:
                update_sequence(emissions, transitions, tokens, gold, predicted)

    return {
        "format": "address-normalizer-compact-sequence-v1",
        "training": {
            "seed": seed,
            "epochs": epochs,
            "examples": len(examples),
            "source": "synthetic public place-name starter corpus",
        },
        "labels": list(LABELS),
        "emissions": {
            key: round(value, 4)
            for key, value in sorted(emissions.items())
            if value
        },
        "transitions": {
            key: round(value, 4)
            for key, value in sorted(transitions.items())
            if value
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "src/address_normalizer/data/model.json",
    )
    parser.add_argument("--seed", type=int, default=2017)
    parser.add_argument("--epochs", type=int, default=40)
    args = parser.parse_args()

    payload = train(seed=args.seed, epochs=args.epochs)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "output": str(args.output),
                "bytes": args.output.stat().st_size,
                "training": payload["training"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
