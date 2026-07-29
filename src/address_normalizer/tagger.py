"""Tiny dependency-free structured sequence tagger.

The bundled model is deliberately small. It is used only to classify residual,
unmarked text after deterministic address syntax has been extracted.
"""

from __future__ import annotations

from importlib.resources import files
import json
from math import exp
from pathlib import Path
from typing import cast, Iterable, Sequence

from .tokenizer import Token


START = "<START>"
END = "<END>"


def token_features(tokens: Sequence[Token], index: int) -> tuple[str, ...]:
    token = tokens[index]
    lower = token.lower
    features = [
        "bias",
        f"word={lower}",
        f"kind={'number' if token.is_number else 'word' if token.is_word else 'punct'}",
        f"prefix1={lower[:1]}",
        f"prefix2={lower[:2]}",
        f"suffix1={lower[-1:]}",
        f"suffix2={lower[-2:]}",
        f"suffix3={lower[-3:]}",
        f"position={'first' if index == 0 else 'last' if index == len(tokens) - 1 else 'middle'}",
    ]
    if token.is_number:
        features.append(f"digits={min(len(token.text), 6)}")
    if index:
        previous = tokens[index - 1]
        features.extend(
            (
                f"prev={previous.lower}",
                f"prev_kind={'number' if previous.is_number else 'word' if previous.is_word else 'punct'}",
            )
        )
    else:
        features.append("bos")
    if index + 1 < len(tokens):
        following = tokens[index + 1]
        features.extend(
            (
                f"next={following.lower}",
                f"next_kind={'number' if following.is_number else 'word' if following.is_word else 'punct'}",
            )
        )
    else:
        features.append("eos")
    return tuple(features)


class CompactSequenceTagger:
    """Sparse linear-chain model with Viterbi inference."""

    def __init__(
        self,
        labels: Sequence[str],
        emissions: dict[str, float],
        transitions: dict[str, float],
    ) -> None:
        self.labels = tuple(labels)
        self.emissions = emissions
        self.transitions = transitions

    @classmethod
    def from_package(cls) -> "CompactSequenceTagger":
        model_path = files("address_normalizer").joinpath("data/model.json")
        return cls.from_payload(json.loads(model_path.read_text(encoding="utf-8")))

    @classmethod
    def from_path(cls, path: str | Path) -> "CompactSequenceTagger":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_payload(payload)

    @classmethod
    def from_payload(cls, payload: dict[str, object]) -> "CompactSequenceTagger":
        return cls(
            labels=cast(list[str], payload["labels"]),
            emissions=cast(dict[str, float], payload["emissions"]),
            transitions=cast(dict[str, float], payload["transitions"]),
        )

    def emission_score(self, label: str, features: Iterable[str]) -> float:
        return sum(
            self.emissions.get(f"{label}\t{feature}", 0.0)
            for feature in features
        )

    def transition_score(self, previous: str, label: str) -> float:
        return self.transitions.get(f"{previous}\t{label}", 0.0)

    def tag(self, tokens: Sequence[Token]) -> tuple[list[str], list[float]]:
        if not tokens:
            return [], []

        features = [token_features(tokens, index) for index in range(len(tokens))]
        scores: dict[str, float] = {}
        paths: dict[str, list[str]] = {}
        for label in self.labels:
            scores[label] = self.transition_score(START, label) + self.emission_score(
                label, features[0]
            )
            paths[label] = [label]

        margins: list[float] = []
        for index in range(1, len(tokens)):
            next_scores: dict[str, float] = {}
            next_paths: dict[str, list[str]] = {}
            for label in self.labels:
                candidates = [
                    (
                        score
                        + self.transition_score(previous, label)
                        + self.emission_score(label, features[index]),
                        previous,
                    )
                    for previous, score in scores.items()
                ]
                best_score, best_previous = max(candidates)
                next_scores[label] = best_score
                next_paths[label] = [*paths[best_previous], label]
            ordered = sorted(next_scores.values(), reverse=True)
            margins.append(ordered[0] - ordered[1] if len(ordered) > 1 else 10.0)
            scores, paths = next_scores, next_paths

        finals = [
            (score + self.transition_score(label, END), label)
            for label, score in scores.items()
        ]
        _, best_label = max(finals)
        labels = paths[best_label]

        # These are bounded decision-strength scores, not calibrated probabilities.
        token_confidence = [0.78]
        token_confidence.extend(0.5 + 0.45 * (1.0 - exp(-max(0.0, margin))) for margin in margins)
        return labels, token_confidence
