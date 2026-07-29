import json
from importlib.resources import files
from pathlib import Path
import sys

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))

from address_normalizer import parse
from address_normalizer.tagger import CompactSequenceTagger
from address_normalizer.tokenizer import tokenize
from training.real_corpus import (
    build_examples,
    corpus_summary,
    load_reference_rows,
)


def test_bundled_model_is_loadable_and_tags_a_sequence():
    model = CompactSequenceTagger.from_package()
    tokens = [token for token in tokenize("Самара Авроры") if token.is_word]
    labels, confidence = model.tag(tokens)
    assert labels == ["CITY", "STREET"]
    assert len(confidence) == len(tokens)


def test_hybrid_api_uses_model_for_unmarked_components():
    result = parse("Самара Авроры 7 12")
    assert result.city.source == "model"
    assert result.street.source == "model"


def test_bundled_model_was_trained_on_grouped_real_examples():
    payload = json.loads(
        files("address_normalizer")
        .joinpath("data/model.json")
        .read_text(encoding="utf-8")
    )
    training = payload["training"]
    assert training["algorithm"] == "epoch-averaged structured perceptron"
    assert training["dataset"] == "evaluation/legacy_reference_500.jsonl"
    assert training["examples"] >= 300


def test_real_corpus_has_disjoint_nonempty_splits():
    examples = build_examples(
        load_reference_rows(ROOT / "evaluation/legacy_reference_500.jsonl")
    )
    summary = corpus_summary(examples)
    assert summary["leaking_groups"] == []
    for split in ("train", "validation", "test"):
        assert summary["splits"][split]["examples"] > 0
        assert summary["splits"][split]["groups"] > 0
