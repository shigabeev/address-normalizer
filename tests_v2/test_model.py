from address_normalizer import parse
from address_normalizer.tagger import CompactSequenceTagger
from address_normalizer.tokenizer import tokenize


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
