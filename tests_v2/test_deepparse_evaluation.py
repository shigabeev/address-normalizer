from pathlib import Path
import sys

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT / "evaluation"))

from deepparse_data import (
    expected_components,
    group_id_and_split,
    mapped_labels,
    normalized_address_id,
    quality_tier,
)
from evaluate_deepparse import _score, _summary, gold_spans, predicted_spans
from address_normalizer import parse


def test_deepparse_tags_map_to_package_fields():
    tags = (
        "Country",
        "Municipality",
        "StreetName",
        "StreetName",
        "StreetNumber",
        "Unit",
    )
    labels = mapped_labels(tags)
    assert labels == (
        "O",
        "CITY",
        "STREET",
        "STREET",
        "HOUSE",
        "APARTMENT",
    )
    assert expected_components(
        ("Россия", "Самара", "ул", "Авроры", "7", "12"),
        labels,
    ) == {
        "postal_code": None,
        "region": None,
        "district": None,
        "city": "Самара",
        "street": "ул Авроры",
        "house_num": "7",
        "apartment": "12",
    }
    assert quality_tier(tags) == "street_house_unit"


def test_format_variants_share_a_split_and_exact_text_ids_do_not():
    tokens = ("Самара", "ул", "Авроры", "7", "12")
    tags = (
        "Municipality",
        "StreetName",
        "StreetName",
        "StreetNumber",
        "Unit",
    )
    group_a = group_id_and_split(tokens, tags)
    group_b = group_id_and_split(tokens[:-1], tags[:-1])
    assert group_a == group_b
    assert normalized_address_id("Самара ул Авроры 7") == (
        normalized_address_id("  самара  УЛ  авроры  7 ")
    )
    assert normalized_address_id("Самара ул Авроры 8") != (
        normalized_address_id("Самара ул Авроры 7")
    )


def test_gold_and_predicted_spans_align_on_a_conventional_address():
    row = {
        "raw": "Россия Самара ул Авроры 7 12",
        "tokens": ["Россия", "Самара", "ул", "Авроры", "7", "12"],
        "labels": ["O", "CITY", "STREET", "STREET", "HOUSE", "APARTMENT"],
    }
    assert [
        (span.label, row["raw"][span.start : span.end])
        for span in gold_spans(row)
    ] == [
        ("CITY", "Самара"),
        ("STREET", "ул Авроры"),
        ("HOUSE", "7"),
        ("APARTMENT", "12"),
    ]
    result = parse(row["raw"])
    assert {
        (span.label, row["raw"][span.start : span.end])
        for span in predicted_spans(result)
    } >= {
        ("CITY", "Самара"),
        ("STREET", "ул Авроры"),
        ("HOUSE", "7"),
        ("APARTMENT", "12"),
    }

    row.update(
        {
            "source_row": 1,
            "example_id": "example",
            "tier": "street_house_unit",
        }
    )
    report = _score([row])
    assert report["character_micro"]["f1"] == 1.0
    assert report["token_micro"]["f1"] == 1.0
    assert report["exact_token_sequence_rate"] == 1.0
    assert report["span_overlap_micro"] == report["micro"]
    assert report["character_overlap_micro"] == report["character_micro"]
    assert report["token_label_micro"] == report["token_micro"]
    assert report["metric_definitions"]["fields"].startswith("per-field")
    assert report["metric_definitions"]["character_fields"].startswith("per-field")
    assert report["metric_definitions"]["token_fields"].startswith("per-field")
    summary = _summary(report)
    assert summary["micro"] == report["span_overlap_micro"]
    assert "metric_definitions" not in summary
    assert "span_overlap_micro" not in summary
    assert "character_overlap_micro" not in summary
    assert "token_label_micro" not in summary
