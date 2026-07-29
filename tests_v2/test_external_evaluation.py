import json
from pathlib import Path
import sys

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT))

from address_normalizer import parse
from evaluation.evaluate_redmadrobot import (
    AddressSnippet,
    _gold_spans,
    _reconstruct,
    _without_failures,
    evaluate,
)
from evaluation.evaluate_redmadrobot_detection import (
    Message as DetectionMessage,
    Span as DetectionSpan,
    evaluate as evaluate_detection,
)


def _snippet(
    tokens: tuple[str, ...],
    tags: tuple[str, ...],
) -> AddressSnippet:
    text, offsets = _reconstruct(tokens)
    return AddressSnippet(
        source_row=0,
        text=text,
        tokens=tokens,
        tags=tags,
        offsets=offsets,
    )


def test_gold_bio_spans_are_reconstructed_from_tokens():
    snippet = _snippet(
        ("ул", ".", "Ополченская", ",", "дом", "5"),
        (
            "B-STREET",
            "I-STREET",
            "I-STREET",
            "O",
            "B-HOUSE",
            "I-HOUSE",
        ),
    )
    assert [
        (span.label, snippet.text[span.start : span.end])
        for span in _gold_spans(snippet)
    ] == [
        ("STREET", "ул . Ополченская"),
        ("HOUSE", "дом 5"),
    ]


def test_external_span_evaluation_accepts_overlapping_component_values():
    snippet = _snippet(
        ("ул", ".", "Ополченская", ",", "дом", "5"),
        (
            "B-STREET",
            "I-STREET",
            "I-STREET",
            "O",
            "B-HOUSE",
            "I-HOUSE",
        ),
    )
    report = evaluate([snippet], parse)
    assert report["micro"]["support"] == 2
    assert report["micro"]["tp"] == 2
    assert report["micro"]["f1"] == 1.0
    assert report["span_overlap_micro"] == report["micro"]
    assert report["metric_definitions"]["fields"].startswith("per-field")
    summary = _without_failures(report)
    assert summary["micro"] == report["span_overlap_micro"]
    assert "metric_definitions" not in summary
    assert "span_overlap_micro" not in summary


def test_complete_message_detection_is_scored_without_oracle_cropping():
    text = "Текст до ул. Мира, д. 2 после"
    expected_text = "ул. Мира, д. 2"
    start = text.index(expected_text)
    report = evaluate_detection(
        [
            DetectionMessage(
                source_row=1,
                text=text,
                gold=(DetectionSpan(start, start + len(expected_text)),),
            ),
            DetectionMessage(
                source_row=2,
                text="Встреча завтра в 18:30",
                gold=(),
            ),
        ]
    )

    assert report["span_overlap_micro"]["f1"] == 1.0
    assert report["exact_span_micro"]["f1"] == 1.0
    assert report["negative_message_specificity"] == 1.0
    assert report["failure_case_count"] == 0


def test_committed_message_detection_report_keeps_all_failure_cases():
    report = json.loads(
        (
            ROOT / "evaluation/redmadrobot_detection_report.json"
        ).read_text(encoding="utf-8")
    )

    assert report["source"]["revision"]
    assert report["metric_definitions"]["span_overlap_micro"]
    assert report["failure_case_count"] == len(report["failure_cases"])
    assert report["failure_case_count"] == 107
