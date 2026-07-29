from __future__ import annotations

import json

import pytest

from address_normalizer import DetectedAddress, detect_addresses

def test_detects_exact_span_and_keeps_component_offsets_relative():
    message = (
        "Курьер приедет по адресу: Москва, ул. Тверская, "
        "д. 13, кв. 4. Позвоните."
    )
    detected = detect_addresses(message)

    assert len(detected) == 1
    item = detected[0]
    assert isinstance(item, DetectedAddress)
    assert item.text == "Москва, ул. Тверская, д. 13, кв. 4"
    assert message[item.start : item.end] == item.text
    assert item.parsed.raw == item.text
    assert item.parsed.street is not None
    assert (
        item.text[item.parsed.street.start : item.parsed.street.end]
        == item.parsed.street.raw
    )
    assert set(item.signals) >= {
        "address_cue",
        "street_marker",
        "house_marker",
        "parsed_street",
        "parsed_house",
    }


def test_detection_serialization_is_json_compatible():
    item = detect_addresses("Адрес: Ополченская 5-30")[0]
    payload = item.as_dict()

    assert json.loads(json.dumps(payload, ensure_ascii=False)) == payload
    assert payload["span"] == [7, 23]
    assert payload["parsed"]["street"]["value"] == "Ополченская"
    assert payload["parsed"]["warnings"] == ["ambiguous_numeric_tail"]


def test_multiple_addresses_are_ordered_and_non_overlapping():
    message = "Первый: ул. Ленина, д. 1; второй: ул. Мира, д. 2."
    detected = detect_addresses(message)

    assert [item.text for item in detected] == [
        "ул. Ленина, д. 1",
        "ул. Мира, д. 2",
    ]
    assert detected[0].end < detected[1].start


@pytest.mark.parametrize(
    "message",
    [
        "В доме 13 квартир и два подъезда.",
        "Встреча 13.05.2027 в 18:30.",
        "Я живу на улице Науки.",
        "Заказ № 4815 уже передан курьеру.",
        "Ополченская 5-30 указана в старом справочнике.",
    ],
)
def test_conservative_policy_rejects_weak_address_evidence(message):
    assert detect_addresses(message) == ()


def test_detection_rejects_non_string_input():
    with pytest.raises(TypeError, match="text must be a string"):
        detect_addresses(None)
