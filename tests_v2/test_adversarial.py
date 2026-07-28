from __future__ import annotations

import pytest

from address_normalizer import parse


def _value(result, field: str) -> str | None:
    part = getattr(result, field)
    return part.value if part else None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "  Г. МОСКВА; УЛ. ТВЕРСКАЯ, Д. 4, КВ. 12  ",
            {
                "city": "Москва",
                "street": "Тверская",
                "house_num": "4",
                "apartment": "12",
            },
        ),
        (
            "г.\u00a0Москва,\u2003ул.\u202fТверская,\u00a0д.\u00a04",
            {"city": "Москва", "street": "Тверская", "house_num": "4"},
        ),
        (
            "г. Орёл, ул. Лесная, д. 7",
            {"city": "Орёл", "street": "Лесная", "house_num": "7"},
        ),
        (
            "г. Орел, ул. Лесная, д. 7",
            {"city": "Орел", "street": "Лесная", "house_num": "7"},
        ),
        (
            "ул. Тверская, д. 12/3",
            {"street": "Тверская", "house_num": "12/3"},
        ),
        (
            "ул. Тверская, дом 5, корпус 2, строение 1, квартира 9",
            {
                "street": "Тверская",
                "house_num": "5",
                "corpus": "2",
                "structure": "1",
                "apartment": "9",
            },
        ),
        (
            "Москва Тверская 5",
            {"city": "Москва", "street": "Тверская", "house_num": "5"},
        ),
        (
            "д. 5, ул. Тверская, г. Москва",
            {"city": "Москва", "street": "Тверская", "house_num": "5"},
        ),
    ],
)
def test_adversarial_address_shapes(raw, expected):
    result = parse(raw)
    assert {field: _value(result, field) for field in expected} == expected
    for field in expected:
        part = getattr(result, field)
        assert part is not None
        assert raw[part.start : part.end] == part.raw


@pytest.mark.parametrize("raw", ["Тверская, 5-30", "ул. Тверская, д. 5-30"])
def test_ambiguous_numeric_tail_keeps_both_interpretations(raw):
    result = parse(raw)

    assert _value(result, "house_num") == "5"
    assert _value(result, "apartment") == "30"
    assert result.warnings == ("ambiguous_numeric_tail",)
    assert result.alternatives[0].components == {"house_num": "5-30"}
    assert 0.0 <= result.alternatives[0].confidence <= 1.0


def test_unrecognized_tail_is_not_discarded():
    raw = "ул. Тверская, д. 4, подъезд 2"
    result = parse(raw)

    assert result.warnings == ("unparsed_text",)
    assert len(result.unparsed) == 1
    unparsed = result.unparsed[0]
    assert unparsed.raw == "подъезд 2"
    assert raw[unparsed.start : unparsed.end] == unparsed.raw
