from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json

import pytest

from address_normalizer import parse, parse_iter, parse_many


def value(part):
    return part.value if part else None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "ополченская 5-30",
            {"street": "Ополченская", "house_num": "5", "apartment": "30"},
        ),
        (
            "Ополченская, д. 5, кв. 30",
            {"street": "Ополченская", "house_num": "5", "apartment": "30"},
        ),
        (
            "г. Москва, ул. Тверская, д.4, кв.12",
            {
                "city": "Москва",
                "street": "Тверская",
                "house_num": "4",
                "apartment": "12",
            },
        ),
        (
            "142703 Московская область Ленинский район г Видное ул Школьная д 78",
            {
                "postal_code": "142703",
                "region": "Московская",
                "district": "Ленинский",
                "city": "Видное",
                "street": "Школьная",
                "house_num": "78",
            },
        ),
        (
            "СПб Невский проспект 10 корп 2 кв 15",
            {
                "city": "Санкт-Петербург",
                "street": "Невский",
                "house_num": "10",
                "corpus": "2",
                "apartment": "15",
            },
        ),
        (
            "Ростов-на-Дону, Большая Садовая 47",
            {
                "city": "Ростов-на-Дону",
                "street": "Большая Садовая",
                "house_num": "47",
            },
        ),
        (
            "ул 8 Марта д 5",
            {"street": "8 Марта", "house_num": "5"},
        ),
        (
            "Московская 5/1",
            {"street": "Московская", "house_num": "5/1"},
        ),
        (
            "Самара Авроры 7 12",
            {
                "city": "Самара",
                "street": "Авроры",
                "house_num": "7",
                "apartment": "12",
            },
        ),
        (
            "Москва Тверская д 5/1 кв 9",
            {
                "city": "Москва",
                "street": "Тверская",
                "house_num": "5/1",
                "apartment": "9",
            },
        ),
        (
            "109542, г. Москва, Рязанский проспект, дом 91, помещение 11",
            {
                "postal_code": "109542",
                "city": "Москва",
                "street": "Рязанский",
                "house_num": "91",
                "apartment": "11",
            },
        ),
        (
            "140000, Московская обл., г. Люберцы, ул. Красная, д. 1-700",
            {
                "postal_code": "140000",
                "region": "Московская",
                "city": "Люберцы",
                "street": "Красная",
                "house_num": "1",
                "apartment": "700",
            },
        ),
    ],
)
def test_golden_addresses(raw, expected):
    result = parse(raw)
    for name, expected_value in expected.items():
        assert value(getattr(result, name)) == expected_value
    assert result.unparsed == ()


def test_numeric_tail_retains_compound_house_alternative():
    result = parse("ополченская 5-30")
    assert result.alternatives[0].components == {"house_num": "5-30"}
    assert "ambiguous_numeric_tail" in result.warnings


def test_country_phrase_is_ignored_instead_of_becoming_a_region():
    result = parse(
        "610002, Российская Федерация, Кировская область, "
        "Киров, ул. Володарского, д. 171"
    )
    assert value(result.region) == "Кировская"
    assert value(result.city) == "Киров"
    assert all("Российск" not in part.raw for part in result.unparsed)


def test_empty_and_punctuation_only_input():
    assert parse("").as_dict()["normalized"] == ""
    assert parse(" ,... ").as_dict()["normalized"] == ""


def test_non_string_rejected():
    with pytest.raises(TypeError):
        parse(None)


def test_parse_many_preserves_order():
    results = parse_many(["Тверская 1", "Ополченская 2"])
    assert [result.street.value for result in results] == [
        "Тверская",
        "Ополченская",
    ]


def test_parse_many_accepts_generators_and_parse_iter_is_lazy():
    consumed: list[str] = []

    def addresses():
        for raw in ("Тверская 1", "Ополченская 2"):
            consumed.append(raw)
            yield raw

    results = parse_iter(addresses())
    assert consumed == []
    first = next(results)
    assert first.raw == "Тверская 1"
    assert consumed == ["Тверская 1"]
    assert [result.raw for result in results] == ["Ополченская 2"]
    assert [result.raw for result in parse_many(addresses())] == [
        "Тверская 1",
        "Ополченская 2",
    ]


@pytest.mark.parametrize("addresses", ["Тверская 1", b"address", None])
def test_batch_api_rejects_non_iterables_and_single_strings(addresses):
    with pytest.raises(TypeError, match="iterable of strings"):
        parse_many(addresses)


def test_batch_api_identifies_a_bad_element():
    with pytest.raises(TypeError, match=r"addresses\[1\] must be a string"):
        parse_many(["Тверская 1", None])


def test_result_serialization_is_json_compatible_and_keeps_review_signals():
    raw = "ориентир вокзал, Ополченская 5-30"
    result = parse(raw)
    payload = result.as_dict()

    assert json.loads(json.dumps(payload, ensure_ascii=False)) == payload
    assert payload["raw"] == raw
    assert payload["warnings"] == [
        "ambiguous_numeric_tail",
        "unparsed_text",
    ]
    assert payload["alternatives"][0]["components"] == {"house_num": "5-30"}
    assert payload["unparsed"][0]["raw"] == "ориентир"
    for field in ("street", "house_num", "apartment"):
        part = payload[field]
        assert part is not None
        start, end = part["span"]
        assert raw[start:end] == part["raw"]


def test_shared_default_parser_is_safe_for_concurrent_calls():
    addresses = [f"г. Москва, ул. Тверская, д. {index}" for index in range(1, 33)]
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(parse, addresses))

    assert [result.raw for result in results] == addresses
    assert [result.house_num.value for result in results] == [
        str(index) for index in range(1, 33)
    ]


def test_very_long_input_is_not_truncated_and_keeps_offsets():
    raw = " " * 20_000 + "ул. Тверская, д. 4"
    result = parse(raw)

    assert result.raw == raw
    for part in (result.street_type, result.street, result.house_num):
        assert part is not None
        assert raw[part.start : part.end] == part.raw


def test_control_character_input_retains_raw_and_unparsed_content():
    raw = "ул. Тверская, д. 4\x00подъезд 2"
    result = parse(raw)

    assert result.raw == raw
    assert result.house_num is not None
    assert result.house_num.value == "4"
    assert result.warnings == ("unparsed_text",)
    assert result.unparsed[0].raw == "подъезд 2"
    assert raw[result.unparsed[0].start : result.unparsed[0].end] == "подъезд 2"
