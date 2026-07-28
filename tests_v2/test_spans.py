from address_normalizer import parse


def test_parts_point_into_original_string():
    raw = "г. Москва, ул. Тверская, д.4, кв.12"
    result = parse(raw)
    for part in (
        result.city,
        result.street_type,
        result.street,
        result.house_num,
        result.apartment,
    ):
        assert part is not None
        assert raw[part.start : part.end] == part.raw


def test_numeric_tail_spans_exclude_hyphen():
    raw = "ополченская 5-30"
    result = parse(raw)
    assert result.house_num.span == (12, 13)
    assert result.apartment.span == (14, 16)
    assert raw[slice(*result.street.span)] == "ополченская"
