"""Hybrid rule and compact-model parser for Russian address strings."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Callable, Iterable, Protocol, Sequence

from .normalization import normalize_name, normalize_number
from .tagger import CompactSequenceTagger
from .tokenizer import Token, tokenize
from .types import AddressPart, Alternative, ParsedAddress


class SequenceTagger(Protocol):
    def tag(self, tokens: Sequence[Token]) -> tuple[list[str], list[float]]: ...


_LETTER = r"A-Za-zА-Яа-яЁё"
_SIMPLE_NUMBER = rf"\d+[{_LETTER}]?"
_SLASH_NUMBER = rf"{_SIMPLE_NUMBER}(?:\s*/\s*{_SIMPLE_NUMBER})?"
_HOUSE_NUMBER = rf"{_SLASH_NUMBER}(?:\s*-\s*{_SIMPLE_NUMBER})?"

_POSTAL_RE = re.compile(r"(?<!\d)(?P<value>\d{6})(?!\d)")
_COUNTRY_RE = re.compile(
    r"(?<!\w)(?:Российская\s+Федерация|Россия|РФ)(?!\w)",
    re.IGNORECASE,
)
_EXPLICIT_NUMERIC = {
    "house_num": re.compile(
        rf"(?<!\w)(?:д(?:ом)?|вл(?:адение)?)\.?\s*(?:№\s*)?"
        rf"(?P<value>{_HOUSE_NUMBER})(?!\w)",
        re.IGNORECASE,
    ),
    "corpus": re.compile(
        rf"(?<!\w)(?:корп(?:ус)?|кор|к)\.?\s*(?:№\s*)?"
        rf"(?P<value>{_SLASH_NUMBER})(?!\w)",
        re.IGNORECASE,
    ),
    "structure": re.compile(
        rf"(?<!\w)(?:стр(?:оение)?|соор(?:ужение)?)\.?\s*(?:№\s*)?"
        rf"(?P<value>{_SLASH_NUMBER})(?!\w)",
        re.IGNORECASE,
    ),
    "apartment": re.compile(
        rf"(?<!\w)(?:кв(?:артира)?|комн(?:ата)?|оф(?:ис)?|"
        rf"пом(?:ещение)?)\.?\s*(?:№\s*)?"
        rf"(?P<value>{_SLASH_NUMBER})(?!\w)",
        re.IGNORECASE,
    ),
}
_NUMERIC_TAIL_RE = re.compile(
    rf"(?<![\w/])(?P<house>{_SIMPLE_NUMBER})\s*-\s*"
    rf"(?P<apartment>{_SIMPLE_NUMBER})\s*[,.]?\s*$",
    re.IGNORECASE,
)
_SPACE_NUMBER_TAIL_RE = re.compile(
    rf"(?<![\w/])(?P<house>{_SIMPLE_NUMBER})\s+"
    rf"(?P<apartment>{_SIMPLE_NUMBER})\s*[,.]?\s*$",
    re.IGNORECASE,
)
_PLAIN_HOUSE_TAIL_RE = re.compile(
    rf"(?<![\w/])(?P<value>{_SLASH_NUMBER})\s*[,.]?\s*$",
    re.IGNORECASE,
)

_MARKER_RE = re.compile(
    r"(?<!\w)(?P<marker>"
    r"обл(?:асть)?|край|респ(?:ублика)?|а\.?\s*о\.?|"
    r"р-?н|район|"
    r"г(?:ород)?|"
    r"пгт|пос(?:елок|ёлок)?|село|деревня|хутор|х|с|п|д|"
    r"ул(?:ица)?|пр-?кт|просп(?:ект)?|пер(?:еулок)?|"
    r"б-?р|бульвар|наб(?:ережная)?|ш(?:оссе)?|проезд|пр-?д|"
    r"пл(?:ощадь)?|аллея|мкр(?:орайон)?"
    r")\.?(?!\w)",
    re.IGNORECASE,
)

_MARKER_INFO: dict[str, tuple[str, str]] = {
    "обл": ("region", "обл"),
    "область": ("region", "обл"),
    "край": ("region", "край"),
    "респ": ("region", "респ"),
    "республика": ("region", "респ"),
    "ао": ("region", "АО"),
    "рн": ("district", "р-н"),
    "район": ("district", "р-н"),
    "г": ("city", "г"),
    "город": ("city", "г"),
    "пгт": ("settlement", "пгт"),
    "пос": ("settlement", "п"),
    "поселок": ("settlement", "п"),
    "посёлок": ("settlement", "п"),
    "село": ("settlement", "с"),
    "деревня": ("settlement", "д"),
    "хутор": ("settlement", "х"),
    "х": ("settlement", "х"),
    "с": ("settlement", "с"),
    "п": ("settlement", "п"),
    "д": ("settlement", "д"),
    "ул": ("street", "ул"),
    "улица": ("street", "ул"),
    "пркт": ("street", "пр-кт"),
    "просп": ("street", "пр-кт"),
    "проспект": ("street", "пр-кт"),
    "пер": ("street", "пер"),
    "переулок": ("street", "пер"),
    "бр": ("street", "б-р"),
    "бульвар": ("street", "б-р"),
    "наб": ("street", "наб"),
    "набережная": ("street", "наб"),
    "ш": ("street", "ш"),
    "шоссе": ("street", "ш"),
    "проезд": ("street", "пр-д"),
    "прд": ("street", "пр-д"),
    "пл": ("street", "пл"),
    "площадь": ("street", "пл"),
    "аллея": ("street", "аллея"),
    "мкр": ("street", "мкр"),
    "микрорайон": ("street", "мкр"),
}

_SUFFIX_MARKERS = {
    "обл",
    "область",
    "край",
    "респ",
    "республика",
    "рн",
    "район",
    "пркт",
    "просп",
    "проспект",
    "пер",
    "переулок",
    "бр",
    "бульвар",
    "наб",
    "набережная",
    "ш",
    "шоссе",
    "проезд",
    "прд",
    "пл",
    "площадь",
    "аллея",
    "мкр",
    "микрорайон",
}

_KNOWN_CITY_ALIASES = {
    "мск": "Москва",
    "москва": "Москва",
    "спб": "Санкт-Петербург",
    "питер": "Санкт-Петербург",
    "санкт-петербург": "Санкт-Петербург",
    "ростов-на-дону": "Ростов-на-Дону",
}


@dataclass(frozen=True, slots=True)
class _Marker:
    kind: str
    canonical_type: str
    key: str
    start: int
    end: int


def _marker_key(value: str) -> str:
    return re.sub(r"[\s.\-]", "", value.casefold().replace("ё", "е"))


def _trim_span(text: str, start: int, end: int) -> tuple[int, int]:
    while start < end and (text[start].isspace() or text[start] in ",.;:"):
        start += 1
    while end > start and (text[end - 1].isspace() or text[end - 1] in ",.;:"):
        end -= 1
    return start, end


def _overlaps(start: int, end: int, intervals: Iterable[tuple[int, int]]) -> bool:
    return any(start < right and end > left for left, right in intervals)


def _available_spans(
    text: str,
    start: int,
    end: int,
    intervals: Iterable[tuple[int, int]],
) -> list[tuple[int, int]]:
    """Return trimmed portions of a span not covered by consumed intervals."""

    spans = [(start, end)]
    for left, right in sorted(intervals):
        next_spans: list[tuple[int, int]] = []
        for span_start, span_end in spans:
            if right <= span_start or left >= span_end:
                next_spans.append((span_start, span_end))
                continue
            if span_start < left:
                next_spans.append((span_start, left))
            if right < span_end:
                next_spans.append((right, span_end))
        spans = next_spans
    return [
        trimmed
        for span in spans
        if (trimmed := _trim_span(text, *span))[0] < trimmed[1]
    ]


def _part(
    text: str,
    start: int,
    end: int,
    *,
    confidence: float,
    source: str,
    normalizer: Callable[[str], str] = normalize_name,
) -> AddressPart:
    raw = text[start:end]
    return AddressPart(
        value=normalizer(raw),
        raw=raw,
        start=start,
        end=end,
        confidence=confidence,
        source=source,
    )


class AddressParser:
    """Parse addresses without consulting or bundling an address registry."""

    def __init__(self, tagger: SequenceTagger | None = None) -> None:
        self.tagger: SequenceTagger | None
        if tagger is not None:
            self.tagger = tagger
        else:
            try:
                self.tagger = CompactSequenceTagger.from_package()
            except (FileNotFoundError, ModuleNotFoundError):
                self.tagger = None

    def parse(self, text: str) -> ParsedAddress:
        tokens = tokenize(text)
        if not tokens:
            return ParsedAddress(raw=text)

        components: dict[str, AddressPart] = {}
        consumed: list[tuple[int, int]] = []
        warnings: list[str] = []
        alternatives: list[Alternative] = []

        postal = _POSTAL_RE.search(text)
        if postal:
            components["postal_code"] = _part(
                text,
                postal.start("value"),
                postal.end("value"),
                confidence=0.995,
                source="rule",
                normalizer=lambda value: value,
            )
            consumed.append(postal.span())

        consumed.extend(match.span() for match in _COUNTRY_RE.finditer(text))

        for name, pattern in _EXPLICIT_NUMERIC.items():
            match = pattern.search(text)
            if not match:
                continue
            components[name] = _part(
                text,
                match.start("value"),
                match.end("value"),
                confidence=0.995,
                source="rule",
                normalizer=normalize_number,
            )
            consumed.append(match.span())

        explicit_house = components.get("house_num")
        if explicit_house and "apartment" not in components:
            split = re.fullmatch(
                rf"(?P<house>{_SLASH_NUMBER})\s*-\s*"
                rf"(?P<apartment>{_SIMPLE_NUMBER})",
                explicit_house.raw,
                re.IGNORECASE,
            )
            if split:
                original_value = normalize_number(explicit_house.raw)
                house_start = explicit_house.start + split.start("house")
                house_end = explicit_house.start + split.end("house")
                apartment_start = explicit_house.start + split.start("apartment")
                apartment_end = explicit_house.start + split.end("apartment")
                components["house_num"] = _part(
                    text,
                    house_start,
                    house_end,
                    confidence=0.91,
                    source="postprocessor",
                    normalizer=normalize_number,
                )
                components["apartment"] = _part(
                    text,
                    apartment_start,
                    apartment_end,
                    confidence=0.87,
                    source="postprocessor",
                    normalizer=normalize_number,
                )
                alternatives.append(
                    Alternative(
                        components={"house_num": original_value},
                        confidence=0.09,
                        reason=(
                            "A hyphenated numeric tail can also be a compound "
                            "house number."
                        ),
                    )
                )
                warnings.append("ambiguous_numeric_tail")

        if "house_num" not in components and "apartment" not in components:
            numeric_tail = _NUMERIC_TAIL_RE.search(text) or _SPACE_NUMBER_TAIL_RE.search(
                text
            )
            if numeric_tail:
                components["house_num"] = _part(
                    text,
                    numeric_tail.start("house"),
                    numeric_tail.end("house"),
                    confidence=0.91,
                    source="postprocessor",
                    normalizer=normalize_number,
                )
                components["apartment"] = _part(
                    text,
                    numeric_tail.start("apartment"),
                    numeric_tail.end("apartment"),
                    confidence=0.87,
                    source="postprocessor",
                    normalizer=normalize_number,
                )
                consumed.append(numeric_tail.span())
                if "-" in numeric_tail.group(0):
                    alternatives.append(
                        Alternative(
                            components={
                                "house_num": normalize_number(
                                    numeric_tail.group(0).strip(" ,.")
                                )
                            },
                            confidence=0.09,
                            reason=(
                                "A hyphenated numeric tail can also be a compound "
                                "house number."
                            ),
                        )
                    )
                    warnings.append("ambiguous_numeric_tail")

        if "house_num" not in components and not any(
            name in components for name in ("corpus", "structure", "apartment")
        ):
            plain_tail = _PLAIN_HOUSE_TAIL_RE.search(text)
            if plain_tail and not (
                components.get("postal_code")
                and plain_tail.span("value") == components["postal_code"].span
            ):
                components["house_num"] = _part(
                    text,
                    plain_tail.start("value"),
                    plain_tail.end("value"),
                    confidence=0.9,
                    source="postprocessor",
                    normalizer=normalize_number,
                )
                consumed.append(plain_tail.span())

        self._extract_residual_house(text, tokens, components, consumed)
        markers = self._markers(text)
        self._extract_known_initial_city(text, components, consumed)
        self._extract_marked_names(text, markers, components, consumed)
        self._extract_with_model(text, tokens, components, consumed)
        self._extract_implicit_segments(text, components, consumed)

        unparsed = self._unparsed(text, tokens, consumed)
        if unparsed:
            warnings.append("unparsed_text")

        confidences = [
            part.confidence
            for name, part in components.items()
            if name != "street_type"
        ]
        overall = sum(confidences) / len(confidences) if confidences else 0.0
        if unparsed:
            overall *= 0.9

        return ParsedAddress(
            raw=text,
            postal_code=components.get("postal_code"),
            region=components.get("region"),
            district=components.get("district"),
            city=components.get("city"),
            settlement=components.get("settlement"),
            street=components.get("street"),
            street_type=components.get("street_type"),
            house_num=components.get("house_num"),
            corpus=components.get("corpus"),
            structure=components.get("structure"),
            apartment=components.get("apartment"),
            unparsed=tuple(unparsed),
            warnings=tuple(dict.fromkeys(warnings)),
            alternatives=tuple(alternatives),
            confidence=overall,
        )

    def _markers(self, text: str) -> list[_Marker]:
        markers: list[_Marker] = []
        for match in _MARKER_RE.finditer(text):
            key = _marker_key(match.group("marker"))
            info = _MARKER_INFO.get(key)
            if not info:
                continue
            markers.append(
                _Marker(
                    kind=info[0],
                    canonical_type=info[1],
                    key=key,
                    start=match.start(),
                    end=match.end(),
                )
            )
        return markers

    def _extract_known_initial_city(
        self,
        text: str,
        components: dict[str, AddressPart],
        consumed: list[tuple[int, int]],
    ) -> None:
        if "city" in components:
            return
        postal_code = components.get("postal_code")
        start = postal_code.end if postal_code else 0
        while start < len(text) and (text[start].isspace() or text[start] in ",.;"):
            start += 1
        match = re.match(r"[A-Za-zА-Яа-яЁё]+(?:-[A-Za-zА-Яа-яЁё]+)*", text[start:])
        if not match:
            return
        raw = match.group(0)
        canonical = _KNOWN_CITY_ALIASES.get(raw.casefold().replace("ё", "е"))
        if not canonical:
            return
        end = start + len(raw)
        components["city"] = AddressPart(
            value=canonical,
            raw=raw,
            start=start,
            end=end,
            confidence=0.98,
            source="rule",
        )
        consumed.append((start, end))

    def _extract_marked_names(
        self,
        text: str,
        markers: Sequence[_Marker],
        components: dict[str, AddressPart],
        consumed: list[tuple[int, int]],
    ) -> None:
        for index, marker in enumerate(markers):
            if marker.kind in components:
                continue
            if _overlaps(marker.start, marker.end, consumed):
                continue

            previous_boundary = 0
            if index:
                previous_boundary = markers[index - 1].end
            comma = max(
                text.rfind(",", previous_boundary, marker.start),
                text.rfind(";", previous_boundary, marker.start),
            )
            before_start = comma + 1 if comma >= 0 else previous_boundary
            before_start, before_end = _trim_span(text, before_start, marker.start)

            next_boundary = len(text)
            if index + 1 < len(markers):
                next_boundary = markers[index + 1].start
            comma_positions = [
                position
                for position in (
                    text.find(",", marker.end, next_boundary),
                    text.find(";", marker.end, next_boundary),
                )
                if position >= 0
            ]
            if comma_positions:
                next_boundary = min(next_boundary, *comma_positions)
            after_start, after_end = _trim_span(text, marker.end, next_boundary)

            prefer_before = marker.key in _SUFFIX_MARKERS
            before_candidates = _available_spans(
                text, before_start, before_end, consumed
            )
            after_candidates = _available_spans(text, after_start, after_end, consumed)
            candidates = (
                (*reversed(before_candidates), *after_candidates)
                if prefer_before
                else (*after_candidates, *reversed(before_candidates))
            )
            selected: tuple[int, int] | None = None
            for start, end in candidates:
                if start >= end:
                    continue
                value = text[start:end]
                if not re.search(rf"[{_LETTER}]", value):
                    continue
                selected = start, end
                break
            if not selected:
                continue

            start, end = selected
            components[marker.kind] = _part(
                text,
                start,
                end,
                confidence=0.96,
                source="rule",
            )
            consumed.extend(((marker.start, marker.end), (start, end)))
            if marker.kind == "street":
                components["street_type"] = AddressPart(
                    value=marker.canonical_type,
                    raw=text[marker.start:marker.end],
                    start=marker.start,
                    end=marker.end,
                    confidence=0.995,
                    source="rule",
                )

    def _extract_residual_house(
        self,
        text: str,
        tokens: Sequence[Token],
        components: dict[str, AddressPart],
        consumed: list[tuple[int, int]],
    ) -> None:
        if "house_num" in components:
            return
        candidates = [
            token
            for token in tokens
            if token.is_number
            and len(token.text) < 6
            and not _overlaps(token.start, token.end, consumed)
        ]
        if not candidates:
            return
        # With explicit corpus/apartment markers, the remaining right-most number is
        # normally the house. Without them, plain-tail extraction is more reliable.
        if not any(name in components for name in ("corpus", "structure", "apartment")):
            return
        token = candidates[-1]
        components["house_num"] = _part(
            text,
            token.start,
            token.end,
            confidence=0.86,
            source="postprocessor",
            normalizer=normalize_number,
        )
        consumed.append((token.start, token.end))

    def _extract_with_model(
        self,
        text: str,
        tokens: Sequence[Token],
        components: dict[str, AddressPart],
        consumed: list[tuple[int, int]],
    ) -> None:
        if self.tagger is None:
            return
        residual = [
            token
            for token in tokens
            if token.is_word and not _overlaps(token.start, token.end, consumed)
        ]
        if not residual:
            return
        labels, confidences = self.tagger.tag(residual)
        runs: list[tuple[str, int, int, list[float]]] = []
        for token, label, confidence in zip(residual, labels, confidences):
            if label == "O" or label.lower() in components:
                continue
            if runs and runs[-1][0] == label and token.start - runs[-1][2] <= 2:
                previous = runs[-1]
                runs[-1] = (
                    previous[0],
                    previous[1],
                    token.end,
                    [*previous[3], confidence],
                )
            else:
                runs.append((label, token.start, token.end, [confidence]))
        for label, start, end, scores in runs:
            kind = label.lower()
            if kind not in {"region", "district", "city", "settlement", "street"}:
                continue
            if kind in components or _overlaps(start, end, consumed):
                continue
            components[kind] = _part(
                text,
                start,
                end,
                confidence=min(0.92, sum(scores) / len(scores)),
                source="model",
            )
            consumed.append((start, end))

    def _extract_implicit_segments(
        self,
        text: str,
        components: dict[str, AddressPart],
        consumed: list[tuple[int, int]],
    ) -> None:
        chunks: list[tuple[int, int]] = []
        for available_start, available_end in _available_spans(
            text, 0, len(text), consumed
        ):
            for match in re.finditer(
                rf"[{_LETTER}](?:[{_LETTER}\d]|[\s\-](?=[{_LETTER}\d]))*",
                text[available_start:available_end],
            ):
                start = available_start + match.start()
                end = available_start + match.end()
                start, end = _trim_span(text, start, end)
                if start < end and re.search(rf"[{_LETTER}]", text[start:end]):
                    chunks.append((start, end))

        if "street" not in components and chunks and "house_num" in components:
            start, end = chunks[-1]
            components["street"] = _part(
                text,
                start,
                end,
                confidence=0.82,
                source="postprocessor",
            )
            consumed.append((start, end))

        remaining = [
            span for span in chunks if not _overlaps(span[0], span[1], consumed)
        ]
        if "city" not in components and len(remaining) >= 2:
            start, end = remaining[0]
            components["city"] = _part(
                text,
                start,
                end,
                confidence=0.72,
                source="postprocessor",
            )
            consumed.append((start, end))

    def _unparsed(
        self,
        text: str,
        tokens: Sequence[Token],
        consumed: Sequence[tuple[int, int]],
    ) -> list[AddressPart]:
        residual = [
            token
            for token in tokens
            if (token.is_word or token.is_number)
            and not _overlaps(token.start, token.end, consumed)
        ]
        if not residual:
            return []

        groups: list[list[Token]] = []
        for token in residual:
            if groups and token.start - groups[-1][-1].end <= 2:
                groups[-1].append(token)
            else:
                groups.append([token])
        return [
            _part(
                text,
                group[0].start,
                group[-1].end,
                confidence=0.0,
                source="unparsed",
                normalizer=lambda value: value.strip(),
            )
            for group in groups
        ]
