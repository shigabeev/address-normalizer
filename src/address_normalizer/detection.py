"""Conservative address-span detection for free-form messages."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

from .parser import AddressParser
from .types import DetectedAddress


_LETTER = r"A-Za-zА-Яа-яЁё"
_SIMPLE_NUMBER = rf"\d+[{_LETTER}]?"
_SLASH_NUMBER = rf"{_SIMPLE_NUMBER}(?:\s*/\s*{_SIMPLE_NUMBER})?"
_HOUSE_NUMBER = rf"{_SLASH_NUMBER}(?:\s*-\s*{_SIMPLE_NUMBER})?"

_CUE_RE = re.compile(
    r"(?<![\w-])(?:по\s+адресу|"
    r"адрес(?:а|у|ом|е)?(?:\s+доставки|\s+офиса|\s+встречи)?)"
    r"(?!\w)\s*:?\s*",
    re.IGNORECASE,
)
_NETWORK_PREFIX_RE = re.compile(
    r"(?:ip|ipv4|ipv6)\s*[-:]?\s*$",
    re.IGNORECASE,
)
_NON_NAME_AFTER_MARKER_RE = re.compile(
    r"^\s*(?:в|во|на|по|для|из|к|с|со|от)\s+",
    re.IGNORECASE,
)
_STREET_MARKER_RE = re.compile(
    r"(?<!\w)(?:"
    r"ул(?:ица|ице|ицу|ицы|ицей)?|"
    r"пр-?кт|просп(?:ект|екта|екте|екту|ектом)?|"
    r"пер(?:еулок|еулка|еулке|еулку|еулком)?|"
    r"б-?р|бульвар|наб(?:ережная)?|ш(?:оссе)?|"
    r"проезд|пр-?д|пл(?:ощадь)?|аллея|мкр(?:орайон)?"
    r")\s*\.?(?!\w)",
    re.IGNORECASE,
)
_ORDINAL_BEFORE_MARKER_RE = re.compile(
    r"(?<!\w)\d+\s*-\s*(?:я|й|ая|ый|ой)\s*$",
    re.IGNORECASE,
)
_HOUSE_RE = re.compile(
    rf"(?<!\w)(?:д(?:ом)?|вл(?:адение)?)\s*\.?\s*(?:№\s*)?"
    rf"(?P<value>{_HOUSE_NUMBER})(?!\w)",
    re.IGNORECASE,
)
_UNIT_RE = re.compile(
    rf"(?<!\w)(?:"
    rf"корп(?:ус)?|кор|к|стр(?:оение)?|соор(?:ужение)?|"
    rf"кв(?:артира)?|комн(?:ата)?|оф(?:ис)?|пом(?:ещение)?"
    rf")\s*\.?\s*(?:№\s*)?(?P<value>{_SLASH_NUMBER})(?!\w)",
    re.IGNORECASE,
)
_BARE_NUMBER_RE = re.compile(rf"(?<![\w/]){_HOUSE_NUMBER}(?!\w)", re.IGNORECASE)
_POSTAL_RE = re.compile(r"(?<!\d)\d{6}(?!\d)")
_LOCATION_MARKER_RE = re.compile(
    r"(?<!\w)(?:г(?:ород)?|обл(?:асть)?|край|респ(?:ублика)?|"
    r"р-?н|район|пгт|пос(?:елок|ёлок)?|село|деревня)\s*\.?(?!\w)",
    re.IGNORECASE,
)
_COUNTRY_RE = re.compile(
    r"(?<!\w)(?:Российская\s+Федерация|Россия|РФ)(?!\w)",
    re.IGNORECASE,
)
_KNOWN_BARE_LOCATION_RE = re.compile(
    r"^(?:Москва|Санкт-Петербург|СПб|Ростов-на-Дону)$",
    re.IGNORECASE,
)
_ONLY_GAP_RE = re.compile(r"^[\s,./()«»\"'–—-]*$")
_TRIM_LEFT = " \t\r\n,;:()[]{}«»\"'–—"
_TRIM_RIGHT = " \t\r\n,;:.()[]{}«»\"'"
_MAX_STREET_TO_HOUSE = 160
_MAX_CANDIDATE = 240


@dataclass(frozen=True, slots=True)
class _Candidate:
    start: int
    end: int
    cue: bool
    street_marker: bool
    house_marker: bool


def _trim_span(text: str, start: int, end: int) -> tuple[int, int]:
    while start < end and text[start] in _TRIM_LEFT:
        start += 1
    while end > start and text[end - 1] in _TRIM_RIGHT:
        end -= 1
    return start, end


def _is_sentence_period(text: str, index: int) -> bool:
    next_index = index + 1
    while next_index < len(text) and text[next_index].isspace():
        next_index += 1
    if next_index >= len(text) or not text[next_index].isupper():
        return False

    previous = text[:index].rstrip()
    token_match = re.search(r"([A-Za-zА-Яа-яЁё]+|\d+)$", previous)
    if token_match is None:
        return True
    token = token_match.group(1).casefold().replace("ё", "е")
    abbreviations = {
        "г",
        "д",
        "вл",
        "ул",
        "пер",
        "пр",
        "стр",
        "корп",
        "кв",
        "ком",
        "комн",
        "кор",
        "наб",
        "обл",
        "оф",
        "пом",
        "просп",
        "р",
        "п",
        "с",
        "ш",
    }
    return token.isdigit() or token not in abbreviations


def _clauses(text: str) -> Iterable[tuple[int, int]]:
    start = 0
    for index, character in enumerate(text):
        boundary = character in ";\n!?"
        if character == ".":
            boundary = _is_sentence_period(text, index)
        if not boundary:
            continue
        left, right = _trim_span(text, start, index)
        if left < right:
            yield left, right
        start = index + 1
    left, right = _trim_span(text, start, len(text))
    if left < right:
        yield left, right


def _street_start(
    text: str,
    clause_start: int,
    street: re.Match[str],
    house_start: int,
) -> int:
    ordinal = _ORDINAL_BEFORE_MARKER_RE.search(
        text,
        clause_start,
        street.start(),
    )
    if ordinal is not None:
        return ordinal.start()

    after_marker = text[street.end() : house_start]
    if re.search(rf"[{_LETTER}]", after_marker):
        return street.start()

    delimiter = max(
        text.rfind(",", clause_start, street.start()),
        text.rfind(":", clause_start, street.start()),
    )
    return delimiter + 1 if delimiter >= clause_start else clause_start


def _is_location_chunk(value: str) -> bool:
    stripped = value.strip(" \t,")
    return bool(
        stripped
        and (
            _POSTAL_RE.fullmatch(stripped)
            or _COUNTRY_RE.search(stripped)
            or _LOCATION_MARKER_RE.search(stripped)
            or _KNOWN_BARE_LOCATION_RE.fullmatch(stripped)
        )
    )


def _extend_location_prefix(
    text: str,
    clause_start: int,
    address_start: int,
) -> int:
    extended = address_start
    cursor = address_start
    while cursor > clause_start:
        delimiter = cursor
        while delimiter > clause_start and text[delimiter - 1].isspace():
            delimiter -= 1
        if delimiter <= clause_start or text[delimiter - 1] != ",":
            break
        comma = delimiter - 1
        previous = max(
            text.rfind(",", clause_start, comma),
            text.rfind(":", clause_start, comma),
        )
        chunk_start = previous + 1 if previous >= clause_start else clause_start
        if not _is_location_chunk(text[chunk_start:comma]):
            break
        extended = chunk_start
        cursor = chunk_start
    return extended


def _extend_units(text: str, start: int, clause_end: int) -> int:
    end = start
    for unit in _UNIT_RE.finditer(text, start, clause_end):
        if not _ONLY_GAP_RE.fullmatch(text[end : unit.start()]):
            break
        end = unit.end()
    return end


def _select_bare_number(
    text: str,
    street: re.Match[str],
    numbers: list[re.Match[str]],
) -> re.Match[str]:
    after_comma = [
        number
        for number in numbers
        if re.search(r",\s*$", text[street.end() : number.start()])
    ]
    if after_comma:
        return after_comma[0]
    first = numbers[0]
    if (
        len(numbers) > 1
        and not text[street.end() : first.start()].strip(" .")
    ):
        return numbers[1]
    return first


def _explicit_candidates(text: str) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for clause_start, clause_end in _clauses(text):
        streets = list(_STREET_MARKER_RE.finditer(text, clause_start, clause_end))
        houses = list(_HOUSE_RE.finditer(text, clause_start, clause_end))
        cues = list(_CUE_RE.finditer(text, clause_start, clause_end))
        for house in houses:
            eligible = [
                street
                for street in streets
                if street.start() < house.start()
                and house.start() - street.end() <= _MAX_STREET_TO_HOUSE
            ]
            if not eligible:
                continue
            street = eligible[-1]
            cue = next(
                (
                    item
                    for item in reversed(cues)
                    if item.end() <= street.start()
                ),
                None,
            )
            prefix_start = cue.end() if cue is not None else clause_start
            start = _street_start(
                text,
                prefix_start,
                street,
                house.start(),
            )
            start = _extend_location_prefix(text, prefix_start, start)
            end = _extend_units(text, house.end(), clause_end)
            start, end = _trim_span(text, start, end)
            if start < end and end - start <= _MAX_CANDIDATE:
                candidates.append(
                    _Candidate(
                        start=start,
                        end=end,
                        cue=cue is not None,
                        street_marker=True,
                        house_marker=True,
                    )
                )

        for street in streets:
            if any(
                street.start() >= candidate.start
                and street.end() <= candidate.end
                for candidate in candidates
            ):
                continue
            numbers = [
                match
                for match in _BARE_NUMBER_RE.finditer(
                    text,
                    street.end(),
                    clause_end,
                )
                if match.start() - street.end() <= _MAX_STREET_TO_HOUSE
            ]
            if not numbers:
                continue
            number = _select_bare_number(text, street, numbers)
            cue = next(
                (
                    item
                    for item in reversed(cues)
                    if item.end() <= street.start()
                ),
                None,
            )
            if (
                cue is None
                and _NON_NAME_AFTER_MARKER_RE.match(
                    text[street.end() : number.start()]
                )
            ):
                continue
            prefix_start = cue.end() if cue is not None else clause_start
            start = _street_start(
                text,
                prefix_start,
                street,
                number.start(),
            )
            start = _extend_location_prefix(text, prefix_start, start)
            end = _extend_units(text, number.end(), clause_end)
            start, end = _trim_span(text, start, end)
            if start < end and end - start <= _MAX_CANDIDATE:
                candidates.append(
                    _Candidate(
                        start=start,
                        end=end,
                        cue=cue is not None,
                        street_marker=True,
                        house_marker=False,
                    )
                )
    return candidates


def _cue_candidates(text: str) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for clause_start, clause_end in _clauses(text):
        for cue in _CUE_RE.finditer(text, clause_start, clause_end):
            if _NETWORK_PREFIX_RE.search(text[clause_start : cue.start()]):
                continue
            numbers = list(_BARE_NUMBER_RE.finditer(text, cue.end(), clause_end))
            if not numbers:
                continue
            start, end = _trim_span(text, cue.end(), numbers[-1].end())
            if start < end and end - start <= _MAX_CANDIDATE:
                candidates.append(
                    _Candidate(
                        start=start,
                        end=end,
                        cue=True,
                        street_marker=bool(
                            _STREET_MARKER_RE.search(text, start, end)
                        ),
                        house_marker=bool(_HOUSE_RE.search(text, start, end)),
                    )
                )
    return candidates


def _signals(
    candidate: _Candidate,
    text: str,
    *,
    parsed_street: bool,
    parsed_house: bool,
    has_unparsed: bool,
) -> tuple[tuple[str, ...], float]:
    signals: list[str] = []
    score = 0.0
    if candidate.cue:
        signals.append("address_cue")
        score += 0.18
    if candidate.street_marker:
        signals.append("street_marker")
        score += 0.28
    if candidate.house_marker:
        signals.append("house_marker")
        score += 0.28
    if _POSTAL_RE.search(text):
        signals.append("postal_code")
        score += 0.08
    if _LOCATION_MARKER_RE.search(text):
        signals.append("location_marker")
        score += 0.06
    if _UNIT_RE.search(text):
        signals.append("unit_marker")
        score += 0.05
    if parsed_street:
        signals.append("parsed_street")
        score += 0.08
    if parsed_house:
        signals.append("parsed_house")
        score += 0.08
    if has_unparsed:
        signals.append("unparsed_text")
        score -= 0.05
    return tuple(signals), max(0.0, min(0.99, score))


class AddressDetector:
    """Detect conservative address candidates without registry lookups."""

    def __init__(self, parser: AddressParser) -> None:
        self._parser = parser

    def detect(self, text: str) -> tuple[DetectedAddress, ...]:
        """Return non-overlapping address-like spans in message order."""

        raw_candidates = [*_explicit_candidates(text), *_cue_candidates(text)]
        accepted: list[DetectedAddress] = []
        seen: set[tuple[int, int]] = set()
        for candidate in raw_candidates:
            key = candidate.start, candidate.end
            if key in seen:
                continue
            seen.add(key)
            candidate_text = text[candidate.start : candidate.end]
            parsed = self._parser.parse(candidate_text)
            has_street = parsed.street is not None
            has_house = parsed.house_num is not None
            if not (has_street and has_house):
                continue
            if candidate.cue and not candidate.street_marker:
                assert parsed.street is not None
                assert parsed.house_num is not None
                street_letters = re.sub(
                    rf"[^{_LETTER}]",
                    "",
                    parsed.street.raw,
                )
                word_count = len(
                    re.findall(rf"[{_LETTER}]+", candidate_text)
                )
                if (
                    len(street_letters) < 3
                    or word_count > 8
                    or len(parsed.house_num.value) > 6
                ):
                    continue
            if (
                not candidate.cue
                and not candidate.house_marker
                and parsed.unparsed
            ):
                continue
            if not (candidate.street_marker or candidate.cue):
                continue
            signals, confidence = _signals(
                candidate,
                candidate_text,
                parsed_street=has_street,
                parsed_house=has_house,
                has_unparsed=bool(parsed.unparsed),
            )
            accepted.append(
                DetectedAddress(
                    text=candidate_text,
                    start=candidate.start,
                    end=candidate.end,
                    confidence=confidence,
                    signals=signals,
                    parsed=parsed,
                )
            )

        ranked = sorted(
            accepted,
            key=lambda item: (
                item.start,
                -item.confidence,
                -(item.end - item.start),
            ),
        )
        non_overlapping: list[DetectedAddress] = []
        for item in ranked:
            overlapping = [
                existing
                for existing in non_overlapping
                if item.start < existing.end and item.end > existing.start
            ]
            if not overlapping:
                non_overlapping.append(item)
                continue
            best = max(
                [item, *overlapping],
                key=lambda value: (
                    value.confidence,
                    value.end - value.start,
                ),
            )
            if best is item:
                non_overlapping = [
                    existing
                    for existing in non_overlapping
                    if existing not in overlapping
                ]
                non_overlapping.append(item)
        return tuple(sorted(non_overlapping, key=lambda item: item.start))
