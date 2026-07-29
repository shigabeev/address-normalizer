#!/usr/bin/env python3
"""Create a row-level diagnostic table for the historical reference set.

The likely-cause labels are deterministic triage hints, not human-verified
causal ground truth. They make recurring failure shapes visible before a
maintainer decides whether the parser, the reference label, or both need work.
"""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import json
from pathlib import Path
import re
import sys
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from address_normalizer import parse
from address_normalizer.types import ParsedAddress


FIELDS = (
    "postal_code",
    "region",
    "district",
    "city",
    "settlement",
    "street",
    "street_type",
    "house_num",
    "corpus",
    "structure",
    "apartment",
)
NUMERIC_FIELDS = {"house_num", "corpus", "structure", "apartment"}
ADMIN_FIELDS = {"region", "district", "city", "settlement"}
STREET_TYPE_PATTERNS = {
    "ул": re.compile(r"(?<!\w)(?:ул(?:ица)?)\.?(?!\w)", re.IGNORECASE),
    "пр-кт": re.compile(
        r"(?<!\w)(?:пр-?кт|просп(?:ект)?|пр-?т)\.?(?!\w)",
        re.IGNORECASE,
    ),
    "пер": re.compile(r"(?<!\w)(?:пер(?:еулок)?)\.?(?!\w)", re.IGNORECASE),
    "б-р": re.compile(r"(?<!\w)(?:б-?р|бул(?:ьвар)?)\.?(?!\w)", re.IGNORECASE),
    "наб": re.compile(r"(?<!\w)(?:наб(?:ережная)?)\.?(?!\w)", re.IGNORECASE),
    "ш": re.compile(r"(?<!\w)(?:ш(?:оссе)?)\.?(?!\w)", re.IGNORECASE),
    "пр-д": re.compile(
        r"(?<!\w)(?:проезд|пр-?д|пр\.)",
        re.IGNORECASE,
    ),
    "пл": re.compile(r"(?<!\w)(?:пл(?:ощадь)?)\.?(?!\w)", re.IGNORECASE),
    "аллея": re.compile(r"(?<!\w)аллея(?!\w)", re.IGNORECASE),
    "мкр": re.compile(
        r"(?<!\w)(?:мкр(?:орайон)?|микрорайон)\.?(?!\w)",
        re.IGNORECASE,
    ),
}
ANY_STREET_MARKER_RE = re.compile(
    r"(?<!\w)(?:ул(?:ица)?|пр-?кт|просп(?:ект)?|пр-?т|"
    r"пер(?:еулок)?|б-?р|бул(?:ьвар)?|наб(?:ережная)?|"
    r"ш(?:оссе)?|проезд|пр-?д|пл(?:ощадь)?|аллея|"
    r"мкр(?:орайон)?|микрорайон)\.?(?!\w)",
    re.IGNORECASE,
)
HOUSE_MARKER_RE = re.compile(
    r"(?<!\w)(?:д(?:ом)?|вл(?:адение)?)\.?(?!\w)",
    re.IGNORECASE,
)
UNIT_MARKER_RE = re.compile(
    r"(?<!\w)(?:корп(?:ус)?|кор|к|стр(?:оение)?|с|"
    r"кв(?:артира)?|комн?(?:ата)?|оф(?:ис)?|пом(?:ещение)?)\.?(?!\w)",
    re.IGNORECASE,
)
COUNTRY_RE = re.compile(
    r"(?<!\w)(?:Российская\s+Федерация|Россия|РФ)(?!\w)",
    re.IGNORECASE,
)
POSTAL_RE = re.compile(r"(?<!\d)\d{6}(?!\d)")
COMPACT_PUNCTUATION_RE = re.compile(r"[,.;:]\S")
UNMARKED_NUMERIC_SEQUENCE_RE = re.compile(
    r",\s*\d+[A-Za-zА-Яа-яЁё/.-]*\s*,\s*"
    r"\d+[A-Za-zА-Яа-яЁё/.-]*"
)
COMPOUND_NUMBER_RE = re.compile(
    r"(?<!\w)\d+[A-Za-zА-Яа-яЁё]?(?:\s*[/\-]\s*"
    r"\d+[A-Za-zА-Яа-яЁё]?)+(?!\w)"
)
LETTER_SUFFIX_RE = re.compile(r"(?<!\w)\d+[A-Za-zА-Яа-яЁё](?!\w)")
ORDINAL_STREET_RE = re.compile(
    r"(?<!\w)\d+\s*-\s*(?:я|й|ая|ый|ой)(?!\w)",
    re.IGNORECASE,
)
AMBIGUOUS_ABBREVIATION_RE = re.compile(
    r"(?<!\w)(?:пр|с|ком|уп)\.",
    re.IGNORECASE,
)
NUMERIC_MARKER_PATTERNS = {
    "house_num": HOUSE_MARKER_RE,
    "corpus": re.compile(
        r"(?<!\w)(?:корп(?:ус)?|кор|к)\.?(?!\w)",
        re.IGNORECASE,
    ),
    "structure": re.compile(
        r"(?<!\w)(?:стр(?:оение)?|с)\.?(?!\w)",
        re.IGNORECASE,
    ),
    "apartment": re.compile(
        r"(?<!\w)(?:кв(?:артира)?|комн?(?:ата)?|оф(?:ис)?|"
        r"пом(?:ещение)?)\.?(?!\w)",
        re.IGNORECASE,
    ),
}


def _fold(value: str | None) -> str | None:
    if value is None:
        return None
    return " ".join(value.casefold().replace("ё", "е").split())


def _boundary_fold(value: str | None) -> str | None:
    folded = _fold(value)
    if folded is None:
        return None
    return re.sub(r"[\W_]+", "", folded)


def _load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if not isinstance(row, dict) or not isinstance(row.get("raw"), str):
            raise ValueError(f"{path}:{line_number}: invalid evaluation row")
        if not isinstance(row.get("expected"), dict):
            raise ValueError(f"{path}:{line_number}: expected must be an object")
        rows.append(row)
    return rows


def _values(result: ParsedAddress) -> dict[str, str | None]:
    return {
        field: (
            getattr(result, field).value
            if getattr(result, field) is not None
            else None
        )
        for field in FIELDS
    }


def _field_status(expected: str | None, actual: str | None) -> str:
    wanted = _fold(expected)
    observed = _fold(actual)
    if wanted == observed:
        return "match"
    if wanted is not None and observed is None:
        return "missing"
    if wanted is None and observed is not None:
        return "extra"
    return "wrong_value"


def _street_marker_style(raw: str, expected_street: str | None) -> str:
    markers = list(ANY_STREET_MARKER_RE.finditer(raw))
    if not markers:
        return "absent"
    if len(markers) > 1:
        return "multiple"
    if not expected_street:
        return "present_unknown_position"
    street_index = _fold(raw).find(_fold(expected_street) or "")
    if street_index < 0:
        return "present_street_not_literal"
    return "prefix" if markers[0].start() <= street_index else "suffix"


def _scenario_columns(
    raw: str,
    expected: dict[str, Any],
) -> dict[str, str]:
    expected_street = (
        str(expected["street"]) if expected.get("street") is not None else None
    )
    expected_city = (
        str(expected["city"]) if expected.get("city") is not None else None
    )
    marker_style = _street_marker_style(raw, expected_street)
    booleans = {
        "has_postal_code": bool(POSTAL_RE.search(raw)),
        "has_country_phrase": bool(COUNTRY_RE.search(raw)),
        "has_administrative_expected": any(
            expected.get(field) is not None for field in ADMIN_FIELDS
        ),
        "has_unit_expected": any(
            expected.get(field) is not None
            for field in ("corpus", "structure", "apartment")
        ),
        "has_street_marker": bool(ANY_STREET_MARKER_RE.search(raw)),
        "has_house_marker": bool(HOUSE_MARKER_RE.search(raw)),
        "has_unit_marker": bool(UNIT_MARKER_RE.search(raw)),
        "has_compact_punctuation": bool(COMPACT_PUNCTUATION_RE.search(raw)),
        "has_unicode_whitespace": any(
            character.isspace() and character != " " for character in raw
        ),
        "has_compound_number": bool(COMPOUND_NUMBER_RE.search(raw)),
        "has_slash_number": bool(re.search(r"\d\s*/\s*\d", raw)),
        "has_hyphenated_number": bool(re.search(r"\d\s*-\s*\d", raw)),
        "has_letter_suffix_number": bool(LETTER_SUFFIX_RE.search(raw)),
        "has_unmarked_numeric_sequence": bool(
            UNMARKED_NUMERIC_SEQUENCE_RE.search(raw)
        ),
        "has_ordinal_street": bool(ORDINAL_STREET_RE.search(raw)),
        "has_ambiguous_abbreviation": bool(
            AMBIGUOUS_ABBREVIATION_RE.search(raw)
        ),
        "has_multiword_street": bool(
            expected_street and len(expected_street.split()) > 1
        ),
        "has_repeated_city": bool(
            expected_city
            and _fold(raw).count(_fold(expected_city) or "") > 1
        ),
    }
    tags = [
        name.removeprefix("has_")
        for name, present in booleans.items()
        if present
    ]
    tags.append(f"street_marker_{marker_style}")
    return {
        **{name: str(value).lower() for name, value in booleans.items()},
        "street_marker_style": marker_style,
        "scenario_tags": "|".join(tags),
    }


def _expected_street_marker_is_visible(
    raw: str,
    expected_type: str | None,
) -> bool:
    if expected_type is None:
        return False
    pattern = STREET_TYPE_PATTERNS.get(expected_type)
    return bool(pattern and pattern.search(raw))


def _diagnose(
    raw: str,
    expected: dict[str, str | None],
    actual: dict[str, str | None],
    statuses: dict[str, str],
    result: ParsedAddress,
) -> tuple[list[str], list[str], str]:
    mismatch_fields = [
        field for field, status in statuses.items() if status != "match"
    ]
    failure_types = sorted({statuses[field] for field in mismatch_fields})
    causes: set[str] = set()

    expected_by_value = {
        _fold(value): field
        for field, value in expected.items()
        if value is not None
    }
    for field in mismatch_fields:
        wanted = expected.get(field)
        observed = actual.get(field)
        wanted_folded = _fold(wanted)
        observed_folded = _fold(observed)

        if observed_folded is not None and observed_folded in expected_by_value:
            if expected_by_value[observed_folded] != field:
                causes.add("component_label_confusion")

        if field == "street_type":
            if AMBIGUOUS_ABBREVIATION_RE.search(raw):
                causes.add("ambiguous_or_unsupported_abbreviation")
            elif (
                wanted is not None
                and not _expected_street_marker_is_visible(raw, wanted)
            ):
                if ANY_STREET_MARKER_RE.search(raw):
                    causes.add("reference_conflicts_with_explicit_street_type")
                else:
                    causes.add("reference_infers_missing_street_type")
            elif len(list(ANY_STREET_MARKER_RE.finditer(raw))) > 1:
                causes.add("conflicting_street_markers")
            else:
                causes.add("street_type_recognition")
            continue

        if field in NUMERIC_FIELDS:
            if AMBIGUOUS_ABBREVIATION_RE.search(raw):
                causes.add("ambiguous_or_unsupported_abbreviation")
            if UNMARKED_NUMERIC_SEQUENCE_RE.search(raw):
                causes.add("unmarked_numeric_role_ambiguity")
            if wanted and observed and (
                wanted_folded in (observed_folded or "")
                or (observed_folded or "") in (wanted_folded or "")
            ):
                causes.add("compound_or_letter_number_boundary")
            elif wanted and (
                "/" in wanted or "-" in wanted or LETTER_SUFFIX_RE.search(wanted)
            ):
                causes.add("compound_or_letter_number_boundary")
            elif statuses[field] == "missing":
                causes.add("numeric_component_not_recognized")
            elif statuses[field] == "extra":
                causes.add("spurious_numeric_component")
            else:
                causes.add("numeric_value_or_role")
            continue

        if field in ADMIN_FIELDS:
            if statuses[field] == "missing":
                causes.add("administrative_component_missed")
            elif statuses[field] == "extra":
                causes.add("spurious_administrative_component")
            else:
                causes.add("administrative_label_or_boundary")
            continue

        if field == "street":
            if _boundary_fold(wanted) == _boundary_fold(observed):
                causes.add("normalization_only_difference")
            elif wanted_folded and observed_folded and (
                wanted_folded in observed_folded
                or observed_folded in wanted_folded
            ):
                causes.add("street_span_boundary")
            elif statuses[field] == "missing":
                causes.add("street_not_recognized")
            else:
                causes.add("street_label_or_value")
            continue

        causes.add(f"{field}_{statuses[field]}")

    for expected_field in NUMERIC_FIELDS:
        wanted = _fold(expected.get(expected_field))
        if wanted is None:
            continue
        for actual_field in NUMERIC_FIELDS - {expected_field}:
            if (
                _fold(actual.get(actual_field)) == wanted
                and expected.get(actual_field) is None
                and NUMERIC_MARKER_PATTERNS[actual_field].search(raw)
            ):
                causes.add("reference_conflicts_with_explicit_numeric_marker")
    summary = "; ".join(
        f"{field}: expected={expected.get(field)!r}, actual={actual.get(field)!r}"
        for field in mismatch_fields
    )
    return failure_types, sorted(causes), summary


def _primary_cause(causes: Iterable[str]) -> str:
    available = set(causes)
    priority = (
        "reference_conflicts_with_explicit_numeric_marker",
        "reference_conflicts_with_explicit_street_type",
        "reference_infers_missing_street_type",
        "ambiguous_or_unsupported_abbreviation",
        "conflicting_street_markers",
        "unmarked_numeric_role_ambiguity",
        "compound_or_letter_number_boundary",
        "component_label_confusion",
        "numeric_component_not_recognized",
        "numeric_value_or_role",
        "administrative_component_missed",
        "administrative_label_or_boundary",
        "spurious_administrative_component",
        "street_span_boundary",
        "normalization_only_difference",
        "street_type_recognition",
        "street_label_or_value",
        "street_not_recognized",
        "spurious_numeric_component",
    )
    return next(
        (cause for cause in priority if cause in available),
        sorted(available)[0] if available else "",
    )


def diagnose_row(row: dict[str, Any]) -> dict[str, str]:
    """Return one flat, CSV-ready diagnostic record."""

    raw = str(row["raw"])
    expected = {
        field: (
            str(row["expected"][field])
            if row["expected"].get(field) is not None
            else None
        )
        for field in FIELDS
    }
    result = parse(raw)
    actual = _values(result)
    statuses = {
        field: _field_status(expected[field], actual[field])
        for field in FIELDS
    }
    mismatch_fields = [
        field for field, status in statuses.items() if status != "match"
    ]
    missing_fields = [
        field for field, status in statuses.items() if status == "missing"
    ]
    extra_fields = [
        field for field, status in statuses.items() if status == "extra"
    ]
    wrong_value_fields = [
        field for field, status in statuses.items() if status == "wrong_value"
    ]
    failure_types, causes, failure_summary = _diagnose(
        raw,
        expected,
        actual,
        statuses,
        result,
    )
    if not mismatch_fields:
        triage_priority = "none"
        diagnosis_status = "not_applicable"
    elif causes == ["reference_infers_missing_street_type"]:
        triage_priority = "reference_review"
        diagnosis_status = "heuristic_needs_human_review"
    elif missing_fields or "component_label_confusion" in causes:
        triage_priority = "high"
        diagnosis_status = "heuristic_needs_human_review"
    elif causes == ["normalization_only_difference"]:
        triage_priority = "low"
        diagnosis_status = "heuristic_needs_human_review"
    else:
        triage_priority = "medium"
        diagnosis_status = "heuristic_needs_human_review"

    source = row.get("source")
    source_row = source.get("row") if isinstance(source, dict) else None
    output = {
        "id": str(row.get("id", "")),
        "dataset": "legacy_reference_500",
        "source_row": "" if source_row is None else str(source_row),
        "raw": raw,
        "review_status": str(row.get("review_status", "unspecified")),
        "exact_address": str(not mismatch_fields).lower(),
        "triage_priority": triage_priority,
        "diagnosis_status": diagnosis_status,
        "parser_confidence": f"{result.confidence:.6f}",
        "expected_component_count": str(
            sum(value is not None for value in expected.values())
        ),
        "actual_component_count": str(
            sum(value is not None for value in actual.values())
        ),
        "mismatch_count": str(len(mismatch_fields)),
        "mismatch_fields": "|".join(mismatch_fields),
        "missing_fields": "|".join(missing_fields),
        "extra_fields": "|".join(extra_fields),
        "wrong_value_fields": "|".join(wrong_value_fields),
        "failure_types": "|".join(failure_types),
        "primary_likely_cause": _primary_cause(causes),
        "likely_causes": "|".join(causes),
        "failure_summary": failure_summary,
        "warnings": "|".join(result.warnings),
        "unparsed_spans": json.dumps(
            [part.raw for part in result.unparsed],
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "alternatives": json.dumps(
            [alternative.as_dict() for alternative in result.alternatives],
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        **_scenario_columns(raw, expected),
    }
    for field in FIELDS:
        output[f"expected_{field}"] = expected[field] or ""
        output[f"actual_{field}"] = actual[field] or ""
        output[f"status_{field}"] = statuses[field]
    return output


def _representative_failures(
    diagnostics: Iterable[dict[str, str]],
    limit: int = 10,
) -> list[dict[str, str]]:
    selected: list[dict[str, str]] = []
    used_causes: set[str] = set()
    failures = [
        row for row in diagnostics if row["exact_address"] == "false"
    ]
    for row in failures:
        causes = row["likely_causes"].split("|")
        if any(cause not in used_causes for cause in causes):
            selected.append(row)
            used_causes.update(causes)
        if len(selected) == limit:
            return selected
    for row in failures:
        if row not in selected:
            selected.append(row)
        if len(selected) == limit:
            break
    return selected


def summarize(diagnostics: list[dict[str, str]]) -> dict[str, Any]:
    """Summarize failure counts without collapsing metric domains."""

    failures = [
        row for row in diagnostics if row["exact_address"] == "false"
    ]
    cause_counts: Counter[str] = Counter()
    primary_cause_counts: Counter[str] = Counter()
    field_counts: Counter[str] = Counter()
    scenario_counts: Counter[str] = Counter()
    for row in failures:
        cause_counts.update(filter(None, row["likely_causes"].split("|")))
        primary_cause_counts.update([row["primary_likely_cause"]])
        field_counts.update(filter(None, row["mismatch_fields"].split("|")))
        scenario_counts.update(filter(None, row["scenario_tags"].split("|")))
    sample_columns = (
        "id",
        "raw",
        "mismatch_fields",
        "primary_likely_cause",
        "likely_causes",
        "failure_summary",
        "unparsed_spans",
    )
    return {
        "dataset": "legacy_reference_500",
        "rows": len(diagnostics),
        "exact_rows": len(diagnostics) - len(failures),
        "failed_rows": len(failures),
        "diagnostic_semantics": (
            "likely_causes are deterministic triage hypotheses and require "
            "human review; they are not causal ground truth"
        ),
        "failure_rows_by_likely_cause": dict(cause_counts.most_common()),
        "failure_rows_by_primary_likely_cause": dict(
            primary_cause_counts.most_common()
        ),
        "failure_rows_by_mismatch_field": dict(field_counts.most_common()),
        "scenario_tags_on_failure_rows": dict(scenario_counts.most_common()),
        "representative_failure_sample": [
            {column: row[column] for column in sample_columns}
            for row in _representative_failures(diagnostics)
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data",
        type=Path,
        default=ROOT / "evaluation/legacy_reference_500.jsonl",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "evaluation/legacy_reference_500_diagnostics.csv",
    )
    parser.add_argument(
        "--summary-output",
        type=Path,
        default=ROOT / "evaluation/legacy_reference_500_failure_summary.json",
    )
    args = parser.parse_args(argv)

    diagnostics = [diagnose_row(row) for row in _load_rows(args.data)]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as target:
        writer = csv.DictWriter(
            target,
            fieldnames=list(diagnostics[0]),
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(diagnostics)

    report = summarize(diagnostics)
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    args.summary_output.parent.mkdir(parents=True, exist_ok=True)
    args.summary_output.write_text(f"{rendered}\n", encoding="utf-8")
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
