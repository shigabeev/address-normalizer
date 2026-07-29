from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).parents[1]


def _load_analyzer():
    path = ROOT / "evaluation/analyze_failures.py"
    spec = importlib.util.spec_from_file_location("analyze_failures", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_diagnostic_table_covers_every_reference_row_with_narrow_columns():
    with (
        ROOT / "evaluation/legacy_reference_500_diagnostics.csv"
    ).open(encoding="utf-8", newline="") as source:
        rows = list(csv.DictReader(source))

    assert len(rows) == 500
    assert len({row["id"] for row in rows}) == 500
    required = {
        "exact_address",
        "triage_priority",
        "diagnosis_status",
        "mismatch_fields",
        "missing_fields",
        "extra_fields",
        "wrong_value_fields",
        "failure_types",
        "primary_likely_cause",
        "likely_causes",
        "failure_summary",
        "warnings",
        "unparsed_spans",
        "scenario_tags",
        "street_marker_style",
        "has_compact_punctuation",
        "has_compound_number",
        "has_unmarked_numeric_sequence",
        "expected_street",
        "actual_street",
        "status_street",
        "expected_house_num",
        "actual_house_num",
        "status_house_num",
    }
    assert required <= set(rows[0])
    assert sum(row["exact_address"] == "false" for row in rows) == 98
    assert all(
        row["diagnosis_status"] == "heuristic_needs_human_review"
        for row in rows
        if row["exact_address"] == "false"
    )


def test_failure_summary_matches_current_diagnostics():
    analyzer = _load_analyzer()
    source_rows = analyzer._load_rows(
        ROOT / "evaluation/legacy_reference_500.jsonl"
    )
    diagnostics = [analyzer.diagnose_row(row) for row in source_rows]
    with (
        ROOT / "evaluation/legacy_reference_500_diagnostics.csv"
    ).open(encoding="utf-8", newline="") as source:
        committed_diagnostics = list(csv.DictReader(source))
    actual = analyzer.summarize(diagnostics)
    committed = json.loads(
        (
            ROOT / "evaluation/legacy_reference_500_failure_summary.json"
        ).read_text(encoding="utf-8")
    )

    assert diagnostics == committed_diagnostics
    assert actual == committed
    assert actual["rows"] == 500
    assert actual["failed_rows"] == 98
    assert len(actual["representative_failure_sample"]) == 10
