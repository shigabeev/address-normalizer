from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).parents[1]


def test_legacy_reference_is_unique_and_fixed_size():
    rows = [
        json.loads(line)
        for line in (ROOT / "evaluation/legacy_reference_500.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    assert len(rows) == 500
    assert len({row["raw"] for row in rows}) == 500
    assert all(row["expected"].get("street") for row in rows)
    assert all(row["expected"].get("house_num") for row in rows)


def test_legacy_release_gate_passes():
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "evaluation/evaluate.py"),
            "--data",
            str(ROOT / "evaluation/legacy_reference_500.jsonl"),
            "--gates",
            str(ROOT / "evaluation/release_gates.json"),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    report = json.loads(completed.stdout)
    assert report["release_gate_passed"] is True
    assert report["exact_component_value_micro"] == report["micro"]
    assert "exact component values" in (
        report["metric_definitions"]["exact_component_value_micro"]
    )


@pytest.mark.parametrize(
    ("filename", "aliases"),
    [
        ("redmadrobot_report.json", {"span_overlap_micro": "micro"}),
        (
            "deepparse_report.json",
            {
                "span_overlap_micro": "micro",
                "character_overlap_micro": "character_micro",
                "token_label_micro": "token_micro",
            },
        ),
        ("datamos_report.json", {"exact_component_value_micro": "micro"}),
    ],
)
def test_committed_reports_use_explicit_metric_names(filename, aliases):
    report = json.loads((ROOT / "evaluation" / filename).read_text(encoding="utf-8"))

    assert report["metric_definitions"]
    for explicit_name, compatibility_name in aliases.items():
        assert report[explicit_name] == report[compatibility_name]
