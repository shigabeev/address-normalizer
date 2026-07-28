from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


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
    assert json.loads(completed.stdout)["release_gate_passed"] is True
