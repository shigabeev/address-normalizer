from pathlib import Path
import sys

ROOT = Path(__file__).parents[1]
sys.path.insert(0, str(ROOT / "evaluation"))

from datamos_data import (
    expected_components,
    group_id_and_split,
    rejection_reason,
)
from evaluate_datamos import _summary, score


def _row() -> dict:
    return {
        "OnTerritoryOfMoscow": "да",
        "ADR_TYPE": "Официальный",
        "SOSTAD": "Зарегистрирован в АР",
        "STATUS": "Внесён в ГКН",
        "SIMPLE_ADDRESS": "Косинская улица, дом 26А",
        "ADDRESS": "город Москва, Косинская улица, дом 26А",
        "P7": "Косинская улица",
        "L1_VALUE": "26А",
        "L2_VALUE": "",
        "L3_VALUE": "",
        "N_FIAS": "235212A3-01E8-4CC3-87D5-59F00C83898A",
    }


def test_datamos_high_confidence_filter_and_grouping():
    row = _row()
    assert rejection_reason(row) is None
    assert expected_components(row) == {
        "street": "Косинская улица",
        "house_num": "26А",
        "corpus": None,
        "structure": None,
    }
    group_id, split = group_id_and_split(row)
    assert len(group_id) == 32
    assert split in {"train", "validation", "test"}
    row["STATUS"] = "Аннулирован в ГКН"
    assert rejection_reason(row) == "not_in_gkn"


def test_exact_moscow_component_evaluation():
    row = {
        "source_row": 0,
        "fias_id": "235212a3-01e8-4cc3-87d5-59f00c83898a",
        "tier": "house_only",
        "raw": "Косинская улица, дом 26А",
        "expected": expected_components(_row()),
    }
    report = score([row])
    assert report["micro"]["f1"] == 1.0
    assert report["exact_address_rate"] == 1.0
    assert report["exact_component_value_micro"] == report["micro"]
    assert report["metric_definitions"]["fields"].startswith("per-field")
    summary = _summary(report)
    assert summary["micro"] == report["exact_component_value_micro"]
    assert "metric_definitions" not in summary
    assert "exact_component_value_micro" not in summary
