"""Pinned metadata and pure transformations for the Moscow address registry."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
from typing import Any
import uuid


ROOT = Path(__file__).resolve().parents[1]
DATASET_ID = 60_562
DATASET_VERSION = "3.630"
DATASET_DATE = "2021-10-15"
SOURCE_ROWS = 440_399
ARCHIVE_SHA256 = (
    "a272480189bf1e17e70b0e0b2e115520896ff1d168681d452697c3908ad75a66"
)
INNER_DATA_SHA256 = (
    "c253335e8b25fce0264837c026536689b33ff9fc5ac9d305158685d4528d95bb"
)
ARCHIVE_URL = (
    "https://data2.apicrafter.ru/packages/datamos-addressreestr/build/"
    "datamos-7705031674-AddressReestr-2021-10-23-7-55/get"
)
DEFAULT_ARCHIVE = (
    ROOT / ".cache" / "external" / "datamos-addressreestr-2021-10-23.zip"
)
DEFAULT_FILTERED = (
    ROOT
    / ".cache"
    / "external"
    / "datamos-addressreestr-usable-2021-10-23.jsonl.gz"
)
DEFAULT_MANIFEST = ROOT / "evaluation" / "datamos_manifest.json"
FIELDS = ("street", "house_num", "corpus", "structure")
_SPACE_RE = re.compile(r"\s+")


def fold(value: str) -> str:
    return _SPACE_RE.sub(" ", value.casefold().replace("ё", "е")).strip(" ,.;")


def rejection_reason(row: dict[str, Any]) -> str | None:
    if row.get("OnTerritoryOfMoscow") != "да":
        return "outside_moscow"
    if row.get("ADR_TYPE") != "Официальный":
        return "not_official"
    if row.get("SOSTAD") != "Зарегистрирован в АР":
        return "not_registered"
    if row.get("STATUS") != "Внесён в ГКН":
        return "not_in_gkn"
    for field in ("SIMPLE_ADDRESS", "P7", "L1_VALUE"):
        if not str(row.get(field, "")).strip():
            return f"missing_{field.lower()}"
    try:
        uuid.UUID(str(row.get("N_FIAS", "")))
    except (ValueError, AttributeError):
        return "invalid_fias_id"
    return None


def expected_components(row: dict[str, Any]) -> dict[str, str | None]:
    return {
        "street": str(row["P7"]).strip(),
        "house_num": str(row["L1_VALUE"]).strip(),
        "corpus": str(row.get("L2_VALUE", "")).strip() or None,
        "structure": str(row.get("L3_VALUE", "")).strip() or None,
    }


def quality_tier(row: dict[str, Any]) -> str:
    has_corpus = bool(str(row.get("L2_VALUE", "")).strip())
    has_structure = bool(str(row.get("L3_VALUE", "")).strip())
    if has_corpus and has_structure:
        return "house_corpus_structure"
    if has_corpus:
        return "house_corpus"
    if has_structure:
        return "house_structure"
    return "house_only"


def group_id_and_split(row: dict[str, Any]) -> tuple[str, str]:
    identity = [
        fold(str(row.get(field, "")))
        for field in ("P7", "L1_VALUE", "L2_VALUE", "L3_VALUE")
    ]
    key = json.dumps(
        identity,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hashlib.sha256(b"datamos-building-v1\0" + key).digest()
    bucket = int.from_bytes(digest[:8], "big") % 10_000
    split = (
        "train"
        if bucket < 9_000
        else "validation"
        if bucket < 9_500
        else "test"
    )
    return digest[:16].hex(), split
