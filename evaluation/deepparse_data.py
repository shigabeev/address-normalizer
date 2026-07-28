"""Pure helpers and pinned metadata for the Deepparse Russian address shard."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]
DATASET_REVISION = "cb61e5e49db87f8c3586b5494149f612460f8992"
DATASET_SHA256 = (
    "e61981a059967a1062fe445fa5ffe745a661f8b4afcec59a60c3cbb4f1444110"
)
DATASET_ROWS = 13_152_918
DATASET_URL = (
    "https://huggingface.co/datasets/deepparse/worldwide-addresses/resolve/"
    f"{DATASET_REVISION}/ru/chunk-0.parquet"
)
DEFAULT_SOURCE = (
    ROOT
    / ".cache"
    / "external"
    / f"deepparse-worldwide-ru-{DATASET_REVISION[:8]}.parquet"
)
DEFAULT_FILTERED = (
    ROOT
    / ".cache"
    / "external"
    / f"deepparse-ru-usable-{DATASET_REVISION[:8]}.parquet"
)
DEFAULT_SAMPLE = (
    ROOT
    / ".cache"
    / "external"
    / f"deepparse-ru-test-100k-{DATASET_REVISION[:8]}.jsonl.gz"
)
DEFAULT_MANIFEST = ROOT / "evaluation" / "deepparse_manifest.json"

SOURCE_TAGS = frozenset(
    {
        "Country",
        "Province",
        "County",
        "District",
        "Municipality",
        "Suburb",
        "PostalCode",
        "StreetName",
        "StreetNumber",
        "Unit",
    }
)
USEFUL_TAGS = frozenset({"StreetName", "StreetNumber", "Unit"})
LABEL_MAP = {
    "PostalCode": "POSTAL_CODE",
    "Province": "REGION",
    "County": "DISTRICT",
    "District": "DISTRICT",
    "Municipality": "CITY",
    "StreetName": "STREET",
    "StreetNumber": "HOUSE",
    "Unit": "APARTMENT",
}
SCORED_LABELS = (
    "POSTAL_CODE",
    "REGION",
    "DISTRICT",
    "CITY",
    "STREET",
    "HOUSE",
    "APARTMENT",
)
EXPECTED_FIELDS = (
    "postal_code",
    "region",
    "district",
    "city",
    "street",
    "house_num",
    "apartment",
)
LABEL_FIELDS = {
    "POSTAL_CODE": "postal_code",
    "REGION": "region",
    "DISTRICT": "district",
    "CITY": "city",
    "STREET": "street",
    "HOUSE": "house_num",
    "APARTMENT": "apartment",
}
_IDENTITY_TAGS = (
    "Province",
    "County",
    "District",
    "Municipality",
    "Suburb",
    "StreetName",
    "StreetNumber",
)
_SPACE_RE = re.compile(r"\s+")


def fold(value: str) -> str:
    """Normalize only distinctions that are not useful address evidence."""

    return _SPACE_RE.sub(" ", value.casefold().replace("ё", "е")).strip()


def normalized_address_id(address: str) -> bytes:
    """Return a compact deterministic ID used for exact-text deduplication."""

    return hashlib.blake2b(
        fold(address).encode("utf-8"),
        digest_size=16,
        person=b"addr-example-v1",
    ).digest()


def mapped_labels(tags: Sequence[str]) -> tuple[str, ...]:
    return tuple(LABEL_MAP.get(tag, "O") for tag in tags)


def expected_components(
    tokens: Sequence[str],
    labels: Sequence[str],
) -> dict[str, str | None]:
    values: dict[str, list[str]] = {
        field: [] for field in EXPECTED_FIELDS
    }
    for token, label in zip(tokens, labels):
        field = LABEL_FIELDS.get(label)
        if field is not None:
            values[field].append(token)
    return {
        field: " ".join(parts) if parts else None
        for field, parts in values.items()
    }


def quality_tier(tags: Sequence[str]) -> str:
    present = set(tags)
    if {"StreetName", "StreetNumber", "Unit"} <= present:
        return "street_house_unit"
    if {"StreetName", "StreetNumber"} <= present:
        return "street_house"
    if "StreetName" in present:
        return "street_only"
    return "number_or_unit_only"


def canonical_group_key(
    tokens: Sequence[str],
    tags: Sequence[str],
) -> bytes:
    """Group formatting variants without leaking one building across splits."""

    by_tag: dict[str, list[str]] = {tag: [] for tag in _IDENTITY_TAGS}
    for token, tag in zip(tokens, tags):
        if tag in by_tag:
            by_tag[tag].append(token)
    identity = [
        [tag, fold(" ".join(by_tag[tag]))]
        for tag in _IDENTITY_TAGS
        if by_tag[tag]
    ]
    if not identity:
        identity = [["fallback", fold(" ".join(tokens))]]
    return json.dumps(
        identity,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def group_id_and_split(
    tokens: Sequence[str],
    tags: Sequence[str],
) -> tuple[bytes, str]:
    key = canonical_group_key(tokens, tags)
    digest = hashlib.sha256(b"address-group-v1\0" + key).digest()
    bucket = int.from_bytes(digest[:8], "big") % 10_000
    split = (
        "train"
        if bucket < 9_000
        else "validation"
        if bucket < 9_500
        else "test"
    )
    return digest[:16], split


def sample_rank(example_id: bytes) -> int:
    return int.from_bytes(
        hashlib.sha256(b"benchmark-sample-v1\0" + example_id).digest()[:8],
        "big",
    )
