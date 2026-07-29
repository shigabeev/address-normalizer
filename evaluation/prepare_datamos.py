"""Filter a pinned Moscow official-address snapshot into deterministic splits."""

from __future__ import annotations

import argparse
from collections import Counter
import gzip
import hashlib
import io
import json
from pathlib import Path
import sys
from typing import Any, Iterable
from urllib.request import Request, urlopen
import zipfile

from datamos_data import (
    ARCHIVE_SHA256,
    ARCHIVE_URL,
    DATASET_DATE,
    DATASET_ID,
    DATASET_VERSION,
    DEFAULT_ARCHIVE,
    DEFAULT_FILTERED,
    DEFAULT_MANIFEST,
    INNER_DATA_SHA256,
    SOURCE_ROWS,
    expected_components,
    fold,
    group_id_and_split,
    quality_tier,
    rejection_reason,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _archive_member_sha256(path: Path, member: str) -> str:
    digest = hashlib.sha256()
    with zipfile.ZipFile(path) as archive:
        with archive.open(member) as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()


def download_source(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.part")
    request = Request(ARCHIVE_URL, headers={"User-Agent": "address-normalizer/2"})
    with urlopen(request, timeout=120) as response, temporary.open("wb") as output:
        while chunk := response.read(1024 * 1024):
            output.write(chunk)
    actual = _sha256(temporary)
    if actual != ARCHIVE_SHA256:
        temporary.unlink(missing_ok=True)
        raise ValueError(
            f"archive checksum mismatch: expected {ARCHIVE_SHA256}, got {actual}"
        )
    temporary.replace(path)


def _documents(path: Path) -> Iterable[dict[str, Any]]:
    try:
        from bson import decode_file_iter
    except ModuleNotFoundError as error:
        raise SystemExit(
            "PyMongo is required only for Moscow dataset preparation. Install "
            "`requirements-evaluation.txt` in a separate environment."
        ) from error
    with zipfile.ZipFile(path) as archive:
        with archive.open("data.bson.gz") as compressed_source:
            with gzip.GzipFile(fileobj=compressed_source, mode="rb") as source:
                yield from decode_file_iter(source)


def prepare(
    source: Path,
    filtered: Path,
    manifest_path: Path,
    *,
    overwrite: bool,
) -> dict[str, Any]:
    for path in (filtered, manifest_path):
        if path.exists() and not overwrite:
            raise FileExistsError(f"{path} already exists; pass --overwrite")
    actual_archive_sha256 = _sha256(source)
    if actual_archive_sha256 != ARCHIVE_SHA256:
        raise ValueError(
            f"archive checksum mismatch: expected {ARCHIVE_SHA256}, "
            f"got {actual_archive_sha256}"
        )
    actual_inner_sha256 = _archive_member_sha256(source, "data.bson.gz")
    if actual_inner_sha256 != INNER_DATA_SHA256:
        raise ValueError(
            f"inner data checksum mismatch: expected {INNER_DATA_SHA256}, "
            f"got {actual_inner_sha256}"
        )

    filtered.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = filtered.with_suffix(f"{filtered.suffix}.part")
    counts: Counter[str] = Counter()
    seen: set[str] = set()
    with temporary.open("wb") as raw_output:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw_output,
            mtime=0,
        ) as compressed:
            with io.TextIOWrapper(compressed, encoding="utf-8") as output:
                for source_row, row in enumerate(_documents(source)):
                    counts["source_rows"] += 1
                    reason = rejection_reason(row)
                    if reason is not None:
                        counts[f"rejected_{reason}"] += 1
                        continue
                    raw = str(row["SIMPLE_ADDRESS"]).strip()
                    normalized = fold(raw)
                    if normalized in seen:
                        counts["duplicate_rows_removed"] += 1
                        continue
                    seen.add(normalized)
                    group_id, split = group_id_and_split(row)
                    tier = quality_tier(row)
                    record = {
                        "source_row": source_row,
                        "group_id": group_id,
                        "split": split,
                        "tier": tier,
                        "raw": raw,
                        "legal_address": str(row["ADDRESS"]).strip(),
                        "expected": expected_components(row),
                        "fias_id": str(row["N_FIAS"]).lower(),
                        "unom": row.get("UNOM"),
                        "registry_id": row.get("NREG"),
                        "object_type": row.get("OBJ_TYPE"),
                    }
                    output.write(
                        json.dumps(
                            record,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    )
                    output.write("\n")
                    counts["unique_usable_rows"] += 1
                    counts[f"split_{split}"] += 1
                    counts[f"tier_{tier}"] += 1
                    counts[f"object_{row.get('OBJ_TYPE')}"] += 1
    if counts["source_rows"] != SOURCE_ROWS:
        temporary.unlink(missing_ok=True)
        raise ValueError(
            f"unexpected source row count: expected {SOURCE_ROWS}, "
            f"got {counts['source_rows']}"
        )
    temporary.replace(filtered)

    manifest = {
        "format_version": 1,
        "source": {
            "title": (
                "Адресный реестр объектов недвижимости города Москвы"
            ),
            "publisher": (
                "Департамент городского имущества города Москвы"
            ),
            "original_portal": "https://data.mos.ru",
            "dataset_id": DATASET_ID,
            "version": DATASET_VERSION,
            "release_date": DATASET_DATE,
            "mirror": (
                "https://data2.apicrafter.ru/packages/"
                "datamos-addressreestr"
            ),
            "archive_url": ARCHIVE_URL,
            "archive_bytes": source.stat().st_size,
            "archive_sha256": actual_archive_sha256,
            "inner_data_sha256": actual_inner_sha256,
            "source_rows": SOURCE_ROWS,
            "embedded_terms": (
                "Типовые условия доступа к открытым данным органов власти в РФ"
            ),
            "mirror_terms": "CC-BY-SA",
        },
        "policy": {
            "purpose": (
                "Moscow-only official clean-address training/evaluation corpus; "
                "not bundled in the runtime package"
            ),
            "filter": [
                "OnTerritoryOfMoscow == да",
                "ADR_TYPE == Официальный",
                "SOSTAD == Зарегистрирован в АР",
                "STATUS == Внесён в ГКН",
                "non-empty SIMPLE_ADDRESS, P7 street, and L1_VALUE house",
                "valid N_FIAS UUID",
            ],
            "deduplication": (
                "first case-folded, ё/е-folded, whitespace-normalized "
                "SIMPLE_ADDRESS"
            ),
            "grouping": (
                "street/house/corpus/structure identity; SHA-256 groups are "
                "assigned 90% train, 5% validation, 5% test"
            ),
        },
        "counts": dict(sorted(counts.items())),
        "artifact": {
            "filename": filtered.name,
            "rows": counts["unique_usable_rows"],
            "bytes": filtered.stat().st_size,
            "sha256": _sha256(filtered),
        },
        "limitations": [
            "The snapshot is from October 2021 and is not a current registry.",
            "The corpus is Moscow-only and consists of clean legal formatting.",
            (
                "Only street, house, corpus, and structure are scored from "
                "SIMPLE_ADDRESS; administrative fields are intentionally out "
                "of scope for this view."
            ),
            (
                "The archive is obtained from an attributed mirror; retain its "
                "embedded metadata and confirm redistribution terms before "
                "publishing derived rows."
            ),
        ],
    }
    rendered = f"{json.dumps(manifest, ensure_ascii=False, indent=2)}\n"
    temporary_manifest = manifest_path.with_suffix(
        f"{manifest_path.suffix}.part"
    )
    temporary_manifest.write_text(rendered, encoding="utf-8")
    temporary_manifest.replace(manifest_path)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--filtered", type=Path, default=DEFAULT_FILTERED)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--download", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args(argv)
    if args.download:
        download_source(args.source)
    if not args.source.exists():
        parser.error(
            f"{args.source} does not exist; provide --source or use --download"
        )
    try:
        manifest = prepare(
            args.source,
            args.filtered,
            args.manifest,
            overwrite=args.overwrite,
        )
    except (FileExistsError, ValueError) as error:
        parser.error(str(error))
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
