"""Prepare the full Deepparse Russian shard for training and evaluation."""

from __future__ import annotations

import argparse
from collections import Counter
import gzip
import hashlib
import heapq
import io
import json
from pathlib import Path
import sqlite3
import sys
from typing import Any, Iterable
from urllib.request import Request, urlopen

from deepparse_data import (
    DATASET_REVISION,
    DATASET_ROWS,
    DATASET_SHA256,
    DATASET_URL,
    DEFAULT_FILTERED,
    DEFAULT_MANIFEST,
    DEFAULT_SAMPLE,
    DEFAULT_SOURCE,
    EXPECTED_FIELDS,
    SOURCE_TAGS,
    USEFUL_TAGS,
    expected_components,
    group_id_and_split,
    mapped_labels,
    normalized_address_id,
    quality_tier,
    sample_rank,
)


BATCH_SIZE = 131_072
WRITE_BATCH_SIZE = 65_536


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_source(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.part")
    request = Request(DATASET_URL, headers={"User-Agent": "address-normalizer/2"})
    with urlopen(request, timeout=120) as response, temporary.open("wb") as output:
        while chunk := response.read(1024 * 1024):
            output.write(chunk)
    actual = _sha256(temporary)
    if actual != DATASET_SHA256:
        temporary.unlink(missing_ok=True)
        raise ValueError(
            f"source checksum mismatch: expected {DATASET_SHA256}, got {actual}"
        )
    temporary.replace(path)


def _require_pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as error:
        raise SystemExit(
            "PyArrow is required only for dataset preparation. Install "
            "`requirements-evaluation.txt` in a separate environment."
        ) from error
    return pa, pq


def _schema(pa: Any) -> Any:
    expected = pa.struct([(field, pa.string()) for field in EXPECTED_FIELDS])
    return pa.schema(
        [
            ("source_row", pa.int64()),
            ("example_id", pa.binary(16)),
            ("group_id", pa.binary(16)),
            ("split", pa.string()),
            ("tier", pa.string()),
            ("language", pa.string()),
            ("raw", pa.string()),
            ("tokens", pa.list_(pa.string())),
            ("source_tags", pa.list_(pa.string())),
            ("labels", pa.list_(pa.string())),
            ("expected", expected),
        ]
    )


def _source_batches(parquet_file: Any) -> Iterable[tuple[list[Any], ...]]:
    for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
        yield tuple(column.to_pylist() for column in batch.columns)


def _validate_source(parquet_file: Any) -> None:
    required = {"Address", "Tags", "Language"}
    actual = set(parquet_file.schema_arrow.names)
    if actual != required:
        raise ValueError(
            f"unexpected source columns: expected {sorted(required)}, "
            f"got {sorted(actual)}"
        )
    if parquet_file.metadata.num_rows != DATASET_ROWS:
        raise ValueError(
            f"unexpected row count: expected {DATASET_ROWS}, "
            f"got {parquet_file.metadata.num_rows}"
        )


def _open_dedupe_database(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=OFF")
    connection.execute("PRAGMA synchronous=OFF")
    connection.execute("PRAGMA temp_store=MEMORY")
    connection.execute("PRAGMA locking_mode=EXCLUSIVE")
    connection.execute(
        "CREATE TABLE seen (example_id BLOB PRIMARY KEY, source_row INTEGER) "
        "WITHOUT ROWID"
    )
    return connection


def _mark_accepted_rows(
    parquet_file: Any,
    database: sqlite3.Connection,
) -> tuple[bytearray, dict[str, int]]:
    stats: Counter[str] = Counter()
    source_row = 0
    for addresses, tag_lists, languages in _source_batches(parquet_file):
        candidates: list[tuple[bytes, int]] = []
        for address, tags, language in zip(addresses, tag_lists, languages):
            stats["source_rows"] += 1
            tokens = address.split() if isinstance(address, str) else []
            if language != "rus":
                stats["rejected_language"] += 1
            elif not tokens:
                stats["rejected_empty"] += 1
            elif len(tokens) != len(tags):
                stats["rejected_token_tag_mismatch"] += 1
            elif len(tokens) > 64:
                stats["rejected_too_long"] += 1
            elif not set(tags) <= SOURCE_TAGS:
                stats["rejected_unknown_tag"] += 1
            elif not set(tags) & USEFUL_TAGS:
                stats["rejected_administrative_only"] += 1
            else:
                stats["eligible_rows"] += 1
                candidates.append((normalized_address_id(address), source_row))
            source_row += 1
        database.executemany(
            "INSERT OR IGNORE INTO seen(example_id, source_row) VALUES (?, ?)",
            candidates,
        )
        database.commit()

    unique_rows = int(database.execute("SELECT count(*) FROM seen").fetchone()[0])
    stats["unique_usable_rows"] = unique_rows
    stats["duplicate_rows_removed"] = stats["eligible_rows"] - unique_rows
    accepted = bytearray((stats["source_rows"] + 7) // 8)
    for (row_number,) in database.execute("SELECT source_row FROM seen"):
        accepted[row_number >> 3] |= 1 << (row_number & 7)
    return accepted, dict(stats)


def _is_accepted(accepted: bytearray, source_row: int) -> bool:
    return bool(accepted[source_row >> 3] & (1 << (source_row & 7)))


def _write_records(
    writer: Any,
    pa: Any,
    schema: Any,
    records: list[dict[str, Any]],
) -> None:
    if records:
        writer.write_table(pa.Table.from_pylist(records, schema=schema))
        records.clear()


def _write_filtered(
    parquet_file: Any,
    accepted: bytearray,
    output: Path,
    sample_size: int,
    pa: Any,
    pq: Any,
) -> tuple[Counter[tuple[str, str]], set[int]]:
    schema = _schema(pa)
    temporary = output.with_suffix(f"{output.suffix}.part")
    writer = pq.ParquetWriter(
        temporary,
        schema,
        compression="zstd",
        compression_level=6,
        use_dictionary=["split", "tier", "language", "source_tags", "labels"],
        write_statistics=True,
    )
    records: list[dict[str, Any]] = []
    sample_heap: list[tuple[int, int]] = []
    counts: Counter[tuple[str, str]] = Counter()
    source_row = 0
    try:
        for addresses, tag_lists, languages in _source_batches(parquet_file):
            for address, tags, language in zip(
                addresses, tag_lists, languages
            ):
                if not _is_accepted(accepted, source_row):
                    source_row += 1
                    continue
                tokens = address.split()
                labels = mapped_labels(tags)
                example_id = normalized_address_id(address)
                group_id, split = group_id_and_split(tokens, tags)
                tier = quality_tier(tags)
                records.append(
                    {
                        "source_row": source_row,
                        "example_id": example_id,
                        "group_id": group_id,
                        "split": split,
                        "tier": tier,
                        "language": language,
                        "raw": address,
                        "tokens": tokens,
                        "source_tags": tags,
                        "labels": labels,
                        "expected": expected_components(tokens, labels),
                    }
                )
                counts[(split, tier)] += 1
                if split == "test" and sample_size:
                    rank = sample_rank(example_id)
                    candidate = (-rank, source_row)
                    if len(sample_heap) < sample_size:
                        heapq.heappush(sample_heap, candidate)
                    elif candidate > sample_heap[0]:
                        heapq.heapreplace(sample_heap, candidate)
                if len(records) >= WRITE_BATCH_SIZE:
                    _write_records(writer, pa, schema, records)
                source_row += 1
        _write_records(writer, pa, schema, records)
    except BaseException:
        writer.close()
        temporary.unlink(missing_ok=True)
        raise
    writer.close()
    temporary.replace(output)
    return counts, {source_row for _, source_row in sample_heap}


def _write_sample(
    filtered: Path,
    output: Path,
    selected_rows: set[int],
    pq: Any,
) -> int:
    temporary = output.with_suffix(f"{output.suffix}.part")
    written = 0
    with temporary.open("wb") as raw_output:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            fileobj=raw_output,
            mtime=0,
        ) as compressed:
            with io.TextIOWrapper(compressed, encoding="utf-8") as text:
                parquet_file = pq.ParquetFile(filtered)
                for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE):
                    for row in batch.to_pylist():
                        if row["source_row"] not in selected_rows:
                            continue
                        row["example_id"] = row["example_id"].hex()
                        row["group_id"] = row["group_id"].hex()
                        text.write(
                            json.dumps(
                                row,
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                        )
                        text.write("\n")
                        written += 1
    temporary.replace(output)
    return written


def prepare(
    source: Path,
    filtered: Path,
    sample: Path,
    manifest_path: Path,
    *,
    sample_size: int,
    overwrite: bool,
) -> dict[str, Any]:
    if sample_size < 0:
        raise ValueError("sample_size must be non-negative")
    for path in (filtered, sample, manifest_path):
        if path.exists() and not overwrite:
            raise FileExistsError(f"{path} already exists; pass --overwrite")
    actual_sha256 = _sha256(source)
    if actual_sha256 != DATASET_SHA256:
        raise ValueError(
            f"source checksum mismatch: expected {DATASET_SHA256}, "
            f"got {actual_sha256}"
        )

    pa, pq = _require_pyarrow()
    parquet_file = pq.ParquetFile(source)
    _validate_source(parquet_file)
    filtered.parent.mkdir(parents=True, exist_ok=True)
    sample.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    database_path = filtered.with_suffix(".dedupe.sqlite3")
    database_path.unlink(missing_ok=True)
    database = _open_dedupe_database(database_path)
    try:
        accepted, filter_counts = _mark_accepted_rows(parquet_file, database)
    finally:
        database.close()

    parquet_file = pq.ParquetFile(source)
    counts, selected_rows = _write_filtered(
        parquet_file,
        accepted,
        filtered,
        sample_size,
        pa,
        pq,
    )
    written_sample_rows = _write_sample(
        filtered,
        sample,
        selected_rows,
        pq,
    )
    if written_sample_rows != min(
        sample_size,
        sum(count for (split, _), count in counts.items() if split == "test"),
    ):
        raise RuntimeError("benchmark sample row count does not match selection")
    database_path.unlink(missing_ok=True)

    split_counts = {
        split: sum(
            count for (candidate, _), count in counts.items()
            if candidate == split
        )
        for split in ("train", "validation", "test")
    }
    tier_counts = {
        tier: sum(
            count for (_, candidate), count in counts.items()
            if candidate == tier
        )
        for tier in (
            "street_house_unit",
            "street_house",
            "street_only",
            "number_or_unit_only",
        )
    }
    manifest = {
        "format_version": 1,
        "source": {
            "repository": "deepparse/worldwide-addresses",
            "configuration": "ru",
            "license": "CC BY 4.0",
            "revision": DATASET_REVISION,
            "url": DATASET_URL,
            "rows": DATASET_ROWS,
            "bytes": source.stat().st_size,
            "sha256": actual_sha256,
        },
        "policy": {
            "purpose": (
                "external clean-address training/evaluation corpus; not bundled "
                "in the runtime package"
            ),
            "filter": (
                "Russian rows with 1-64 whitespace tokens, known tags, matching "
                "token/tag lengths, and at least one StreetName, StreetNumber, "
                "or Unit tag"
            ),
            "deduplication": (
                "first row for each BLAKE2b-128 hash of case-folded, ё/е-folded, "
                "whitespace-normalized address text"
            ),
            "grouping": (
                "SHA-256 of structured province/county/district/municipality/"
                "suburb/street/house identity; unit and presentation fields are "
                "excluded so one building cannot cross splits"
            ),
            "split": "group hash buckets: train 90%, validation 5%, test 5%",
            "sample": (
                "lowest deterministic SHA-256 ranks from the sealed test split"
            ),
            "ignored_labels": ["Country", "Suburb"],
        },
        "filter_counts": filter_counts,
        "split_counts": split_counts,
        "tier_counts": tier_counts,
        "split_tier_counts": {
            f"{split}/{tier}": count
            for (split, tier), count in sorted(counts.items())
        },
        "artifacts": {
            "filtered_parquet": {
                "filename": filtered.name,
                "rows": sum(split_counts.values()),
                "bytes": filtered.stat().st_size,
                "sha256": _sha256(filtered),
            },
            "test_sample_jsonl_gz": {
                "filename": sample.name,
                "rows": written_sample_rows,
                "bytes": sample.stat().st_size,
                "sha256": _sha256(sample),
            },
        },
        "limitations": [
            (
                "The source is curated from open geographic address data and "
                "does not reproduce misspellings or punctuation-heavy user input."
            ),
            (
                "Country and Suburb have no direct public package field and are "
                "kept as source context but mapped to O."
            ),
            (
                "This deterministic test split becomes tuning data after its "
                "failures are used to change the parser; reserve another test "
                "source before making a final production claim."
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
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--filtered", type=Path, default=DEFAULT_FILTERED)
    parser.add_argument("--sample", type=Path, default=DEFAULT_SAMPLE)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--sample-size", type=int, default=100_000)
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
        report = prepare(
            args.source,
            args.filtered,
            args.sample,
            args.manifest,
            sample_size=args.sample_size,
            overwrite=args.overwrite,
        )
    except (FileExistsError, ValueError) as error:
        parser.error(str(error))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
