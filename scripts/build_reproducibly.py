#!/usr/bin/env python3
"""Build twice with a fixed epoch and keep only byte-identical artifacts."""

from __future__ import annotations

import argparse
import copy
import gzip
from hashlib import sha256
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile


DEFAULT_SOURCE_DATE_EPOCH = "1704067200"  # 2024-01-01T00:00:00Z


def canonicalize_sdist(path: Path, epoch: int) -> None:
    """Remove ambient filesystem and gzip metadata from a setuptools sdist."""
    temporary = path.with_name(f".{path.name}.canonical")
    try:
        with tarfile.open(path, mode="r:gz") as source:
            members = source.getmembers()
            with temporary.open("wb") as raw_output:
                with gzip.GzipFile(
                    filename="",
                    mode="wb",
                    fileobj=raw_output,
                    compresslevel=9,
                    mtime=epoch,
                ) as compressed:
                    with tarfile.open(
                        fileobj=compressed,
                        mode="w",
                        format=tarfile.PAX_FORMAT,
                    ) as target:
                        for original in sorted(members, key=lambda member: member.name):
                            member = copy.copy(original)
                            member.uid = 0
                            member.gid = 0
                            member.uname = ""
                            member.gname = ""
                            member.mtime = epoch
                            member.mode = 0o755 if member.isdir() else 0o644
                            member.pax_headers = {}
                            file_data = source.extractfile(original) if original.isfile() else None
                            target.addfile(member, file_data)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def artifact_bytes(directory: Path) -> dict[str, bytes]:
    artifacts = {
        path.name: path.read_bytes()
        for pattern in ("*.whl", "*.tar.gz")
        for path in directory.glob(pattern)
    }
    if len([name for name in artifacts if name.endswith(".whl")]) != 1:
        raise SystemExit("reproducible build failed: expected exactly one wheel")
    if len([name for name in artifacts if name.endswith(".tar.gz")]) != 1:
        raise SystemExit("reproducible build failed: expected exactly one sdist")
    return artifacts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("dist"))
    parser.add_argument(
        "--source-date-epoch",
        default=os.environ.get("SOURCE_DATE_EPOCH", DEFAULT_SOURCE_DATE_EPOCH),
    )
    args = parser.parse_args()
    if not args.source_date_epoch.isdigit():
        raise SystemExit("reproducible build failed: SOURCE_DATE_EPOCH must be an integer")
    if args.output.exists() and any(args.output.iterdir()):
        raise SystemExit(f"reproducible build failed: output is not empty: {args.output}")

    environment = os.environ.copy()
    environment["SOURCE_DATE_EPOCH"] = args.source_date_epoch
    with tempfile.TemporaryDirectory(prefix="address-normalizer-build-a-") as first_dir:
        with tempfile.TemporaryDirectory(prefix="address-normalizer-build-b-") as second_dir:
            first = Path(first_dir)
            second = Path(second_dir)
            for output in (first, second):
                subprocess.run(
                    [sys.executable, "-m", "build", "--outdir", str(output)],
                    check=True,
                    env=environment,
                )
                sdist = next(output.glob("*.tar.gz"))
                canonicalize_sdist(sdist, int(args.source_date_epoch))
            first_artifacts = artifact_bytes(first)
            second_artifacts = artifact_bytes(second)
            if first_artifacts != second_artifacts:
                details = []
                for name in sorted(first_artifacts.keys() | second_artifacts.keys()):
                    first_hash = (
                        sha256(first_artifacts[name]).hexdigest()
                        if name in first_artifacts
                        else "missing"
                    )
                    second_hash = (
                        sha256(second_artifacts[name]).hexdigest()
                        if name in second_artifacts
                        else "missing"
                    )
                    details.append(f"{name}: first={first_hash}, second={second_hash}")
                raise SystemExit(
                    "reproducible build failed: artifacts are not byte-identical; "
                    + "; ".join(details)
                )

            args.output.mkdir(parents=True, exist_ok=True)
            for name, content in first_artifacts.items():
                target = args.output / name
                target.write_bytes(content)
                print(f"{name}: {len(content)} bytes sha256={sha256(content).hexdigest()}")

    print(f"reproducible build: OK (SOURCE_DATE_EPOCH={args.source_date_epoch})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
