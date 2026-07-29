#!/usr/bin/env python3
"""Focused negative tests for distribution allowlists."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import tarfile
import tempfile
import zipfile

import check_artifacts


def expect_failure(action: object, expected_text: str) -> None:
    try:
        action()  # type: ignore[operator]
    except SystemExit as error:
        if expected_text not in str(error):
            raise AssertionError(f"unexpected checker failure: {error}") from error
    else:
        raise AssertionError("malicious archive unexpectedly passed")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dist", type=Path)
    args = parser.parse_args()
    wheel = next(args.dist.glob("*.whl"))
    sdist = next(args.dist.glob("*.tar.gz"))
    version = check_artifacts.package_version()

    with tempfile.TemporaryDirectory(prefix="artifact-policy-test-") as temp_dir:
        temp = Path(temp_dir)
        bad_wheel = temp / wheel.name
        shutil.copyfile(wheel, bad_wheel)
        with zipfile.ZipFile(bad_wheel, mode="a") as archive:
            archive.writestr("payload.sh", "#!/bin/sh\n")
        expect_failure(
            lambda: check_artifacts.check_wheel(bad_wheel, version),
            "wheel members differ",
        )

        bad_sdist = temp / sdist.name
        expected_root = f"address_normalizer-{version}"
        with tarfile.open(sdist, mode="r:gz") as source:
            with tarfile.open(bad_sdist, mode="w:gz") as target:
                for member in source.getmembers():
                    target.addfile(member, source.extractfile(member) if member.isfile() else None)
                payload = b"raise RuntimeError('unexpected source payload')\n"
                member = tarfile.TarInfo(f"{expected_root}/src/evil.py")
                member.size = len(payload)
                import io

                target.addfile(member, io.BytesIO(payload))
        expect_failure(
            lambda: check_artifacts.check_sdist(bad_sdist, version),
            "sdist files differ",
        )

    print("artifact policy negative tests: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
