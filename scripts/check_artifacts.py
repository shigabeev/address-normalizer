#!/usr/bin/env python3
"""Inspect built distributions and write a small provenance manifest."""

from __future__ import annotations

import argparse
import base64
import csv
from email.parser import BytesParser
from hashlib import sha256
import io
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import subprocess
import tarfile
import tomllib
import zipfile


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = ROOT / "src" / "address_normalizer"
MODEL_PATH = PACKAGE_ROOT / "data" / "model.json"
WHEEL_MAX_BYTES = 256 * 1024
SDIST_MAX_BYTES = 256 * 1024
MODEL_MAX_BYTES = 64 * 1024

FORBIDDEN_PARTS = {
    ".cache",
    ".git",
    ".github",
    ".ipynb_checkpoints",
    "__pycache__",
    "evaluation",
    "ref",
    "tests",
    "tests_v2",
    "training",
}
FORBIDDEN_SUFFIXES = {
    ".ipynb",
    ".jsonl",
    ".pem",
    ".pyc",
    ".pyo",
    ".sqlite",
    ".xlsx",
}


def fail(message: str) -> None:
    raise SystemExit(f"artifact check failed: {message}")


def package_version() -> str:
    init_text = (PACKAGE_ROOT / "__init__.py").read_text(encoding="utf-8")
    match = re.search(
        r'^__version__\s*=\s*["\'](?P<version>[^"\']+)["\']\s*$',
        init_text,
        flags=re.MULTILINE,
    )
    if match is None:
        fail("address_normalizer.__version__ must be a string literal")
    return match.group("version")


def expected_package_files() -> set[str]:
    expected: set[str] = set()
    for path in PACKAGE_ROOT.rglob("*"):
        if not path.is_file() or "__pycache__" in path.parts:
            continue
        relative = path.relative_to(PACKAGE_ROOT).as_posix()
        if path.suffix == ".py" or relative in {"py.typed", "data/model.json"}:
            expected.add(f"address_normalizer/{relative}")
            continue
        fail(f"unexpected source-package file {relative}")
    return expected


def project_metadata() -> dict[str, object]:
    return tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))[
        "project"
    ]


def configured_license_files() -> set[str]:
    patterns = project_metadata().get("license-files", [])
    if not isinstance(patterns, list):
        fail("project.license-files must be a list when present")
    return {
        path.relative_to(ROOT).as_posix()
        for pattern in patterns
        if isinstance(pattern, str)
        for path in ROOT.glob(pattern)
        if path.is_file()
    }


def expected_sdist_documentation() -> set[str]:
    expected = {
        "CHANGELOG.md",
        "CONTRIBUTING.md",
        "LICENSING.md",
        "MANIFEST.in",
        "README.md",
        "SECURITY.md",
        "SUPPORT.md",
        "pyproject.toml",
    }
    allowed_suffixes = {"docs": {".md"}, "examples": {".md", ".py"}}
    for directory, suffixes in allowed_suffixes.items():
        for path in (ROOT / directory).rglob("*"):
            if (
                not path.is_file()
                or "__pycache__" in path.parts
                or path.suffix in {".pyc", ".pyo"}
            ):
                continue
            if path.suffix not in suffixes:
                fail(f"unexpected {directory} artifact {path.relative_to(ROOT)}")
            expected.add(path.relative_to(ROOT).as_posix())
    return expected


def safe_archive_name(name: str) -> PurePosixPath:
    if "\\" in name:
        fail(f"archive member uses a backslash: {name!r}")
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts:
        fail(f"unsafe archive member: {name!r}")
    return path


def forbidden_member(name: str) -> bool:
    path = safe_archive_name(name)
    lower_parts = {part.lower() for part in path.parts}
    return bool(lower_parts & FORBIDDEN_PARTS) or path.suffix.lower() in FORBIDDEN_SUFFIXES


def verify_record(archive: zipfile.ZipFile, record_name: str) -> None:
    rows = list(csv.reader(io.StringIO(archive.read(record_name).decode("utf-8"))))
    recorded = {row[0]: row[1:] for row in rows}
    if set(recorded) != set(archive.namelist()):
        fail("wheel RECORD does not enumerate every archive member exactly once")

    for name in archive.namelist():
        digest, size = recorded[name]
        if name == record_name:
            if digest or size:
                fail("wheel RECORD must not hash itself")
            continue
        if not digest.startswith("sha256="):
            fail(f"wheel RECORD lacks a SHA-256 digest for {name}")
        expected_digest = digest.removeprefix("sha256=")
        actual_digest = base64.urlsafe_b64encode(
            sha256(archive.read(name)).digest()
        ).rstrip(b"=").decode("ascii")
        if expected_digest != actual_digest:
            fail(f"wheel RECORD digest mismatch for {name}")
        if size != str(len(archive.read(name))):
            fail(f"wheel RECORD size mismatch for {name}")


def check_wheel(path: Path, expected_version: str) -> dict[str, object]:
    if path.stat().st_size > WHEEL_MAX_BYTES:
        fail(f"wheel is {path.stat().st_size} bytes; limit is {WHEEL_MAX_BYTES}")

    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)):
            fail("wheel contains duplicate member names")
        for name in names:
            safe_archive_name(name)
            if forbidden_member(name):
                fail(f"wheel contains forbidden member {name}")

        expected_files = expected_package_files()
        package_files = {name for name in names if name.startswith("address_normalizer/")}
        if package_files != expected_files:
            missing = sorted(expected_files - package_files)
            unexpected = sorted(package_files - expected_files)
            fail(f"wheel package files differ; missing={missing}, unexpected={unexpected}")

        metadata_names = [name for name in names if name.endswith(".dist-info/METADATA")]
        record_names = [name for name in names if name.endswith(".dist-info/RECORD")]
        wheel_names = [name for name in names if name.endswith(".dist-info/WHEEL")]
        if len(metadata_names) != 1 or len(record_names) != 1 or len(wheel_names) != 1:
            fail("wheel must contain one METADATA, WHEEL, and RECORD file")

        metadata = BytesParser().parsebytes(archive.read(metadata_names[0]))
        if metadata["Name"] != "address-normalizer":
            fail(f"unexpected distribution name {metadata['Name']!r}")
        if metadata["Version"] != expected_version:
            fail(
                f"metadata version {metadata['Version']!r} does not match "
                f"source version {expected_version!r}"
            )
        if metadata.get_all("Requires-Dist"):
            fail(f"runtime dependencies found: {metadata.get_all('Requires-Dist')}")
        if metadata["Requires-Python"] != ">=3.10":
            fail(f"unexpected Requires-Python value {metadata['Requires-Python']!r}")
        dist_info = metadata_names[0].rsplit("/", 1)[0]
        expected_dist_info = f"address_normalizer-{expected_version}.dist-info"
        if dist_info != expected_dist_info:
            fail(f"unexpected dist-info directory {dist_info!r}")

        project = project_metadata()
        expected_license = project.get("license")
        actual_license = metadata["License-Expression"]
        if expected_license != actual_license:
            fail(
                f"wheel license expression {actual_license!r} does not match "
                f"pyproject value {expected_license!r}"
            )
        license_files = configured_license_files()
        metadata_license_files = set(metadata.get_all("License-File", []))
        if metadata_license_files != license_files:
            fail(
                f"wheel License-File metadata differs; expected={sorted(license_files)}, "
                f"actual={sorted(metadata_license_files)}"
            )

        allowed_dist_info = {
            f"{dist_info}/METADATA",
            f"{dist_info}/RECORD",
            f"{dist_info}/WHEEL",
            f"{dist_info}/entry_points.txt",
            f"{dist_info}/top_level.txt",
            *(f"{dist_info}/licenses/{name}" for name in license_files),
        }
        allowed_names = expected_files | allowed_dist_info
        if set(names) != allowed_names:
            missing = sorted(allowed_names - set(names))
            unexpected = sorted(set(names) - allowed_names)
            fail(f"wheel members differ; missing={missing}, unexpected={unexpected}")

        wheel_metadata = archive.read(wheel_names[0]).decode("utf-8")
        if "Tag: py3-none-any" not in wheel_metadata:
            fail("wheel is not tagged as platform-independent py3-none-any")

        model_bytes = archive.read("address_normalizer/data/model.json")
        if model_bytes != MODEL_PATH.read_bytes():
            fail("wheel model differs from the source model")
        if len(model_bytes) > MODEL_MAX_BYTES:
            fail(f"model is {len(model_bytes)} bytes; limit is {MODEL_MAX_BYTES}")
        try:
            json.loads(model_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            fail(f"bundled model is not valid UTF-8 JSON: {error}")

        verify_record(archive, record_names[0])

    return {
        "file": path.name,
        "sha256": sha256(path.read_bytes()).hexdigest(),
        "size": path.stat().st_size,
    }


def check_sdist(path: Path, expected_version: str) -> dict[str, object]:
    if path.stat().st_size > SDIST_MAX_BYTES:
        fail(f"sdist is {path.stat().st_size} bytes; limit is {SDIST_MAX_BYTES}")

    expected_root = f"address_normalizer-{expected_version}"
    with tarfile.open(path, mode="r:gz") as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        if len(names) != len(set(names)):
            fail("sdist contains duplicate member names")
        for member in members:
            member_path = safe_archive_name(member.name)
            if not member_path.parts or member_path.parts[0] != expected_root:
                fail(f"sdist member is outside {expected_root}: {member.name}")
            if member.issym() or member.islnk() or member.isdev():
                fail(f"sdist contains a link or device: {member.name}")
            if forbidden_member("/".join(member_path.parts[1:])):
                fail(f"sdist contains forbidden member {member.name}")

        relative_files = {
            "/".join(safe_archive_name(member.name).parts[1:])
            for member in members
            if member.isfile()
        }
        expected_files = {
            *expected_sdist_documentation(),
            "PKG-INFO",
            *(f"src/{name}" for name in expected_package_files()),
            "setup.cfg",
            "src/address_normalizer.egg-info/PKG-INFO",
            "src/address_normalizer.egg-info/SOURCES.txt",
            "src/address_normalizer.egg-info/dependency_links.txt",
            "src/address_normalizer.egg-info/entry_points.txt",
            "src/address_normalizer.egg-info/top_level.txt",
            *configured_license_files(),
        }
        if relative_files != expected_files:
            missing = sorted(expected_files - relative_files)
            unexpected = sorted(relative_files - expected_files)
            fail(f"sdist files differ; missing={missing}, unexpected={unexpected}")

        expected_directories = {""}
        for name in expected_files:
            parent = PurePosixPath(name).parent
            while str(parent) != ".":
                expected_directories.add(parent.as_posix())
                parent = parent.parent
        relative_directories = {
            "/".join(safe_archive_name(member.name).parts[1:])
            for member in members
            if member.isdir()
        }
        if relative_directories != expected_directories:
            missing = sorted(expected_directories - relative_directories)
            unexpected = sorted(relative_directories - expected_directories)
            fail(f"sdist directories differ; missing={missing}, unexpected={unexpected}")

        model_member = archive.extractfile(
            f"{expected_root}/src/address_normalizer/data/model.json"
        )
        if model_member is None or model_member.read() != MODEL_PATH.read_bytes():
            fail("sdist model differs from the source model")

    return {
        "file": path.name,
        "sha256": sha256(path.read_bytes()).hexdigest(),
        "size": path.stat().st_size,
    }


def git_details() -> tuple[str | None, bool | None]:
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None, None
    return revision, bool(status)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dist", type=Path, help="directory containing one wheel and one sdist")
    parser.add_argument(
        "--expected-version",
        help="fail unless built metadata matches this version as well as the source",
    )
    parser.add_argument("--write-manifest", type=Path)
    parser.add_argument(
        "--verify-manifest",
        type=Path,
        help="verify artifacts and source provenance exactly match a prior manifest",
    )
    args = parser.parse_args()

    wheels = sorted(args.dist.glob("*.whl"))
    sdists = sorted(args.dist.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sdists) != 1:
        fail(
            f"expected exactly one wheel and one .tar.gz in {args.dist}; "
            f"found {len(wheels)} wheel(s) and {len(sdists)} sdist(s)"
        )

    version = package_version()
    if args.expected_version is not None and args.expected_version != version:
        fail(
            f"requested version {args.expected_version!r} does not match "
            f"source version {version!r}"
        )

    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    if pyproject["project"].get("dependencies") != []:
        fail("pyproject runtime dependencies must remain an explicit empty list")
    if pyproject["build-system"]["requires"] != ["setuptools==80.9.0"]:
        fail("build backend must stay exactly pinned for reproducible builds")

    artifacts = [
        check_wheel(wheels[0], version),
        check_sdist(sdists[0], version),
    ]
    revision, dirty = git_details()
    manifest = {
        "schema_version": 1,
        "distribution": "address-normalizer",
        "version": version,
        "source_revision": os.environ.get("GITHUB_SHA", revision),
        "source_tree_dirty": dirty,
        "build_python": platform.python_version(),
        "build_backend": "setuptools==80.9.0",
        "runtime_dependencies": [],
        "model": {
            "file": "src/address_normalizer/data/model.json",
            "sha256": sha256(MODEL_PATH.read_bytes()).hexdigest(),
            "size": MODEL_PATH.stat().st_size,
        },
        "artifacts": artifacts,
    }
    if args.verify_manifest:
        expected_manifest = json.loads(args.verify_manifest.read_text(encoding="utf-8"))
        if expected_manifest != manifest:
            fail(
                f"current artifact provenance does not match {args.verify_manifest}; "
                f"expected={json.dumps(expected_manifest, sort_keys=True)}, "
                f"actual={json.dumps(manifest, sort_keys=True)}"
            )
    if args.write_manifest:
        args.write_manifest.parent.mkdir(parents=True, exist_ok=True)
        args.write_manifest.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    for artifact in artifacts:
        print(f"{artifact['file']}: {artifact['size']} bytes sha256={artifact['sha256']}")
    print(f"model.json: {MODEL_PATH.stat().st_size} bytes")
    print("artifact policy: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
