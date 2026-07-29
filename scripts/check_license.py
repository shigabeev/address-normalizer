#!/usr/bin/env python3
"""Enforce the recorded license and model-provenance release state."""

from __future__ import annotations

import argparse
from pathlib import Path
import tomllib


ROOT = Path(__file__).resolve().parents[1]
LICENSE_NAMES = ("LICENSE", "LICENSE.txt", "LICENSE.md", "COPYING", "COPYING.txt")
BLOCKER_TEXT = "No license currently applies to this repository"
POLICY_PATH = ROOT / "release-policy.toml"


def fail(message: str) -> None:
    raise SystemExit(f"license gate failed: {message}")


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--expect-blocked", action="store_true")
    mode.add_argument("--require-publishable", action="store_true")
    args = parser.parse_args()

    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))[
        "project"
    ]
    policy = tomllib.loads(POLICY_PATH.read_text(encoding="utf-8"))
    if policy.get("schema_version") != 1:
        fail("release-policy.toml has an unsupported schema")
    publication = policy.get("publication")
    if not isinstance(publication, dict):
        fail("release-policy.toml lacks a [publication] table")
    license_status = publication.get("license_status")
    model_status = publication.get("model_provenance_status")
    license_expression = project.get("license")
    license_patterns = project.get("license-files")
    license_files = [ROOT / name for name in LICENSE_NAMES if (ROOT / name).is_file()]
    blocker = ROOT / "LICENSING.md"
    blocker_is_current = blocker.is_file() and BLOCKER_TEXT in blocker.read_text(
        encoding="utf-8"
    )

    if args.expect_blocked:
        if license_status != "blocked" or model_status != "blocked":
            fail("both release-policy.toml publication statuses must remain blocked")
        if license_expression or license_patterns or license_files:
            fail("license metadata or a license file appeared; update the release policy deliberately")
        if not blocker_is_current:
            fail("LICENSING.md no longer records the known publication blocker")
        print(
            "publication status: BLOCKED by license and compact-model provenance; "
            "package publication must remain disabled"
        )
        return 0

    if license_status != "approved":
        fail("release-policy.toml license_status is not approved")
    if model_status != "approved":
        fail("release-policy.toml model_provenance_status is not approved")
    for key in ("license_evidence", "model_provenance_evidence"):
        evidence = publication.get(key)
        if not isinstance(evidence, list) or not evidence:
            fail(f"release-policy.toml {key} must list recorded evidence")
        for item in evidence:
            if not isinstance(item, str) or not (ROOT / item).is_file():
                fail(f"release-policy.toml {key} references a missing file: {item!r}")
    if not isinstance(license_expression, str) or not license_expression.strip():
        fail("project.license must contain the maintainer-approved SPDX expression")
    if not isinstance(license_patterns, list) or not license_patterns:
        fail("project.license-files must identify the approved license file")
    matched_files = [
        path
        for pattern in license_patterns
        for path in ROOT.glob(pattern)
        if path.is_file()
    ]
    if not matched_files:
        fail("project.license-files does not match a repository file")
    if blocker_is_current:
        fail("LICENSING.md still says that no license applies")
    print(f"license status: publishable ({license_expression})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
