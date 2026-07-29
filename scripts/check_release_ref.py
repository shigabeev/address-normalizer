#!/usr/bin/env python3
"""Require a release workflow to run from the immutable matching version tag."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).resolve().parents[1]
RELEASE_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:(?:a|b|rc)[0-9]+)?$")


def fail(message: str) -> None:
    raise SystemExit(f"release ref check failed: {message}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    args = parser.parse_args()

    if RELEASE_VERSION.fullmatch(args.version) is None:
        fail(f"{args.version!r} is not an allowed alpha/beta/rc/stable version")
    expected_tag = f"v{args.version}"
    ref_type = os.environ.get("GITHUB_REF_TYPE")
    ref_name = os.environ.get("GITHUB_REF_NAME")
    revision = os.environ.get("GITHUB_SHA")
    if ref_type != "tag" or ref_name != expected_tag:
        fail(
            f"workflow must be dispatched from tag {expected_tag!r}; "
            f"received ref_type={ref_type!r}, ref_name={ref_name!r}"
        )
    if not revision:
        fail("GITHUB_SHA is missing")

    tagged_commit = subprocess.run(
        ["git", "rev-parse", f"refs/tags/{expected_tag}^{{commit}}"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    checked_out_commit = subprocess.run(
        ["git", "rev-parse", "HEAD^{commit}"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if tagged_commit != revision or checked_out_commit != revision:
        fail(
            f"tag, GITHUB_SHA, and checkout differ: "
            f"tag={tagged_commit}, github={revision}, checkout={checked_out_commit}"
        )
    print(f"release ref: OK ({expected_tag} -> {revision})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
