#!/usr/bin/env python3
"""Smoke-test an installed wheel without source-tree or network access."""

from __future__ import annotations

from contextlib import redirect_stdout
import importlib.metadata
import io
import json
from pathlib import Path
import sys


def deny_network(event: str, _args: tuple[object, ...]) -> None:
    if event.startswith(("socket.", "urllib.", "http.client.")):
        raise RuntimeError(f"unexpected network operation: {event}")


def main() -> int:
    sys.addaudithook(deny_network)

    import address_normalizer
    from address_normalizer import parse, parse_many
    from address_normalizer.cli import main as cli_main

    package_path = Path(address_normalizer.__file__).resolve()
    if "site-packages" not in package_path.parts:
        raise AssertionError(f"not importing an installed wheel: {package_path}")
    if importlib.metadata.requires("address-normalizer"):
        raise AssertionError("the installed distribution has runtime dependencies")
    if importlib.metadata.version("address-normalizer") != address_normalizer.__version__:
        raise AssertionError("runtime and distribution versions differ")

    result = parse("г. Москва, ул. Тверская, д.4, кв.12")
    assert result.house_num is not None and result.house_num.value == "4"
    assert len(parse_many(["Ополченская 5-30", "Невский проспект 10"])) == 2

    single_output = io.StringIO()
    with redirect_stdout(single_output):
        assert cli_main(["Москва", "Тверская", "1"]) == 0
    json.loads(single_output.getvalue())

    old_stdin = sys.stdin
    batch_output = io.StringIO()
    try:
        sys.stdin = io.StringIO("Ополченская 5-30\nНевский проспект 10\n")
        with redirect_stdout(batch_output):
            assert cli_main(["--jsonl"]) == 0
    finally:
        sys.stdin = old_stdin
    lines = batch_output.getvalue().splitlines()
    assert len(lines) == 2
    for line in lines:
        json.loads(line)

    print(f"installed smoke: OK ({package_path})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
