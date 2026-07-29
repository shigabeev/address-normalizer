"""Command-line interface for JSON and JSONL parsing."""

from __future__ import annotations

import argparse
import json
import sys

from .api import parse


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="address-normalizer",
        description="Parse Russian addresses without an external registry.",
    )
    parser.add_argument("address", nargs="*", help="address text; stdin is used if omitted")
    parser.add_argument(
        "--jsonl",
        action="store_true",
        help="read one address per stdin line and emit one JSON object per line",
    )
    args = parser.parse_args(argv)

    if args.jsonl:
        for line in sys.stdin:
            value = line.rstrip("\r\n")
            print(json.dumps(parse(value).as_dict(), ensure_ascii=False))
        return 0

    value = " ".join(args.address) if args.address else sys.stdin.read().strip()
    print(json.dumps(parse(value).as_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
