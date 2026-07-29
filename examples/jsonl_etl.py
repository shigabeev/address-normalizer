"""Convert newline-delimited raw addresses to newline-delimited parse results."""

from __future__ import annotations

import json
import sys

from address_normalizer import parse_iter


def main() -> None:
    addresses = (line.rstrip("\r\n") for line in sys.stdin)
    for source_line, result in enumerate(parse_iter(addresses), start=1):
        record = {
            "source_line": source_line,
            "address": result.raw,
            "parsed_address": result.as_dict(),
        }
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
