"""Parse one address and make review routing explicit."""

from __future__ import annotations

import argparse
import json

from address_normalizer import ParsedAddress, parse


def needs_review(result: ParsedAddress) -> bool:
    """Use structural evidence, not an uncalibrated global score threshold."""

    required_fields_missing = result.street is None or result.house_num is None
    unresolved_evidence = bool(
        result.warnings or result.alternatives or result.unparsed
    )
    return required_fields_missing or unresolved_evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("address", nargs="?", default="Ополченская 5-30")
    args = parser.parse_args()

    result = parse(args.address)
    print(json.dumps(result.as_dict(), ensure_ascii=False, indent=2))
    print(f"needs_review={str(needs_review(result)).lower()}")


if __name__ == "__main__":
    main()
