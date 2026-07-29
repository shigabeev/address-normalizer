"""Build or send a request to a customer-managed FIAS/GAR resolver.

The package itself never performs this request. This illustrative contract must
be adapted to the resolver, authentication, and registry version you operate.
"""

from __future__ import annotations

import argparse
import json
from typing import Any
from urllib.request import Request, urlopen

from address_normalizer import ParsedAddress, parse


FIELD_NAMES = (
    "postal_code",
    "region",
    "district",
    "city",
    "settlement",
    "street",
    "street_type",
    "house_num",
    "corpus",
    "structure",
    "apartment",
)


def resolver_payload(result: ParsedAddress) -> dict[str, Any]:
    """Map selected values and ambiguity evidence to a generic resolver input."""

    components = {
        name: part.value
        for name in FIELD_NAMES
        if (part := getattr(result, name)) is not None
    }
    return {
        "raw": result.raw,
        "components": components,
        "warnings": list(result.warnings),
        "alternatives": [
            alternative.as_dict() for alternative in result.alternatives
        ],
    }


def send_json(url: str, payload: dict[str, Any]) -> Any:
    """POST JSON using only the standard library."""

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = Request(
        url,
        data=body,
        headers={"content-type": "application/json; charset=utf-8"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("address")
    parser.add_argument(
        "--resolver-url",
        default="http://127.0.0.1:8080/v1/candidates",
        help="customer-managed endpoint; used only together with --send",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="perform the network request; default is a local dry run",
    )
    args = parser.parse_args()

    payload = resolver_payload(parse(args.address))
    print(json.dumps({"request": payload}, ensure_ascii=False, indent=2))

    if args.send:
        response = send_json(args.resolver_url, payload)
        print(json.dumps({"response": response}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
