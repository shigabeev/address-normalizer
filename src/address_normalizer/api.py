"""Public parser API."""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable

from .parser import AddressParser
from .types import ParsedAddress


@lru_cache(maxsize=1)
def _default_parser() -> AddressParser:
    return AddressParser()


def parse(text: str) -> ParsedAddress:
    """Parse one address without validating it against an external registry."""

    if not isinstance(text, str):
        raise TypeError("address must be a string")
    return _default_parser().parse(text)


def parse_many(addresses: Iterable[str]) -> list[ParsedAddress]:
    """Parse addresses while keeping order and without creating a DataFrame."""

    parser = _default_parser()
    return [parser.parse(address) for address in addresses]
