"""Public parser API."""

from __future__ import annotations

from functools import lru_cache
from typing import Iterable, Iterator

from .parser import AddressParser
from .types import ParsedAddress


@lru_cache(maxsize=1)
def _default_parser() -> AddressParser:
    return AddressParser()


def _require_address(value: object, *, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _addresses(addresses: Iterable[str]) -> Iterator[str]:
    if isinstance(addresses, (str, bytes, bytearray)):
        raise TypeError(
            "addresses must be an iterable of strings, not a single string"
        )
    try:
        iterator = iter(addresses)
    except TypeError:
        raise TypeError("addresses must be an iterable of strings") from None
    for index, address in enumerate(iterator):
        yield _require_address(address, name=f"addresses[{index}]")


def parse(text: str) -> ParsedAddress:
    """Parse one string without validating it against an external registry.

    Empty and punctuation-only strings return an empty result that retains
    ``raw``. Non-string values raise :class:`TypeError`.
    """

    return _default_parser().parse(_require_address(text, name="address"))


def parse_iter(addresses: Iterable[str]) -> Iterator[ParsedAddress]:
    """Lazily parse an iterable of strings in input order.

    The iterator keeps only one parsed result at a time. A bare string is
    rejected because it is one address, not a batch. Invalid elements raise
    :class:`TypeError` when iteration reaches them.
    """

    parser = _default_parser()
    for address in _addresses(addresses):
        yield parser.parse(address)


def parse_many(addresses: Iterable[str]) -> list[ParsedAddress]:
    """Parse an iterable of strings into an ordered list.

    Use :func:`parse_iter` when the complete result list should not be retained
    in memory.
    """

    return list(parse_iter(addresses))
