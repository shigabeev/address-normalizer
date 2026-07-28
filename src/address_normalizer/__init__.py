"""Small, offline Russian address parser."""

from .api import parse, parse_iter, parse_many
from .types import (
    AddressPart,
    AddressPartDict,
    Alternative,
    AlternativeDict,
    ParsedAddress,
    ParsedAddressDict,
)

__all__ = [
    "AddressPart",
    "AddressPartDict",
    "Alternative",
    "AlternativeDict",
    "ParsedAddress",
    "ParsedAddressDict",
    "parse",
    "parse_iter",
    "parse_many",
]

__version__ = "2.0.0a1"
