"""Small, offline Russian address parser."""

from .api import detect_addresses, parse, parse_iter, parse_many
from .types import (
    AddressPart,
    AddressPartDict,
    Alternative,
    AlternativeDict,
    DetectedAddress,
    DetectedAddressDict,
    ParsedAddress,
    ParsedAddressDict,
)

__all__ = [
    "AddressPart",
    "AddressPartDict",
    "Alternative",
    "AlternativeDict",
    "DetectedAddress",
    "DetectedAddressDict",
    "ParsedAddress",
    "ParsedAddressDict",
    "detect_addresses",
    "parse",
    "parse_iter",
    "parse_many",
]

__version__ = "2.0.0"
