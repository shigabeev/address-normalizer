"""Small, offline Russian address parser."""

from .api import parse, parse_many
from .types import AddressPart, Alternative, ParsedAddress

__all__ = [
    "AddressPart",
    "Alternative",
    "ParsedAddress",
    "parse",
    "parse_many",
]

__version__ = "2.0.0a1"
