"""Public result types for the parser."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict


class AddressPartDict(TypedDict):
    """JSON-compatible serialization of :class:`AddressPart`."""

    value: str
    raw: str
    span: list[int]
    confidence: float
    source: str


class AlternativeDict(TypedDict):
    """JSON-compatible serialization of :class:`Alternative`."""

    components: dict[str, str]
    confidence: float
    reason: str


class ParsedAddressDict(TypedDict):
    """Stable JSON-compatible serialization of :class:`ParsedAddress`."""

    raw: str
    normalized: str
    confidence: float
    postal_code: AddressPartDict | None
    region: AddressPartDict | None
    district: AddressPartDict | None
    city: AddressPartDict | None
    settlement: AddressPartDict | None
    street: AddressPartDict | None
    street_type: AddressPartDict | None
    house_num: AddressPartDict | None
    corpus: AddressPartDict | None
    structure: AddressPartDict | None
    apartment: AddressPartDict | None
    unparsed: list[AddressPartDict]
    warnings: list[str]
    alternatives: list[AlternativeDict]


@dataclass(frozen=True, slots=True)
class AddressPart:
    """One component extracted from a half-open span of the original string.

    ``confidence`` is a bounded decision-strength score, not a calibrated
    probability.
    """

    value: str
    raw: str
    start: int
    end: int
    confidence: float
    source: str

    @property
    def span(self) -> tuple[int, int]:
        return self.start, self.end

    def as_dict(self) -> AddressPartDict:
        """Return the stable JSON-compatible representation."""

        return {
            "value": self.value,
            "raw": self.raw,
            "span": [self.start, self.end],
            "confidence": round(self.confidence, 4),
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class Alternative:
    """A plausible competing interpretation retained for downstream review.

    ``confidence`` is a relative decision-strength score, not a calibrated
    probability.
    """

    components: dict[str, str]
    confidence: float
    reason: str

    def as_dict(self) -> AlternativeDict:
        """Return the stable JSON-compatible representation."""

        return {
            "components": dict(self.components),
            "confidence": round(self.confidence, 4),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class ParsedAddress:
    """Structured, unverified interpretation of an address string.

    Component spans always index ``raw``. Review ``warnings``, ``alternatives``,
    and ``unparsed`` before accepting an automated interpretation.
    ``confidence`` is a decision-strength score, not a probability that the
    address exists or is correct.
    """

    raw: str
    postal_code: AddressPart | None = None
    region: AddressPart | None = None
    district: AddressPart | None = None
    city: AddressPart | None = None
    settlement: AddressPart | None = None
    street: AddressPart | None = None
    street_type: AddressPart | None = None
    house_num: AddressPart | None = None
    corpus: AddressPart | None = None
    structure: AddressPart | None = None
    apartment: AddressPart | None = None
    unparsed: tuple[AddressPart, ...] = ()
    warnings: tuple[str, ...] = ()
    alternatives: tuple[Alternative, ...] = ()
    confidence: float = 0.0

    @property
    def normalized(self) -> str:
        """Return a display-oriented composition of extracted values."""

        chunks: list[str] = []
        for part in (
            self.postal_code,
            self.region,
            self.district,
            self.city,
            self.settlement,
        ):
            if part:
                chunks.append(part.value)

        if self.street:
            if self.street_type:
                chunks.append(f"{self.street_type.value} {self.street.value}")
            else:
                chunks.append(self.street.value)

        for prefix, part in (
            ("д", self.house_num),
            ("корп", self.corpus),
            ("стр", self.structure),
            ("кв", self.apartment),
        ):
            if part:
                chunks.append(f"{prefix} {part.value}")
        return ", ".join(chunks)

    def as_dict(self) -> ParsedAddressDict:
        """Return the stable JSON-compatible representation."""

        return {
            "raw": self.raw,
            "normalized": self.normalized,
            "confidence": round(self.confidence, 4),
            "postal_code": self.postal_code.as_dict()
            if self.postal_code
            else None,
            "region": self.region.as_dict() if self.region else None,
            "district": self.district.as_dict() if self.district else None,
            "city": self.city.as_dict() if self.city else None,
            "settlement": self.settlement.as_dict() if self.settlement else None,
            "street": self.street.as_dict() if self.street else None,
            "street_type": self.street_type.as_dict()
            if self.street_type
            else None,
            "house_num": self.house_num.as_dict() if self.house_num else None,
            "corpus": self.corpus.as_dict() if self.corpus else None,
            "structure": self.structure.as_dict() if self.structure else None,
            "apartment": self.apartment.as_dict() if self.apartment else None,
            "unparsed": [part.as_dict() for part in self.unparsed],
            "warnings": list(self.warnings),
            "alternatives": [
                alternative.as_dict() for alternative in self.alternatives
            ],
        }
