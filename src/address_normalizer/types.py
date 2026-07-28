"""Public result types for the parser."""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any


@dataclass(frozen=True, slots=True)
class AddressPart:
    """One component extracted from the original string."""

    value: str
    raw: str
    start: int
    end: int
    confidence: float
    source: str

    @property
    def span(self) -> tuple[int, int]:
        return self.start, self.end

    def as_dict(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "raw": self.raw,
            "span": [self.start, self.end],
            "confidence": round(self.confidence, 4),
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class Alternative:
    """A plausible competing interpretation retained for a downstream resolver."""

    components: dict[str, str]
    confidence: float
    reason: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "components": dict(self.components),
            "confidence": round(self.confidence, 4),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class ParsedAddress:
    """Structured, unverified interpretation of an address string."""

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

    def as_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "raw": self.raw,
            "normalized": self.normalized,
            "confidence": round(self.confidence, 4),
        }
        special = {"raw", "unparsed", "warnings", "alternatives", "confidence"}
        for field in fields(self):
            if field.name in special:
                continue
            value = getattr(self, field.name)
            data[field.name] = value.as_dict() if value else None
        data["unparsed"] = [part.as_dict() for part in self.unparsed]
        data["warnings"] = list(self.warnings)
        data["alternatives"] = [
            alternative.as_dict() for alternative in self.alternatives
        ]
        return data
