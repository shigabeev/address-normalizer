"""Offset-preserving tokenizer for short address strings."""

from __future__ import annotations

from dataclasses import dataclass
import re


TOKEN_RE = re.compile(
    r"[A-Za-zА-Яа-яЁё]+|\d+|[№#]|[^\w\s]",
    re.UNICODE,
)


@dataclass(frozen=True, slots=True)
class Token:
    text: str
    start: int
    end: int

    @property
    def lower(self) -> str:
        return self.text.casefold().replace("ё", "е")

    @property
    def is_word(self) -> bool:
        return self.text.isalpha()

    @property
    def is_number(self) -> bool:
        return self.text.isdigit()

    @property
    def is_separator(self) -> bool:
        return self.text in {",", ";", ":"}


def tokenize(text: str) -> list[Token]:
    return [
        Token(match.group(0), match.start(), match.end())
        for match in TOKEN_RE.finditer(text)
    ]
