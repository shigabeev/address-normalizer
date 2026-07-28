"""Small, registry-independent normalization helpers."""

from __future__ import annotations

import re


NAME_ALIASES = {
    "мск": "Москва",
    "москва": "Москва",
    "спб": "Санкт-Петербург",
    "питер": "Санкт-Петербург",
    "санкт петербург": "Санкт-Петербург",
    "санкт-петербург": "Санкт-Петербург",
}

LOWERCASE_PARTS = {"в", "во", "на", "по", "им", "имени"}


def normalize_name(value: str) -> str:
    value = re.sub(r"\s+", " ", value.strip(" \t\r\n,.;:"))
    folded = value.casefold().replace("ё", "е")
    if folded in NAME_ALIASES:
        return NAME_ALIASES[folded]

    words: list[str] = []
    for word_index, word in enumerate(value.split(" ")):
        hyphenated: list[str] = []
        for part_index, part in enumerate(word.split("-")):
            if not part:
                hyphenated.append(part)
            elif part.isdigit():
                hyphenated.append(part)
            elif re.fullmatch(r"\d+[A-Za-zА-Яа-яЁё]+", part):
                hyphenated.append(part.upper())
            elif part.casefold() in LOWERCASE_PARTS and (
                word_index > 0 or part_index > 0
            ):
                hyphenated.append(part.casefold())
            elif re.fullmatch(r"\d+", word.split("-")[0]) and part_index > 0:
                hyphenated.append(part.casefold())
            else:
                hyphenated.append(part[:1].upper() + part[1:].lower())
        words.append("-".join(hyphenated))
    return " ".join(words)


def normalize_number(value: str) -> str:
    return re.sub(r"\s*([/-])\s*", r"\1", value.strip()).upper()
