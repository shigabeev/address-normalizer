"""Detect address spans in a free-form message without a registry lookup."""

from __future__ import annotations

import json

from address_normalizer import detect_addresses


MESSAGE = (
    "Курьер приедет по адресу: Москва, ул. Тверская, "
    "д. 13, кв. 4. Позвоните заранее."
)


def main() -> None:
    for detected in detect_addresses(MESSAGE):
        assert MESSAGE[detected.start : detected.end] == detected.text
        print(json.dumps(detected.as_dict(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
