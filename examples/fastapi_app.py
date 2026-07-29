"""Optional FastAPI wrapper; FastAPI is not a package runtime dependency."""

from __future__ import annotations

from address_normalizer import ParsedAddressDict, parse
from fastapi import FastAPI
from pydantic import BaseModel


class ParseRequest(BaseModel):
    address: str


app = FastAPI(title="address-normalizer example", version="1")


@app.post("/parse")
def parse_address(request: ParseRequest) -> ParsedAddressDict:
    """Extract fields; callers must still review or resolve the result."""

    return parse(request.address).as_dict()
