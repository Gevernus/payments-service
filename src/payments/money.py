"""Money parsing: strings or JSON numbers into two-place decimals."""

import re
from decimal import Decimal, InvalidOperation
from typing import Annotated, Final

from pydantic import BeforeValidator, Field

# Twelve significant digits in total (ten before the point, two after). A JSON number is parsed
# into a float before it reaches us, and a float is exact only up to 15 significant digits: a
# 17-digit amount sent as a number would be silently altered. Keeping the limit at 12 leaves
# a safe margin (design §4.3).
MAX_DIGITS: Final = 12
AMOUNT_PATTERN: Final = r"^\d{1,10}(\.\d{1,2})?$"

_AMOUNT_RE: Final = re.compile(AMOUNT_PATTERN, re.ASCII)
_CENT: Final = Decimal("0.01")


def parse_amount(value: object) -> Decimal:
    """Validate the string format of an amount and quantize it to two places."""
    if isinstance(value, bool):
        raise ValueError("amount must be a number or a string")
    if isinstance(value, str):
        if _AMOUNT_RE.fullmatch(value) is None:
            raise ValueError(f"amount must match {AMOUNT_PATTERN}")
        return Decimal(value).quantize(_CENT)
    if isinstance(value, int | float | Decimal):
        # repr() gives the shortest decimal that round-trips, so 0.1 stays 0.1.
        number = value if isinstance(value, Decimal) else Decimal(repr(value))
        if not number.is_finite():
            raise ValueError("amount must be finite")
        try:
            quantized = number.quantize(_CENT)
        except InvalidOperation as exc:
            raise ValueError("amount is out of range") from exc
        if quantized != number:
            raise ValueError("amount must have at most two decimal places")
        return quantized
    raise ValueError("amount must be a number or a string")


Amount = Annotated[
    Decimal,
    BeforeValidator(parse_amount),
    Field(gt=0, max_digits=MAX_DIGITS, decimal_places=2),
]
