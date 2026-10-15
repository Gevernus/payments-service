"""Money parsing: strings or JSON numbers into two-place decimals."""

import re
from decimal import Decimal
from typing import Annotated, Final

from pydantic import AfterValidator, BeforeValidator, Field

# Twelve significant digits in total (ten before the point, two after). A JSON number is parsed
# into a float before it reaches us, and a float is exact only up to 15 significant digits: a
# 17-digit amount sent as a number would be silently altered. Keeping the limit at 12 leaves
# a safe margin (design §4.3).
MAX_DIGITS: Final = 12
AMOUNT_PATTERN: Final = r"^\d{1,10}(\.\d{1,2})?$"

_AMOUNT_RE: Final = re.compile(AMOUNT_PATTERN, re.ASCII)
_CENT: Final = Decimal("0.01")


def _check_string(value: object) -> object:
    """Strings are matched against the amount format; numbers are left to pydantic."""
    if isinstance(value, str) and _AMOUNT_RE.fullmatch(value) is None:
        raise ValueError(f"amount must match {AMOUNT_PATTERN}")
    return value


Amount = Annotated[
    Decimal,
    BeforeValidator(_check_string),
    Field(gt=0, max_digits=MAX_DIGITS, decimal_places=2),
    AfterValidator(lambda amount: amount.quantize(_CENT)),
]
