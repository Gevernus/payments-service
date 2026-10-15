"""Money parsing: strings or JSON numbers into two-place decimals."""

from decimal import Decimal
from typing import Annotated, Final

from pydantic import BeforeValidator, Field

# Twelve significant digits in total (ten before the point, two after). A JSON number is parsed
# into a float before it reaches us, and a float is exact only up to 15 significant digits: a
# 17-digit amount sent as a number would be silently altered. Keeping the limit at 12 leaves
# a safe margin (design §4.3).
MAX_DIGITS: Final = 12
AMOUNT_PATTERN: Final = r"^\d{1,10}(\.\d{1,2})?$"


def parse_amount(value: object) -> Decimal:
    """Validate the string format of an amount and quantize it to two places."""
    raise NotImplementedError


Amount = Annotated[
    Decimal,
    BeforeValidator(parse_amount),
    Field(gt=0, max_digits=MAX_DIGITS, decimal_places=2),
]
