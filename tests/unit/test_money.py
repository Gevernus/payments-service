"""Amount parsing: strings or JSON numbers into two-place decimals (design §4.3)."""

from decimal import Decimal

import pytest
from pydantic import TypeAdapter, ValidationError

from payments.money import Amount

ADAPTER: TypeAdapter[Decimal] = TypeAdapter(Amount)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (100, "100.00"),
        (100.5, "100.50"),
        (0.01, "0.01"),
        ("100", "100.00"),
        ("100.5", "100.50"),
        ("100.00", "100.00"),
        ("0.01", "0.01"),
        ("9999999999.99", "9999999999.99"),
        ("1234567890", "1234567890.00"),
    ],
)
def test_amount_accepts_number_and_string(value: object, expected: str) -> None:
    result = ADAPTER.validate_python(value)
    assert result == Decimal(expected)
    assert str(result) == expected


def test_amount_quantizes_exponent() -> None:
    """A JSON number written with an exponent is stored with exactly two decimal places."""
    from_python = ADAPTER.validate_python(1e2)
    from_json = ADAPTER.validate_json(b"1e2")
    for result in (from_python, from_json):
        assert result == Decimal("100.00")
        assert result.as_tuple().exponent == -2
        assert str(result) == "100.00"


@pytest.mark.parametrize(
    "value",
    [
        0,
        -1,
        "1.001",
        "NaN",
        "Infinity",
        1234567890123456.78,
        "100_000",
        "+100",
        "\u0661\u0660\u0660",  # Arabic-Indic digits
        " 100",
        True,
    ],
)
def test_amount_rejects(value: object) -> None:
    with pytest.raises(ValidationError):
        ADAPTER.validate_python(value)
