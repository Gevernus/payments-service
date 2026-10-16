"""Amount parsing: strings or JSON numbers into two-place decimals."""

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
        "10000000000",  # eleven integer digits
        "100000000000.00",
        10000000000,
        1e-7,
    ],
)
def test_amount_rejects(value: object) -> None:
    with pytest.raises(ValidationError):
        ADAPTER.validate_python(value)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("100"), "100.00"),
        (Decimal("100.5"), "100.50"),
        (Decimal("1E+2"), "100.00"),
        (Decimal("9999999999.99"), "9999999999.99"),
    ],
)
def test_amount_accepts_decimal_instance(value: Decimal, expected: str) -> None:
    result = ADAPTER.validate_python(value)
    assert str(result) == expected


@pytest.mark.parametrize(
    "value",
    [
        Decimal("0"),
        Decimal("-1"),
        Decimal("1.001"),
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("10000000000"),
        Decimal("1E+30"),
    ],
)
def test_amount_rejects_decimal_instance(value: Decimal) -> None:
    with pytest.raises(ValidationError):
        ADAPTER.validate_python(value)
