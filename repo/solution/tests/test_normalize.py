import pytest

from solution.normalize import (normalize_header, normalize_phone, parse_date, parse_decimal,
                                parse_timestamp_us)


@pytest.mark.parametrize("raw", ["9876543210", "+91 98765 43210", "09876543210",
                                 "91-98765-43210", "(+91) 9876543210", "+919876543210"])
def test_phone_formats(raw):
    assert normalize_phone(raw) == "+919876543210"


@pytest.mark.parametrize("raw", ["987654321", "5876543210", "022-2345-6789", "N/A",
                                 "+1 415 555 0134", "", "919876543210 1"])
def test_phone_invalid(raw):
    assert normalize_phone(raw) is None


def test_dates_are_day_first():
    assert parse_date("01/02/2026").isoformat() == "2026-02-01"
    assert parse_date("01-02-2026").isoformat() == "2026-02-01"
    assert parse_date("2026-02-01").isoformat() == "2026-02-01"
    assert parse_date("01-DEC-2026").isoformat() == "2026-12-01"
    assert parse_date("01-dec-2026").isoformat() == "2026-12-01"


@pytest.mark.parametrize("raw", ["31/02/2026", "2026-13-01", "soon", "TBD", "01.02.2026"])
def test_dates_invalid(raw):
    assert parse_date(raw) is None


@pytest.mark.parametrize("raw,expected", [
    ("12000", "12000.00"), ("₹12,000", "12000.00"), ("Rs. 4500", "4500.00"),
    ("rs 4500.5", "4500.50"), ("INR 1234.5", "1234.50"), ("inr1,234.567", "1234.57"),
    ("0", "0.00"), ("1.005", "1.01")])
def test_decimal(raw, expected):
    assert parse_decimal(raw) == expected


@pytest.mark.parametrize("raw", ["N/A", "-", "twelve thousand", "-5", "Rs.", ""])
def test_decimal_invalid(raw):
    assert parse_decimal(raw) is None


def test_header_normalisation():
    assert normalize_header("  Policy_No ") == "policy no"
    assert normalize_header("Mobile__ No") == "mobile no"


def test_timestamps_naive_is_ist():
    utc = parse_timestamp_us("2026-10-03T04:31:00Z")
    assert parse_timestamp_us("2026-10-03 10:01:00") == utc
    assert parse_timestamp_us("2026-10-03T10:01:00+05:30") == utc
    assert parse_timestamp_us("garbage") is None
