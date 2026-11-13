"""Pure value parsers. No I/O, so every spec rule here is unit-testable."""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

IST = timezone(timedelta(hours=5, minutes=30))
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_DATE_FORMATS = ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d-%b-%Y")
_AMOUNT = re.compile(r"^(?:₹|rs\.?|inr)?\s*(\d+(?:\.\d+)?)$", re.IGNORECASE)


def normalize_header(header: str) -> str:
    return re.sub(r"[_\s]+", " ", header.strip().lower()).strip()


def normalize_phone(raw: str) -> str | None:
    """Canonical +91XXXXXXXXXX, or None if not a valid Indian mobile number."""
    digits = re.sub(r"[^0-9]", "", raw)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10 and digits[0] in "6789":
        return "+91" + digits
    return None


def parse_date(raw: str) -> date | None:
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def parse_decimal(raw: str) -> str | None:
    m = _AMOUNT.match(raw.replace(",", "").strip())
    if not m:
        return None
    return str(Decimal(m.group(1)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def parse_timestamp_us(raw: object) -> int | None:
    """UTC epoch microseconds. Naive timestamps are IST (spec)."""
    if not isinstance(raw, str):
        return None
    try:
        dt = datetime.fromisoformat(raw.strip())
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=IST)
    delta = dt - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 10**6 + delta.microseconds


def ist_datetime(us: int) -> datetime:
    return datetime.fromtimestamp(us // 10**6, IST)
