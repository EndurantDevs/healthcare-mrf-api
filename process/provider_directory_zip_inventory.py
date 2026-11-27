# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Dated USPS ZIP5 inventory for national directory searches."""

from functools import cache
from pathlib import Path

_ZIP_FILE = Path(__file__).resolve().parents[1] / "support" / "zip" / "usps_zip5_2026_11.txt"


@cache
def national_zip_codes() -> tuple[str, ...]:
    """Read the same ordered five-digit postal codes in every worker on this image."""
    codes = _ZIP_FILE.read_text(encoding="ascii").splitlines()
    if (
        len(codes) != 41515
        or codes != sorted(set(codes))
        or any(len(code) != 5 or not code.isdigit() for code in codes)
    ):
        raise ValueError("national_directory_zip_inventory_invalid")
    return tuple(codes)
