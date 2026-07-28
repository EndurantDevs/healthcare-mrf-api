# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from process.ext.utils import return_checksum


def doctor_address_row(provider_row, now) -> dict | None:
    """Normalize one CMS Doctors row with its stable address checksum."""
    npi_text = provider_row.get("NPI") or provider_row.get("npi")
    if not npi_text:
        return None
    try:
        npi = int(npi_text)
    except ValueError:
        return None
    address_line1 = (
        provider_row.get("Line 1 Street Address")
        or provider_row.get("adr_ln_1")
    )
    address_line2 = (
        provider_row.get("Line 2 Street Address")
        or provider_row.get("adr_ln_2")
    )
    city = (
        provider_row.get("City")
        or provider_row.get("City/Town")
        or provider_row.get("citytown")
    )
    state = provider_row.get("State") or provider_row.get("state")
    zip_code = str(
        provider_row.get("Zip Code")
        or provider_row.get("ZIP Code")
        or provider_row.get("zip_code")
        or ""
    )[:5]
    provider_type = (
        provider_row.get("Primary specialty")
        or provider_row.get("pri_spec")
    )
    if not address_line1 or len(zip_code) < 5:
        return None
    address_checksum = return_checksum(
        [
            npi,
            address_line1,
            address_line2 or "",
            city or "",
            state or "",
            zip_code,
            provider_type or "",
        ]
    )
    return {
        "npi": npi,
        "address_checksum": address_checksum,
        "address_line1": address_line1,
        "address_line2": address_line2,
        "city": city,
        "state": state,
        "zip_code": zip_code,
        "provider_type": provider_type,
        "updated_at": now,
    }
