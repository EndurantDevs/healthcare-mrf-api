# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from process.ptg_parts.ptg2_provider_quarantine import (
    combine_provider_identifier_quarantines,
    provider_identifier_quarantine_payload,
    validate_provider_identifier_quarantine,
)


def test_quarantine_is_canonical_and_combines_exact_occurrences():
    first = provider_identifier_quarantine_payload({123456789: 2, -1: 1})
    second = provider_identifier_quarantine_payload({123456789: 3})

    assert validate_provider_identifier_quarantine(first) == first
    combined = combine_provider_identifier_quarantines((first, second))

    assert combined["occurrence_count"] == 6
    assert combined["distinct_value_count"] == 2
    assert combined["entries"] == [
        {"value": "-1", "occurrence_count": 1},
        {"value": "123456789", "occurrence_count": 5},
    ]
    assert len(combined["sha256"]) == 64

    parity = provider_identifier_quarantine_payload({123456789: 2})
    assert parity["sha256"] == (
        "6b01033baec61d1e9d4738f0f12cf2f48cefbd6a801fd0bd4a9b76d1b570624b"
    )


def test_quarantine_rejects_valid_npis_and_tampered_digest():
    with pytest.raises(ValueError, match="valid NPI"):
        provider_identifier_quarantine_payload({1234567890: 1})
    with pytest.raises(ValueError, match="TIN-only"):
        provider_identifier_quarantine_payload({0: 1})

    payload = provider_identifier_quarantine_payload({123456789: 1})
    payload["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="digest or counts"):
        validate_provider_identifier_quarantine(payload)
