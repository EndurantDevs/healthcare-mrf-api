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


def test_quarantine_binds_malformed_text_and_combines_with_v1():
    legacy = provider_identifier_quarantine_payload({123456789: 1})
    typed = provider_identifier_quarantine_payload(
        {}, text_counts={"1447744750`": 2}
    )

    assert typed["contract"] == "ptg2_provider_identifier_quarantine_v2"
    assert validate_provider_identifier_quarantine(typed) == typed
    assert typed["entries"] == [
        {
            "kind": "string",
            "value_sha256": "27e0d2def7d3bfb8c0538e8af4def83d193d1a59bcdf96c2d1e5ea67e7c766a3",
            "byte_length": 11,
            "occurrence_count": 2,
        }
    ]

    combined = combine_provider_identifier_quarantines((legacy, typed))
    assert combined["contract"] == "ptg2_provider_identifier_quarantine_v2"
    assert combined["occurrence_count"] == 3
    assert combined["distinct_value_count"] == 2


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("value_sha256", "x" * 64, "identity"),
        ("byte_length", 129, "identity"),
        ("occurrence_count", -1, "count"),
    ],
)
def test_v2_quarantine_rejects_noncanonical_text_identity(field, value, error):
    payload = provider_identifier_quarantine_payload(
        {}, text_counts={"1447744750`": 1}
    )
    payload["entries"][0][field] = value
    with pytest.raises(ValueError, match=error):
        validate_provider_identifier_quarantine(payload)


def test_v2_quarantine_rejects_malformed_text_boundaries():
    with pytest.raises(ValueError, match="exceeds 128 bytes"):
        provider_identifier_quarantine_payload({}, text_counts={"x" * 129: 1})
    with pytest.raises(ValueError, match="is not text"):
        provider_identifier_quarantine_payload({}, text_counts={1: 1})
    with pytest.raises(ValueError, match="count is invalid"):
        provider_identifier_quarantine_payload({}, text_counts={"bad": 0})

    quarantine = provider_identifier_quarantine_payload({}, text_counts={"bad": 1})
    quarantine["entries"][0]["kind"] = "text"
    with pytest.raises(ValueError, match="entry is incompatible"):
        validate_provider_identifier_quarantine(quarantine)

    quarantine = provider_identifier_quarantine_payload({}, text_counts={"bad": 1})
    quarantine["entries"] = [None]
    with pytest.raises(ValueError, match="entry is incompatible"):
        validate_provider_identifier_quarantine(quarantine)

    quarantine = provider_identifier_quarantine_payload(
        {}, text_counts={"first": 1, "second": 1}
    )
    quarantine["entries"].reverse()
    with pytest.raises(ValueError, match="values are not ordered"):
        validate_provider_identifier_quarantine(quarantine)

    quarantine = provider_identifier_quarantine_payload(
        {}, text_counts={"first": 1, "second": 1}
    )
    quarantine["entries"][0]["occurrence_count"] = 2**64 - 1
    with pytest.raises(ValueError, match="occurrence count overflows"):
        validate_provider_identifier_quarantine(quarantine)

    quarantine = provider_identifier_quarantine_payload(
        {-2: 1, -1: 1}, text_counts={"bad": 1}
    )
    quarantine["entries"][0]["occurrence_count"] = 2**64 - 1
    with pytest.raises(ValueError, match="occurrence count overflows"):
        validate_provider_identifier_quarantine(quarantine)
