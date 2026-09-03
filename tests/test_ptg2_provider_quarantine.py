# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from process.ptg_parts.ptg2_provider_quarantine import (
    combine_provider_identifier_quarantines,
    provider_identifier_quarantine_evidence,
    provider_identifier_quarantine_payload,
    validate_provider_identifier_quarantine,
    validate_provider_identifier_quarantine_evidence,
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


def test_quarantine_v2_seals_and_combines_provider_group_definition_conflicts():
    conflict = {
        "provider_group_id_sha256": "1" * 64,
        "definition_sha256": ["2" * 64, "3" * 64],
    }
    first = provider_identifier_quarantine_payload(
        {123456789: 2},
        provider_group_definition_conflicts=(conflict,),
    )
    second = provider_identifier_quarantine_payload({})

    assert first["contract"] == "ptg2_provider_identifier_quarantine_v2"
    assert first["provider_group_conflict_count"] == 1
    assert first["provider_group_conflicting_definition_count"] == 2
    assert first["provider_group_definition_conflicts"] == [conflict]
    assert validate_provider_identifier_quarantine(first) == first

    combined = combine_provider_identifier_quarantines((second, first))
    assert combined == first
    evidence = provider_identifier_quarantine_evidence(combined)
    assert evidence["provider_group_conflict_count"] == 1
    assert evidence["provider_group_conflicting_definition_count"] == 2
    assert validate_provider_identifier_quarantine_evidence(evidence) == evidence


def test_quarantine_v2_rejects_noncanonical_or_tampered_conflict_evidence():
    conflict = {
        "provider_group_id_sha256": "1" * 64,
        "definition_sha256": ["2" * 64, "3" * 64],
    }
    payload = provider_identifier_quarantine_payload(
        {},
        provider_group_definition_conflicts=(conflict,),
    )

    payload["provider_group_definition_conflicts"][0]["definition_sha256"].reverse()
    with pytest.raises(ValueError, match="conflict"):
        validate_provider_identifier_quarantine(payload)

    evidence = provider_identifier_quarantine_evidence(
        provider_identifier_quarantine_payload(
            {},
            provider_group_definition_conflicts=(conflict,),
        )
    )
    evidence["provider_group_conflicting_definition_count"] = 1
    with pytest.raises(ValueError, match="evidence"):
        validate_provider_identifier_quarantine_evidence(evidence)


def test_quarantine_v2_digest_matches_rust_scanner_contract():
    payload = provider_identifier_quarantine_payload(
        {},
        provider_group_definition_conflicts=(
            {
                "provider_group_id_sha256": (
                    "3da1de240af934e76c7d88ff8dda3f31"
                    "d9cd997ef000e9635e22bdb62be867ba"
                ),
                "definition_sha256": [
                    "0adaee3f8ac1aba9431a53933bc0b3ff"
                    "857d10a5d6a6e99e8fa476f9947b4e9a",
                    "4bfd3beef3681fdd8adaf2652694dde55"
                    "c68ff125b5d8a350f9f5af93d7745e6",
                ],
            },
        ),
    )

    assert payload["sha256"] == (
        "ac84df86ede681fcfca903a30bb00a23ec4f13d4564f4297d5be88f5f448d264"
    )
