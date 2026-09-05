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


@pytest.mark.parametrize(
    ("mutate", "error"),
    (
        (lambda payload: payload["entries"][0].update(extra=True), "incompatible"),
        (lambda payload: payload["entries"][0].update(value=7), "must be text"),
        (lambda payload: payload["entries"][0].update(value="invalid"), "is invalid"),
        (lambda payload: payload["entries"][0].update(value="-01"), "not canonical"),
        (lambda payload: payload["entries"].reverse(), "not ordered"),
    ),
)
def test_quarantine_rejects_noncanonical_integer_entries(mutate, error):
    payload = provider_identifier_quarantine_payload({-2: 1, -1: 1})
    mutate(payload)

    with pytest.raises(ValueError, match=error):
        validate_provider_identifier_quarantine(payload)


@pytest.mark.parametrize("counts", ({True: 1}, {123456789: True}))
def test_quarantine_rejects_non_integer_keys_and_counts(counts):
    with pytest.raises(ValueError):
        provider_identifier_quarantine_payload(counts)


def test_quarantine_binds_malformed_text_and_combines_with_v1():
    legacy = provider_identifier_quarantine_payload({123456789: 1})
    typed = provider_identifier_quarantine_payload(
        {}, text_counts={"1447744750`": 2}
    )

    assert typed["contract"] == "ptg2_provider_identifier_quarantine_v2"
    assert validate_provider_identifier_quarantine(typed) == typed
    assert typed["provider_group_conflict_count"] == 0
    assert typed["provider_group_conflicting_definition_count"] == 0
    assert typed["provider_group_definition_conflicts"] == []
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


def test_quarantine_v2_combines_typed_entries_and_definition_conflicts():
    conflict_parts = (
        {
            "provider_group_id_sha256": "1" * 64,
            "definition_sha256": ["2" * 64, "3" * 64],
        },
        {
            "provider_group_id_sha256": "1" * 64,
            "definition_sha256": ["3" * 64, "4" * 64],
        },
    )
    combined = provider_identifier_quarantine_payload(
        {123456789: 2},
        text_counts={"1447744750`": 1},
        provider_group_definition_conflicts=conflict_parts,
    )
    partitioned = combine_provider_identifier_quarantines(
        (
            provider_identifier_quarantine_payload(
                {123456789: 2},
                provider_group_definition_conflicts=(conflict_parts[0],),
            ),
            provider_identifier_quarantine_payload(
                {},
                text_counts={"1447744750`": 1},
                provider_group_definition_conflicts=(conflict_parts[1],),
            ),
        )
    )

    assert combined == partitioned
    assert validate_provider_identifier_quarantine(combined) == combined
    assert combined["provider_group_conflict_count"] == 1
    assert combined["provider_group_conflicting_definition_count"] == 3
    assert combined["provider_group_definition_conflicts"] == [
        {
            "provider_group_id_sha256": "1" * 64,
            "definition_sha256": ["2" * 64, "3" * 64, "4" * 64],
        }
    ]
    evidence = provider_identifier_quarantine_evidence(combined)
    assert validate_provider_identifier_quarantine_evidence(evidence) == evidence


def test_combined_v2_digest_matches_rust_scanner_contract():
    payload = provider_identifier_quarantine_payload(
        {-1: 2},
        text_counts={"bad": 3},
        provider_group_definition_conflicts=(
            {
                "provider_group_id_sha256": "1" * 64,
                "definition_sha256": ["2" * 64, "3" * 64],
            },
        ),
    )

    assert payload["sha256"] == (
        "4648d8c0bd10e0f69cd6c54d8d11a186c9f960847059855fd55fa3b76778a537"
    )


def test_quarantine_v2_deduplicates_and_unions_conflict_evidence():
    first = provider_identifier_quarantine_payload(
        {},
        provider_group_definition_conflicts=(
            {
                "provider_group_id_sha256": "1" * 64,
                "definition_sha256": ["2" * 64, "3" * 64],
            },
        ),
    )
    second = provider_identifier_quarantine_payload(
        {},
        provider_group_definition_conflicts=(
            {
                "provider_group_id_sha256": "1" * 64,
                "definition_sha256": ["3" * 64, "4" * 64],
            },
        ),
    )

    forward = combine_provider_identifier_quarantines((first, second, first))
    reverse = combine_provider_identifier_quarantines((second, first))

    assert forward == reverse
    assert forward["provider_group_conflicting_definition_count"] == 3
    assert forward["provider_group_definition_conflicts"][0][
        "definition_sha256"
    ] == ["2" * 64, "3" * 64, "4" * 64]


def test_quarantine_v2_preserves_source_scoped_conflict_identities():
    source_a = provider_identifier_quarantine_payload(
        {},
        provider_group_definition_conflicts=(
            {
                "provider_group_id_sha256": "1" * 64,
                "definition_sha256": ["2" * 64, "3" * 64],
            },
        ),
    )
    source_b = provider_identifier_quarantine_payload(
        {},
        provider_group_definition_conflicts=(
            {
                "provider_group_id_sha256": "4" * 64,
                "definition_sha256": ["2" * 64, "3" * 64],
            },
        ),
    )

    combined = combine_provider_identifier_quarantines((source_a, source_b))

    assert combined["provider_group_conflict_count"] == 2
    assert combined["provider_group_conflicting_definition_count"] == 4
    assert [
        conflict["provider_group_id_sha256"]
        for conflict in combined["provider_group_definition_conflicts"]
    ] == ["1" * 64, "4" * 64]


def test_quarantine_v2_stops_aggregate_conflict_growth_at_contract_caps():
    payloads = (
        provider_identifier_quarantine_payload(
            {},
            provider_group_definition_conflicts=(
                {
                    "provider_group_id_sha256": f"{index:064x}",
                    "definition_sha256": ["a" * 64, "b" * 64],
                },
            ),
        )
        for index in range(1025)
    )
    with pytest.raises(ValueError, match="exceed 1024 identifiers"):
        combine_provider_identifier_quarantines(payloads)

    first_definitions = [f"{index:064x}" for index in range(2049)]
    second_definitions = [f"{index:064x}" for index in range(2049, 4098)]
    partitions = tuple(
        provider_identifier_quarantine_payload(
            {},
            provider_group_definition_conflicts=(
                {
                    "provider_group_id_sha256": "f" * 64,
                    "definition_sha256": definitions,
                },
            ),
        )
        for definitions in (first_definitions, second_definitions)
    )
    with pytest.raises(ValueError, match="exceed 4096 definitions"):
        combine_provider_identifier_quarantines(partitions)


def test_quarantine_v2_rejects_tampered_conflicts_and_evidence():
    payload = provider_identifier_quarantine_payload(
        {},
        provider_group_definition_conflicts=(
            {
                "provider_group_id_sha256": "1" * 64,
                "definition_sha256": ["2" * 64, "3" * 64],
            },
        ),
    )
    payload["provider_group_definition_conflicts"][0]["definition_sha256"].reverse()
    with pytest.raises(ValueError, match="conflict"):
        validate_provider_identifier_quarantine(payload)

    evidence = provider_identifier_quarantine_evidence(
        provider_identifier_quarantine_payload(
            {},
            provider_group_definition_conflicts=(
                {
                    "provider_group_id_sha256": "1" * 64,
                    "definition_sha256": ["2" * 64, "3" * 64],
                },
            ),
        )
    )
    evidence["provider_group_conflicting_definition_count"] = 1
    with pytest.raises(ValueError, match="evidence"):
        validate_provider_identifier_quarantine_evidence(evidence)


@pytest.mark.parametrize(
    ("conflict", "error"),
    [
        (
            {
                "provider_group_id_sha256": "x" * 64,
                "definition_sha256": ["2" * 64, "3" * 64],
            },
            "identifier digest",
        ),
        (
            {"provider_group_id_sha256": "1" * 64},
            "fields are incompatible",
        ),
        (
            {
                "provider_group_id_sha256": "1" * 64,
                "definition_sha256": "2" * 64,
            },
            "definitions must be an array",
        ),
    ],
)
def test_quarantine_v2_rejects_malformed_conflict_records(conflict, error):
    with pytest.raises(ValueError, match=error):
        provider_identifier_quarantine_payload(
            {}, provider_group_definition_conflicts=(conflict,)
        )


def test_quarantine_v2_rejects_incompatible_payload_and_evidence_fields():
    payload = provider_identifier_quarantine_payload(
        {}, text_counts={"bad": 1}
    )
    payload.pop("provider_group_conflict_count")
    with pytest.raises(ValueError, match="fields are incompatible"):
        validate_provider_identifier_quarantine(payload)

    evidence = provider_identifier_quarantine_evidence(
        provider_identifier_quarantine_payload({}, text_counts={"bad": 1})
    )
    evidence["unexpected"] = 0
    with pytest.raises(ValueError, match="evidence is incompatible"):
        validate_provider_identifier_quarantine_evidence(evidence)


def test_quarantine_v2_bounds_conflict_evidence_before_copying():
    with pytest.raises(ValueError, match="exceed 4096 definitions"):
        provider_identifier_quarantine_payload(
            {},
            provider_group_definition_conflicts=(
                {
                    "provider_group_id_sha256": "1" * 64,
                    "definition_sha256": [f"{index:064x}" for index in range(4097)],
                },
            ),
        )

    conflicts = (
        {
            "provider_group_id_sha256": f"{index:064x}",
            "definition_sha256": ["a" * 64, "b" * 64],
        }
        for index in range(1025)
    )
    with pytest.raises(ValueError, match="exceed 1024 identifiers"):
        provider_identifier_quarantine_payload(
            {}, provider_group_definition_conflicts=conflicts
        )


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
