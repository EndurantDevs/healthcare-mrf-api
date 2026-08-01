# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import hashlib

import pytest

from process.uhc_provider_quarantine_contract import (
    UHC_PROVIDER_QUARANTINE_CONTRACT_ID,
    UHC_PROVIDER_QUARANTINE_FIELD,
    UHC_PROVIDER_QUARANTINE_MAX_COUNT,
    UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,
    UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_STRUCTURE,
    UhcProviderQuarantineError,
    provider_quarantine_catalog_limit,
    provider_quarantine_limit,
    provider_quarantine_rejected_counts,
    provider_quarantine_rejected_totals,
    quarantine_identity_set_sha256,
    validate_provider_quarantine_fact,
)


SOURCE_FILE_ID = hashlib.sha256(b"source-file").hexdigest()
RECORD_SHA256 = hashlib.sha256(b"source-record").hexdigest()


def _tombstone(
    reason: str = UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,
) -> dict[str, object]:
    return {
        UHC_PROVIDER_QUARANTINE_FIELD: {
            "contract_id": UHC_PROVIDER_QUARANTINE_CONTRACT_ID,
            "reason": reason,
            "source_file_id": SOURCE_FILE_ID,
            "range_ordinal": 2,
            "occurrence_ordinal": 17,
            "record_sha256": RECORD_SHA256,
        }
    }


def _counter_map(**overrides: int) -> dict[str, int]:
    counter_by_field = {
        "raw_provider_records": 1_000_001,
        "raw_individual_records": 1_000_000,
        "raw_facility_records": 1,
        "raw_address_rows": 1_000_002,
        "raw_provider_plan_rows": 1_000_003,
        "invalid_npi_count": 2,
        "invalid_npi_individual_records": 1,
        "invalid_npi_facility_records": 1,
        "invalid_npi_address_rows": 3,
        "invalid_npi_provider_plan_rows": 4,
    }
    counter_by_field.update(overrides)
    return counter_by_field


def test_exact_tombstone_is_lineage_bound_and_hashable() -> None:
    tombstone = _tombstone()
    quarantine = validate_provider_quarantine_fact(
        tombstone,
        expected_source_file_id=SOURCE_FILE_ID,
        expected_range_ordinal=2,
        expected_occurrence_ordinal=17,
    )

    assert quarantine is not None
    assert quarantine.record_sha256 == RECORD_SHA256
    assert hashlib.sha256(quarantine.identity_bytes).hexdigest() == (
        "1a8864ce4f33c942b1abf878a274e783e3700a2e494b5bf2bd37e76d4c0496d5"
    )
    assert quarantine_identity_set_sha256([quarantine]) == hashlib.sha256(
        quarantine.identity_bytes
    ).hexdigest()
    second_tombstone = _tombstone()
    second_payload = second_tombstone[UHC_PROVIDER_QUARANTINE_FIELD]
    second_payload["range_ordinal"] = 3
    second_payload["occurrence_ordinal"] = 18
    second_payload["record_sha256"] = hashlib.sha256(
        b"second-source-record"
    ).hexdigest()
    second_validated_quarantine = validate_provider_quarantine_fact(
        second_tombstone,
        expected_source_file_id=SOURCE_FILE_ID,
        expected_range_ordinal=3,
        expected_occurrence_ordinal=18,
    )
    assert second_validated_quarantine is not None
    assert second_validated_quarantine != quarantine
    assert quarantine_identity_set_sha256(
        [quarantine, second_validated_quarantine]
    ) == hashlib.sha256(
        quarantine.identity_bytes
        + b"\n"
        + second_validated_quarantine.identity_bytes
    ).hexdigest()
    assert validate_provider_quarantine_fact(
        {"npi": "1003821380"},
        expected_source_file_id=SOURCE_FILE_ID,
        expected_range_ordinal=2,
        expected_occurrence_ordinal=17,
    ) is None


def test_structural_reason_is_identity_bound_without_source_value() -> None:
    quarantine = validate_provider_quarantine_fact(
        _tombstone(UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_STRUCTURE),
        expected_source_file_id=SOURCE_FILE_ID,
        expected_range_ordinal=2,
        expected_occurrence_ordinal=17,
    )

    assert quarantine is not None
    assert quarantine.reason == UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_STRUCTURE
    assert quarantine.identity_bytes != validate_provider_quarantine_fact(
        _tombstone(),
        expected_source_file_id=SOURCE_FILE_ID,
        expected_range_ordinal=2,
        expected_occurrence_ordinal=17,
    ).identity_bytes
    assert b"invalid_npi_structure" in quarantine.identity_bytes


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda value: value.__setitem__("npi", "redacted"), "wrapper"),
        (
            lambda value: value[UHC_PROVIDER_QUARANTINE_FIELD].__setitem__(
                "unexpected", 1
            ),
            "payload",
        ),
        (
            lambda value: value[UHC_PROVIDER_QUARANTINE_FIELD].__setitem__(
                "contract_id", "wrong"
            ),
            "identity",
        ),
        (
            lambda value: value[UHC_PROVIDER_QUARANTINE_FIELD].__setitem__(
                "reason", "wrong"
            ),
            "identity",
        ),
        (
            lambda value: value[UHC_PROVIDER_QUARANTINE_FIELD].__setitem__(
                "source_file_id", "bad"
            ),
            "identity",
        ),
        (
            lambda value: value[UHC_PROVIDER_QUARANTINE_FIELD].__setitem__(
                "record_sha256", "bad"
            ),
            "identity",
        ),
        (
            lambda value: value[UHC_PROVIDER_QUARANTINE_FIELD].__setitem__(
                "range_ordinal", True
            ),
            "range ordinal",
        ),
        (
            lambda value: value[UHC_PROVIDER_QUARANTINE_FIELD].__setitem__(
                "occurrence_ordinal", -1
            ),
            "occurrence ordinal",
        ),
    ),
)
def test_tombstone_rejects_shape_identity_and_ordinal_tampering(
    mutation,
    message,
) -> None:
    tombstone = copy.deepcopy(_tombstone())
    mutation(tombstone)

    with pytest.raises(UhcProviderQuarantineError, match=message):
        validate_provider_quarantine_fact(
            tombstone,
            expected_source_file_id=SOURCE_FILE_ID,
            expected_range_ordinal=2,
            expected_occurrence_ordinal=17,
        )


def test_tombstone_rejects_non_object_input() -> None:
    with pytest.raises(UhcProviderQuarantineError, match="not an object"):
        validate_provider_quarantine_fact(
            [],
            expected_source_file_id=SOURCE_FILE_ID,
            expected_range_ordinal=2,
            expected_occurrence_ordinal=17,
        )


@pytest.mark.parametrize(
    ("expected_source", "expected_range", "expected_occurrence"),
    (
        ("f" * 64, 2, 17),
        (SOURCE_FILE_ID, 3, 17),
        (SOURCE_FILE_ID, 2, 18),
    ),
)
def test_tombstone_rejects_expected_lineage_drift(
    expected_source,
    expected_range,
    expected_occurrence,
) -> None:
    with pytest.raises(UhcProviderQuarantineError, match="lineage mismatch"):
        validate_provider_quarantine_fact(
            _tombstone(),
            expected_source_file_id=expected_source,
            expected_range_ordinal=expected_range,
            expected_occurrence_ordinal=expected_occurrence,
        )


@pytest.mark.parametrize(
    ("provider_count", "expected_limit"),
    (
        (0, 0),
        (1, 1),
        (1_000_000, 1),
        (1_000_001, 2),
        (31_000_001, 32),
        (100_000_000, UHC_PROVIDER_QUARANTINE_MAX_COUNT),
    ),
)
def test_rate_ceiling_is_ceil_one_per_million_with_absolute_cap(
    provider_count,
    expected_limit,
) -> None:
    assert provider_quarantine_limit(provider_count) == expected_limit


@pytest.mark.parametrize("provider_count", (True, -1, 1.0, "1"))
def test_rate_ceiling_rejects_invalid_census(provider_count) -> None:
    with pytest.raises(UhcProviderQuarantineError, match="census"):
        provider_quarantine_limit(provider_count)


@pytest.mark.parametrize("provider_file_count", (True, -1, 1.0, "1"))
def test_catalog_ceiling_rejects_invalid_file_census(provider_file_count) -> None:
    with pytest.raises(UhcProviderQuarantineError, match="file census"):
        provider_quarantine_catalog_limit(provider_file_count)


def test_catalog_ceiling_returns_zero_for_empty_catalog() -> None:
    assert provider_quarantine_catalog_limit(0) == 0


@pytest.mark.parametrize("provider_file_count", (1, 3))
def test_catalog_ceiling_multiplies_validated_file_census(
    provider_file_count,
) -> None:
    assert provider_quarantine_catalog_limit(provider_file_count) == (
        provider_file_count * UHC_PROVIDER_QUARANTINE_MAX_COUNT
    )


def test_public_rejection_counts_are_aggregate_only() -> None:
    rejected = provider_quarantine_rejected_counts(_counter_map())

    assert rejected == {
        "invalid_npi_checksum": 2,
        "invalid_npi_checksum_individual_records": 1,
        "invalid_npi_checksum_facility_records": 1,
        "invalid_npi_checksum_address_rows": 3,
        "invalid_npi_checksum_provider_plan_rows": 4,
        "invalid_npi_structure": 0,
        "invalid_npi_structure_individual_records": 0,
        "invalid_npi_structure_facility_records": 0,
        "invalid_npi_structure_address_rows": 0,
        "invalid_npi_structure_provider_plan_rows": 0,
    }
    serialized = repr(rejected)
    assert SOURCE_FILE_ID not in serialized
    assert RECORD_SHA256 not in serialized


@pytest.mark.parametrize(
    "overrides",
    (
        {"invalid_npi_count": 3},
        {"invalid_npi_address_rows": 1},
        {"invalid_npi_provider_plan_rows": 1},
        {"invalid_npi_individual_records": 1_000_001},
        {"invalid_npi_facility_records": 2},
        {"invalid_npi_count": True},
        {"raw_provider_records": -1},
    ),
)
def test_public_rejection_counts_fail_closed_on_unbalanced_census(
    overrides,
) -> None:
    with pytest.raises(UhcProviderQuarantineError):
        provider_quarantine_rejected_counts(_counter_map(**overrides))


def test_zero_rejection_counts_collapse_to_empty_public_map() -> None:
    zero_count_by_field = {
        key: 0
        for key in _counter_map()
        if key.startswith("invalid_npi_") or key == "invalid_npi_count"
    }
    assert provider_quarantine_rejected_counts(
        _counter_map(**zero_count_by_field)
    ) == {}


def test_zero_rejection_count_rejects_nonzero_dimension() -> None:
    with pytest.raises(UhcProviderQuarantineError, match="do not balance"):
        provider_quarantine_rejected_counts(
            _counter_map(
                invalid_npi_count=0,
                invalid_npi_individual_records=0,
                invalid_npi_facility_records=0,
                invalid_npi_address_rows=1,
                invalid_npi_provider_plan_rows=1,
            )
        )


def test_structural_subset_is_publicly_distinct_and_exactly_balanced() -> None:
    counters = _counter_map(
        invalid_npi_structure_count=1,
        invalid_npi_structure_individual_records=0,
        invalid_npi_structure_facility_records=1,
        invalid_npi_structure_address_rows=2,
        invalid_npi_structure_provider_plan_rows=3,
    )

    rejected = provider_quarantine_rejected_counts(counters)

    assert rejected["invalid_npi_checksum"] == 1
    assert rejected["invalid_npi_structure"] == 1
    assert rejected["invalid_npi_structure_facility_records"] == 1
    assert rejected["invalid_npi_structure_address_rows"] == 2
    assert rejected["invalid_npi_structure_provider_plan_rows"] == 3
    assert provider_quarantine_rejected_totals(rejected, 2) == {
        "individual_records": 1,
        "facility_records": 1,
        "address_rows": 3,
        "provider_plan_rows": 4,
    }


def test_structural_native_counter_group_must_be_complete() -> None:
    with pytest.raises(UhcProviderQuarantineError, match="incomplete"):
        provider_quarantine_rejected_counts(
            _counter_map(invalid_npi_structure_count=1)
        )
