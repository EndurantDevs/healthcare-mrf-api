# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused edge coverage for connector temporal and identifier contracts."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace

import pytest

from process.tin_npi_connector_source import (
    _canonical_source_ids,
    _source_bitmap,
    _strict_hash_hex,
    _strict_optional_text,
    _strict_string_tuple,
)
from process.tin_npi_connector_support import (
    _MalformedFhirIdentifierPeriod,
    _UnresolvedFhirIdentifierPeriod,
    TinNpiConnectorError,
)
from process.tin_npi_connector_temporal import (
    _as_utc_datetime,
    _exact_fhir_datetime_bound,
    _fhir_period_bound,
    _has_identifier_match,
    _identifier_type_codings,
    _is_identifier_effective,
    _normalize_npi,
    _partial_date_bound,
    canonical_evidence_as_of,
)
from tests.tin_npi_connector_unit_support import (
    NPI_SYSTEM,
    REVIEWED_TAX_AS_EIN_POLICY,
    REVIEWED_TAX_AS_EIN_RULE,
    TYPE_SYSTEM,
    fhir_dataset,
    source_vector,
)

UTC = dt.timezone.utc


def test_identifier_codings_ignore_untyped_entries_and_keep_sorted_unique_pairs():
    identifier_by_field = {
        "type_codes": [
            None,
            {"system": TYPE_SYSTEM, "code": "TAX"},
            {"system": TYPE_SYSTEM, "code": "TAX"},
            {"system": TYPE_SYSTEM, "code": 7},
            {"system": 7, "code": "NPI"},
            {"system": NPI_SYSTEM, "code": "NPI"},
        ]
    }

    assert _identifier_type_codings(identifier_by_field) == (
        (NPI_SYSTEM, "NPI"),
        (TYPE_SYSTEM, "TAX"),
    )
    assert _identifier_type_codings({"type": {"coding": "not-a-list"}}) == ()
    assert _has_identifier_match(
        identifier_by_field,
        systems=(),
        type_codings=((TYPE_SYSTEM, "TAX"),),
    )


def test_datetime_normalization_accepts_date_and_naive_datetime_as_utc():
    assert _as_utc_datetime(dt.date(2026, 7, 27)) == dt.datetime(
        2026,
        7,
        27,
        tzinfo=UTC,
    )
    assert canonical_evidence_as_of(dt.datetime(2026, 7, 27, 12, 30)) == (
        "2026-07-27T12:30:00.000000Z"
    )


@pytest.mark.parametrize(
    "candidate",
    (
        "2026-07-27T00:00:00+00:00",
        "not-a-datetimeZ",
        "2026-07-27T00:00:00Z",
        object(),
    ),
)
def test_evidence_cutoff_rejects_noncanonical_or_non_temporal_values(candidate):
    with pytest.raises(TinNpiConnectorError, match="evidence cutoff is invalid"):
        canonical_evidence_as_of(candidate)


@pytest.mark.parametrize(
    ("candidate", "upper", "expected"),
    (
        ("2026", False, (dt.datetime(2026, 1, 1, tzinfo=UTC), True)),
        ("2026", True, (dt.datetime(2027, 1, 1, tzinfo=UTC), False)),
        ("2026-07", False, (dt.datetime(2026, 7, 1, tzinfo=UTC), True)),
        ("2026-07", True, (dt.datetime(2026, 8, 1, tzinfo=UTC), False)),
        ("2026-12", True, (dt.datetime(2027, 1, 1, tzinfo=UTC), False)),
    ),
)
def test_partial_date_bounds_preserve_fhir_precision(candidate, upper, expected):
    assert _partial_date_bound(candidate, upper=upper) == expected


@pytest.mark.parametrize("candidate", ("2026-13", "2026-02-31"))
def test_partial_date_bounds_reject_impossible_calendar_values(candidate):
    with pytest.raises(
        _MalformedFhirIdentifierPeriod,
        match="FHIR identifier period is malformed",
    ):
        _partial_date_bound(candidate, upper=True)


@pytest.mark.parametrize(
    ("candidate", "expected"),
    (
        (
            "2026-07-27T12:00:00+02:30",
            dt.datetime(2026, 7, 27, 9, 30, tzinfo=UTC),
        ),
        (
            "2026-07-27T12:00:00-02:30",
            dt.datetime(2026, 7, 27, 14, 30, tzinfo=UTC),
        ),
        (
            "2026-07-27T23:59:60Z",
            dt.datetime(2026, 7, 28, tzinfo=UTC),
        ),
        (
            "2026-07-27T00:00:00.0000001Z",
            dt.datetime(2026, 7, 27, 0, 0, 0, 1, tzinfo=UTC),
        ),
    ),
)
def test_exact_datetime_bounds_normalize_offsets_and_submicrosecond_starts(
    candidate,
    expected,
):
    assert _exact_fhir_datetime_bound(candidate, upper=False) == (expected, True)


def test_exact_datetime_bound_rejects_impossible_calendar_date():
    with pytest.raises(
        _MalformedFhirIdentifierPeriod,
        match="FHIR identifier period is malformed",
    ):
        _exact_fhir_datetime_bound("2026-02-30T00:00:00Z", upper=False)


@pytest.mark.parametrize("candidate", (7, "", " 2026-07-27"))
def test_period_bound_rejects_non_string_empty_or_padded_values(candidate):
    with pytest.raises(
        _MalformedFhirIdentifierPeriod,
        match="FHIR identifier period is malformed",
    ):
        _fhir_period_bound(candidate, upper=False)


def test_effective_identifier_requires_cutoff_for_period_and_rejects_inversion():
    with pytest.raises(
        _UnresolvedFhirIdentifierPeriod,
        match="period cannot be resolved",
    ):
        _is_identifier_effective(
            {"period": {"start": "2026-01-01"}},
            observed_at=None,
            policy=REVIEWED_TAX_AS_EIN_RULE,
        )

    with pytest.raises(
        _MalformedFhirIdentifierPeriod,
        match="FHIR identifier period is malformed",
    ):
        _is_identifier_effective(
            {"period": {"start": "2026-07-28", "end": "2026-07-26"}},
            observed_at=dt.datetime(2026, 7, 27, tzinfo=UTC),
            policy=REVIEWED_TAX_AS_EIN_RULE,
        )


def test_effective_identifier_honors_open_start_and_end_ranges():
    observation = dt.datetime(2026, 7, 27, 12, tzinfo=UTC)

    assert _is_identifier_effective(
        {"period_end": "2026-07-28"},
        observed_at=observation,
        policy=REVIEWED_TAX_AS_EIN_RULE,
    )
    assert not _is_identifier_effective(
        {"period_start": "2026-07-28"},
        observed_at=observation,
        policy=REVIEWED_TAX_AS_EIN_RULE,
    )


@pytest.mark.parametrize(
    "candidate", (1234567893, "１２３４５６７８９３", "12345", "123x4567893")
)
def test_npi_normalization_rejects_non_string_unicode_short_or_punctuated_values(
    candidate,
):
    with pytest.raises(TinNpiConnectorError, match="NPI is malformed"):
        _normalize_npi(candidate)


def test_npi_normalization_accepts_reviewed_ascii_separators():
    assert _normalize_npi(" 123-456/789.3 ") == 1234567893


@pytest.mark.parametrize("candidate", (None, "A" * 64))
def test_source_hash_helper_rejects_non_string_or_noncanonical_digest(candidate):
    with pytest.raises(TinNpiConnectorError, match="dataset hash is invalid"):
        _strict_hash_hex(candidate, "dataset hash")


def test_source_optional_text_and_sorted_tuple_helpers_preserve_exact_values():
    assert _strict_optional_text(None, "metadata", limit=16) is None
    assert _strict_optional_text("dataset-a", "metadata", limit=16) == "dataset-a"
    assert _strict_string_tuple(
        ("Location", "Organization"), "resources", limit=64
    ) == (
        "Location",
        "Organization",
    )


@pytest.mark.parametrize(
    "candidate", (["Organization"], ("Organization", "Organization"))
)
def test_source_sorted_tuple_helper_rejects_wrong_container_or_duplicates(candidate):
    with pytest.raises(TinNpiConnectorError, match="resources is invalid"):
        _strict_string_tuple(candidate, "resources", limit=64)


@pytest.mark.parametrize("source_ids", ("source-a", 7, ()))
def test_source_ordinal_inputs_reject_scalar_noniterable_or_empty_values(source_ids):
    with pytest.raises(
        TinNpiConnectorError,
        match="connector source ordinal map is invalid",
    ):
        _canonical_source_ids(source_ids)


def test_source_bitmap_rejects_members_outside_authenticated_ordinal_map():
    with pytest.raises(
        TinNpiConnectorError,
        match="outside the source ordinal map",
    ):
        _source_bitmap(("source-b",), source_ordinal_map=("source-a",))


@pytest.mark.parametrize(
    "dataset_change",
    (
        {"dataset_hash": "A" * 64},
        {"selected_resources": ["Organization"]},
        {"selected_resources": ("Organization", "Organization")},
        {"is_current": 1},
        {"promote_on_cutover": 1},
        {"resource_count": True},
        {"resource_count": -1},
        {"organization_resource_count": True},
        {"organization_resource_count": -1},
        {"organization_resource_count": 2},
    ),
)
def test_dataset_fence_rejects_malformed_identity_lifecycle_or_counts(dataset_change):
    bounded_dataset = replace(fhir_dataset(), resource_count=1)
    with pytest.raises(TinNpiConnectorError):
        replace(bounded_dataset, **dataset_change)


@pytest.mark.parametrize(
    "vector_change",
    (
        {"schema_version": 99},
        {"lookup_schema_version": 99},
        {"lookup_contract_id": "healthporta.test.lookup.invalid"},
        {"fhir_datasets": []},
        {"fhir_datasets": ()},
        {"fhir_datasets": (object(),)},
        {"input_relations": []},
        {"input_relations": ()},
        {"input_relations": (object(),)},
        {"token_policies": []},
        {"token_policies": ()},
        {"token_policies": (object(),)},
        {"identifier_policy": None},
    ),
)
def test_source_vector_rejects_wrong_contract_or_container_types(vector_change):
    with pytest.raises(TinNpiConnectorError):
        replace(source_vector(), **vector_change)


def test_source_vector_rejects_duplicate_dataset_and_token_policy_identities():
    vector = source_vector()
    dataset = vector.fhir_datasets[0]
    token_policy = vector.token_policies[0]

    with pytest.raises(TinNpiConnectorError, match="datasets are duplicated"):
        replace(vector, fhir_datasets=(dataset, dataset))
    with pytest.raises(TinNpiConnectorError, match="token policies are duplicated"):
        replace(vector, token_policies=(token_policy, token_policy))


def test_source_vector_rejects_identifier_policy_scope_drift():
    changed_rule = replace(
        REVIEWED_TAX_AS_EIN_RULE,
        rule_id="healthporta.test.fhir-tax-as-ein.source-a.v2",
    )
    changed_policy = replace(
        REVIEWED_TAX_AS_EIN_POLICY,
        rules=(changed_rule,),
    )

    with pytest.raises(TinNpiConnectorError, match="policy scope is inconsistent"):
        replace(source_vector(), identifier_policy=changed_policy)
