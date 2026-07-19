from __future__ import annotations

from copy import deepcopy

import pytest

from process.ptg_parts import ptg2_provider_quarantine as quarantine
from process.ptg_parts import ptg2_source_witness_contract as witness_contract
from tests.test_ptg2_batch_candidate_audit import _source_witness


def _quarantine_validation_case(mutation):
    canonical_by_field = quarantine.provider_identifier_quarantine_payload(
        {-2: 1, -1: 1}
    )
    first_entry_by_field = canonical_by_field["entries"][0]
    second_entry_by_field = canonical_by_field["entries"][1]
    case_by_name = {
        "not_mapping": [],
        "fields": {**canonical_by_field, "unexpected": True},
        "contract": {**canonical_by_field, "contract": "wrong"},
        "entries_type": {**canonical_by_field, "entries": {}},
        "entry_shape": {
            **canonical_by_field,
            "entries": [
                {**first_entry_by_field, "unexpected": True},
                second_entry_by_field,
            ],
        },
        "value_type": {
            **canonical_by_field,
            "entries": [
                {**first_entry_by_field, "value": -2},
                second_entry_by_field,
            ],
        },
        "value_invalid": {
            **canonical_by_field,
            "entries": [
                {**first_entry_by_field, "value": "not-an-int"},
                second_entry_by_field,
            ],
        },
        "value_noncanonical": {
            **canonical_by_field,
            "entries": [
                {**first_entry_by_field, "value": "-02"},
                second_entry_by_field,
            ],
        },
        "unordered": {
            **canonical_by_field,
            "entries": [second_entry_by_field, first_entry_by_field],
        },
    }
    return case_by_name[mutation]


def _source_witness_with(**overrides):
    manifest_by_field = deepcopy(_source_witness())
    manifest_by_field.update(overrides)
    return manifest_by_field


def _source_witness_validation_case(mutation):
    case_by_name = {
        "not_mapping": ([], 1),
        "contract": (_source_witness_with(contract="wrong"), 1),
        "invalid_int": (_source_witness_with(source_count=True), 1),
        "invalid_digest": (
            _source_witness_with(sample_digest="z" * 64),
            1,
        ),
        "zero_population": (
            _source_witness_with(queryable_occurrence_population_count=0),
            1,
        ),
        "unqueryable_exceeds_emitted": (
            _source_witness_with(unqueryable_rate_row_count=3),
            1,
        ),
        "incomplete_counts": (_source_witness_with(record_count=2), 1),
        "source_count": (_source_witness_with(), 2),
        "payload_bound": (
            _source_witness_with(
                payload_bytes=(
                    witness_contract.PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES + 1
                )
            ),
            1,
        ),
        "evidence_bound": (
            _source_witness_with(evidence_dictionary_count=7),
            1,
        ),
    }
    return case_by_name[mutation]


@pytest.mark.parametrize(
    ("counts_by_identifier", "message"),
    (
        ({"not-an-int": 1}, "not an int64"),
        ({2**63: 1}, "not an int64"),
        ({123456789: 0}, "count is invalid"),
        ({123456789: True}, "count is invalid"),
    ),
)
def test_quarantine_payload_rejects_invalid_identifiers_and_counts(
    counts_by_identifier,
    message,
):
    with pytest.raises(ValueError, match=message):
        quarantine.provider_identifier_quarantine_payload(counts_by_identifier)


def test_quarantine_payload_enforces_distinct_and_occurrence_bounds():
    too_many_counts_by_identifier = {
        -(identifier + 1): 1
        for identifier in range(quarantine._MAX_DISTINCT_VALUES + 1)
    }
    with pytest.raises(ValueError, match="exceeds 1024 distinct values"):
        quarantine.provider_identifier_quarantine_payload(
            too_many_counts_by_identifier
        )

    with pytest.raises(ValueError, match="occurrence count overflows"):
        quarantine.provider_identifier_quarantine_payload(
            {-1: 2**63, -2: 2**63}
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("not_mapping", "must be an object"),
        ("fields", "fields are incompatible"),
        ("contract", "contract is incompatible"),
        ("entries_type", "entries must be an array"),
        ("entry_shape", "entry is incompatible"),
        ("value_type", "value must be text"),
        ("value_invalid", "value is invalid"),
        ("value_noncanonical", "value is not canonical"),
        ("unordered", "values are not ordered"),
    ),
)
def test_quarantine_validation_rejects_noncanonical_shapes(mutation, message):
    payload = _quarantine_validation_case(mutation)

    with pytest.raises(ValueError, match=message):
        quarantine.validate_provider_identifier_quarantine(payload)


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("contract", "wrong"),
        ("occurrence_count", -1),
        ("occurrence_count", 2**64),
        ("distinct_value_count", -1),
        ("distinct_value_count", 1025),
        ("distinct_value_count", 2),
        ("sha256", "not-a-digest"),
        ("sha256", "z" * 64),
    ),
)
def test_quarantine_evidence_rejects_invalid_bounds(field_name, invalid_value):
    evidence_by_field = quarantine.provider_identifier_quarantine_evidence(
        quarantine.provider_identifier_quarantine_payload({-1: 1})
    )
    evidence_by_field[field_name] = invalid_value

    with pytest.raises(ValueError, match="evidence is invalid"):
        quarantine.validate_provider_identifier_quarantine_evidence(
            evidence_by_field
        )


def test_quarantine_evidence_rejects_wrong_field_set():
    with pytest.raises(ValueError, match="evidence is incompatible"):
        quarantine.validate_provider_identifier_quarantine_evidence({})


@pytest.mark.parametrize(
    ("occurrence_population", "provider_population"),
    ((-1, 0), (0, -1)),
)
def test_source_witness_targets_reject_negative_populations(
    occurrence_population,
    provider_population,
):
    with pytest.raises(RuntimeError, match="population is invalid"):
        witness_contract.source_witness_targets(
            occurrence_population=occurrence_population,
            provider_population=provider_population,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("not_mapping", "missing source witness manifest"),
        ("contract", "incompatible source witness contract"),
        ("invalid_int", "invalid source witness source_count"),
        ("invalid_digest", "invalid source witness sample_digest"),
        ("zero_population", "no queryable occurrence population"),
        ("unqueryable_exceeds_emitted", "unqueryable rate count is invalid"),
        ("incomplete_counts", "incomplete source witness coverage"),
        ("source_count", "source_count mismatch"),
        ("payload_bound", "payload exceeds its bound"),
        ("evidence_bound", "evidence dictionary count is invalid"),
    ),
)
def test_source_witness_manifest_invalid_contract_matrix(mutation, message):
    manifest_by_field, expected_source_count = _source_witness_validation_case(
        mutation
    )

    with pytest.raises((RuntimeError, ValueError), match=message):
        witness_contract.validate_source_witness_manifest(
            manifest_by_field,
            expected_source_count=expected_source_count,
        )
