# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused proof for pure Flex Practitioner materialization."""

import dataclasses
import datetime
import importlib

import pytest

from db.models import ProviderDirectoryOrganization
from process import uhc_flex_practitioner_materialization as materialization
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_NPI_SYSTEM,
)
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from process.uhc_flex_practitioner_query import (
    UHCFlexPractitionerQueryResult,
    validate_uhc_flex_practitioner_search_bundle,
)


REQUESTED_NPI = 1234567893
OTHER_NPI = 1588616783
DATASET_ID = "dataset-flex-practitioner-candidate"
RUN_ID = "pdufpr_" + "1" * 48
PROJECTION_AS_OF = "2026-08-10"
provider_directory_fhir = importlib.import_module("process.provider_directory_fhir")


def _practitioner(resource_id: str, **additional_fields) -> dict:
    return {
        "resourceType": "Practitioner",
        "id": resource_id,
        "meta": {
            "lastUpdated": "2026-08-09T12:34:56Z",
            "profile": [
                "http://hl7.org/fhir/us/davinci-pdex-plan-net/"
                "StructureDefinition/plannet-Practitioner"
            ],
        },
        "identifier": [
            {
                "system": UHC_FLEX_OFFICIAL_NPI_SYSTEM,
                "value": str(REQUESTED_NPI),
            }
        ],
        "active": True,
        "name": [
            {
                "use": "official",
                "text": "Dr Avery Example",
                "family": "Example",
                "given": ["Avery"],
            }
        ],
        "gender": "female",
        "birthDate": "1980-06-15",
        "telecom": [
            {
                "system": "email",
                "value": "avery@example.test",
                "use": "work",
            },
            {
                "system": "url",
                "value": "https://example.test/avery",
                "use": "work",
            },
        ],
        "qualification": [
            {
                "identifier": [
                    {
                        "system": "https://example.test/license",
                        "value": "LIC-123",
                    }
                ],
                "code": {
                    "coding": [
                        {
                            "system": "https://example.test/license-type",
                            "code": "MD",
                            "display": "Physician",
                        }
                    ],
                    "text": "Medical license",
                },
                "period": {"start": "2005-07-01"},
                "issuer": {"reference": "Organization/licensing-board"},
            }
        ],
        "communication": [
            {
                "language": {
                    "coding": [
                        {
                            "system": "urn:ietf:bcp:47",
                            "code": "es",
                            "display": "Spanish",
                        }
                    ],
                    "text": "Spanish",
                },
                "preferred": True,
            }
        ],
        **additional_fields,
    }


def _result(*resources: dict) -> UHCFlexPractitionerQueryResult:
    return validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": len(resources),
            "entry": [{"resource": resource} for resource in resources],
        },
    )


def _materialize(
    result: UHCFlexPractitionerQueryResult,
    **overrides,
):
    arguments = {
        "dataset_id": DATASET_ID,
        "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "run_id": RUN_ID,
        "semantic_projection_as_of": PROJECTION_AS_OF,
    }
    arguments.update(overrides)
    return materialization.materialize_uhc_flex_practitioner_result(
        result,
        **arguments,
    )


def _patched_parser(monkeypatch, mutate):
    original_parser = materialization.parse_fhir_resource

    def parse_with_mutation(*args, **kwargs):
        parsed = original_parser(*args, **kwargs)
        assert parsed is not None
        model, row_by_field = parsed
        copied_row = dict(row_by_field)
        return mutate(model, copied_row)

    monkeypatch.setattr(
        materialization,
        "parse_fhir_resource",
        parse_with_mutation,
    )


def test_materializes_rich_practitioner_into_semantic_v3_dataset_row():
    result = _result(_practitioner("practitioner-rich"))

    rows = _materialize(result)

    assert len(rows) == 1
    row = rows[0]
    assert row.requested_npi == REQUESTED_NPI
    assert row.resource_id == "practitioner-rich"
    assert (
        row.acquired_resource_sha256
        == dict(result.resource_sha256_by_id)["practitioner-rich"]
    )
    dataset_resource = row.dataset_resource
    assert dataset_resource == {
        **dataset_resource,
        "dataset_id": DATASET_ID,
        "resource_type": "Practitioner",
        "resource_id": "practitioner-rich",
        "acquired_resource_sha256": row.acquired_resource_sha256,
    }
    payload = dataset_resource["payload_json"]
    assert dataset_resource["payload_hash"] == (
        resource_payload_sha256_for_contract(
            payload,
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        )
    )
    assert payload["full_name"] == "Dr Avery Example"
    assert payload["administrative_gender"] == "female"
    assert payload["communications"] == [
        {
            "codes": [
                {
                    "code": "es",
                    "display": "Spanish",
                    "system": "urn:ietf:bcp:47",
                    "text": "Spanish",
                }
            ],
            "preferred": True,
            "text": "Spanish",
        }
    ]
    assert payload["telecom"] == [
        {
            "system": "email",
            "use": "work",
            "value": "avery@example.test",
        },
        {
            "system": "url",
            "use": "work",
            "value": "https://example.test/avery",
        },
    ]
    assert payload["qualification_codes"][0]["code"] == "MD"
    assert payload["qualifications"] == [
        {
            "code_codes": [
                {
                    "code": "MD",
                    "display": "Physician",
                    "system": "https://example.test/license-type",
                    "text": "Medical license",
                }
            ],
            "code_text": "Medical license",
            "identifiers": [
                {
                    "system": "https://example.test/license",
                    "value": "LIC-123",
                }
            ],
            "issuer_ref": "Organization/licensing-board",
            "period_start": "2005-07-01",
        }
    ]
    assert payload["age_years"] == 46
    assert payload["age_as_of"] == PROJECTION_AS_OF
    assert payload["years_of_practice"] == 21
    assert payload["years_of_practice_as_of"] == PROJECTION_AS_OF
    assert {
        "source_id",
        "last_seen_run_id",
        "observed_at",
        "updated_at",
    }.isdisjoint(payload)


def test_materialized_row_does_not_expose_mutable_internal_mapping():
    row = _materialize(_result(_practitioner("practitioner-frozen")))[0]

    returned_mapping = row.dataset_resource
    returned_mapping["payload_json"]["active"] = False

    assert row.dataset_resource["payload_json"]["active"] is True
    with pytest.raises(dataclasses.FrozenInstanceError):
        row.resource_id = "changed"


def test_unmatched_result_materializes_no_dataset_rows():
    assert _materialize(_result()) == ()


def test_semantic_payload_and_hash_ignore_parser_observation_clock(monkeypatch):
    result = _result(_practitioner("practitioner-clock"))
    observed_times = iter(
        (
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            datetime.datetime(2031, 12, 31, tzinfo=datetime.UTC),
        )
    )
    monkeypatch.setattr(provider_directory_fhir, "_now", lambda: next(observed_times))

    first = _materialize(result)[0].dataset_resource
    second = _materialize(result)[0].dataset_resource

    assert first == second
    assert first["payload_json"]["age_as_of"] == PROJECTION_AS_OF
    assert first["payload_json"]["years_of_practice_as_of"] == (PROJECTION_AS_OF)


@pytest.mark.parametrize(
    "projection_as_of",
    [None, datetime.date(2026, 8, 10), "2026-8-10", "2026-02-30", " 2026-08-10"],
)
def test_rejects_noncanonical_semantic_projection_date(projection_as_of):
    with pytest.raises(
        materialization.UHCFlexPractitionerMaterializationError
    ) as error_info:
        _materialize(
            _result(),
            semantic_projection_as_of=projection_as_of,
        )

    assert error_info.value.code == "semantic_projection_as_of_invalid"


def test_rejects_source_identity_drift():
    with pytest.raises(
        materialization.UHCFlexPractitionerMaterializationError
    ) as error_info:
        _materialize(_result(), source_id="pdfhir_drift")

    assert error_info.value.code == "source_drift"


def test_rejects_parser_model_drift(monkeypatch):
    _patched_parser(
        monkeypatch,
        lambda _model, row: (ProviderDirectoryOrganization, row),
    )

    with pytest.raises(
        materialization.UHCFlexPractitionerMaterializationError
    ) as error_info:
        _materialize(_result(_practitioner("practitioner-model")))

    assert error_info.value.code == "resource_model_drift"


def test_rejects_normalized_cross_npi_drift(monkeypatch):
    def replace_npi(model, row):
        row["npi"] = OTHER_NPI
        return model, row

    _patched_parser(monkeypatch, replace_npi)

    with pytest.raises(
        materialization.UHCFlexPractitionerMaterializationError
    ) as error_info:
        _materialize(_result(_practitioner("practitioner-npi")))

    assert error_info.value.code == "resource_npi_drift"


def test_rejects_normalized_resource_id_drift(monkeypatch):
    def replace_resource_id(model, row):
        row["resource_id"] = "another-id"
        return model, row

    _patched_parser(monkeypatch, replace_resource_id)

    with pytest.raises(
        materialization.UHCFlexPractitionerMaterializationError
    ) as error_info:
        _materialize(_result(_practitioner("practitioner-id")))

    assert error_info.value.code == "resource_id_drift"


def test_rejects_acquired_resource_hash_drift(monkeypatch):
    monkeypatch.setattr(
        UHCFlexPractitionerQueryResult,
        "resource_sha256_by_id",
        property(lambda self: ((self.resource_ids[0], "0" * 64),)),
    )

    with pytest.raises(
        materialization.UHCFlexPractitionerMaterializationError
    ) as error_info:
        _materialize(_result(_practitioner("practitioner-raw")))

    assert error_info.value.code == "raw_content_drift"


def test_rejects_conflicting_semantic_hash_collision(monkeypatch):
    monkeypatch.setattr(
        materialization,
        "resource_payload_sha256_for_contract",
        lambda _payload, _contract: "a" * 64,
    )

    with pytest.raises(
        materialization.UHCFlexPractitionerMaterializationError
    ) as error_info:
        _materialize(
            _result(
                _practitioner("practitioner-a"),
                _practitioner("practitioner-b", active=False),
            )
        )

    assert error_info.value.code == "semantic_collision"
