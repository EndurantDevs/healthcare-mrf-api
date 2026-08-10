# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for exact Flex Practitioner materialization."""

from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace

import pytest

from db.models import ProviderDirectoryPractitioner
from process import uhc_flex_practitioner_materialization as materialization
from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from process.uhc_flex_official_cohort_contract import UHC_FLEX_OFFICIAL_NPI_SYSTEM
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID
from process.uhc_flex_practitioner_query import (
    UHCFlexPractitionerQueryResult,
    validate_uhc_flex_practitioner_search_bundle,
)
from process.uhc_flex_practitioner_store_contract import (
    UHCFlexPractitionerResourceRow,
)


REQUESTED_NPI = 1234567893
DATASET_ID = "dataset-boundary"
RUN_ID = "run-boundary"
PROJECTION_DATE = "2026-08-10"


def _practitioner(resource_id="practitioner-a"):
    return {
        "resourceType": "Practitioner",
        "id": resource_id,
        "identifier": [
            {"system": UHC_FLEX_OFFICIAL_NPI_SYSTEM, "value": str(REQUESTED_NPI)}
        ],
        "active": True,
        "name": [{"family": "Boundary", "given": ["Test"]}],
    }


def _result(resource=None):
    resources = [] if resource is None else [resource]
    return validate_uhc_flex_practitioner_search_bundle(
        REQUESTED_NPI,
        {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": len(resources),
            "entry": [{"resource": value} for value in resources],
        },
    )


def _materialize(result):
    return materialization.materialize_uhc_flex_practitioner_result(
        result,
        dataset_id=DATASET_ID,
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        run_id=RUN_ID,
        semantic_projection_as_of=PROJECTION_DATE,
    )


def _valid_row():
    return _materialize(_result(_practitioner()))[0]


def _stored_resource(resource=None):
    resource_by_field = resource or _practitioner()
    payload_text = json.dumps(
        resource_by_field,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return UHCFlexPractitionerResourceRow(
        requested_npi=REQUESTED_NPI,
        resource_id=resource_by_field["id"],
        payload_sha256=hashlib.sha256(payload_text.encode()).hexdigest(),
        payload_json_text=payload_text,
    )


def _stored_materialize(stored_resource):
    return materialization.materialize_uhc_flex_practitioner_stored_resource(
        stored_resource,
        dataset_id=DATASET_ID,
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        run_id=RUN_ID,
        semantic_projection_as_of=PROJECTION_DATE,
    )


def test_materialization_error_falls_back_without_echoing_unknown_code():
    error = materialization.UHCFlexPractitionerMaterializationError("provider-secret")

    assert error.code == "result_invalid"
    assert "provider-secret" not in str(error)


def test_canonical_json_and_projection_date_reject_noncanonical_values():
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError):
        materialization._canonical_json({"not-json": {object()}})
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError) as error_info:
        materialization._canonical_projection_date("2026-W33-1")
    assert error_info.value.code == "semantic_projection_as_of_invalid"


@pytest.mark.parametrize(
    ("field_name", "value", "expected_code"),
    [
        ("dataset_id", None, "dataset_id_invalid"),
        ("dataset_id", " padded", "dataset_id_invalid"),
        ("run_id", "", "run_id_invalid"),
        ("run_id", "r" * 65, "run_id_invalid"),
    ],
)
def test_materialization_context_rejects_unsafe_text(field_name, value, expected_code):
    arguments_by_name = {
        "dataset_id": DATASET_ID,
        "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "run_id": RUN_ID,
        "semantic_projection_as_of": PROJECTION_DATE,
        "requested_npi": REQUESTED_NPI,
    }
    arguments_by_name[field_name] = value

    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError) as error_info:
        materialization._materialization_context(**arguments_by_name)
    assert error_info.value.code == expected_code


def test_normalized_payload_and_hash_helpers_translate_failures(monkeypatch):
    monkeypatch.setattr(
        materialization,
        "canonical_practitioner_payload",
        lambda _payload: (_ for _ in ()).throw(ValueError("secret")),
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError):
        materialization._normalized_payload({})

    monkeypatch.setattr(
        materialization,
        "resource_payload_sha256_for_contract",
        lambda *_args: (_ for _ in ()).throw(ValueError("secret")),
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError):
        materialization._dataset_resource_mapping(
            dataset_id=DATASET_ID,
            resource_id="practitioner-a",
            payload_by_field={},
            acquired_resource_sha256="a" * 64,
        )


def test_dataset_resource_mapping_requires_a_sha256(monkeypatch):
    monkeypatch.setattr(
        materialization,
        "resource_payload_sha256_for_contract",
        lambda *_args: "not-a-hash",
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError):
        materialization._dataset_resource_mapping(
            dataset_id=DATASET_ID,
            resource_id="practitioner-a",
            payload_by_field={},
            acquired_resource_sha256="a" * 64,
        )


def test_materialized_row_rejects_invalid_identity_json_and_payload():
    row = _valid_row()
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError):
        replace(row, requested_npi=1234567890)
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError):
        replace(row, _dataset_resource_json="{")
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError):
        replace(row, resource_id="bad/id")

    resource_mapping = row.dataset_resource
    resource_mapping["payload_json"]["resource_id"] = "other-id"
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError):
        replace(row, _dataset_resource_json=materialization._canonical_json(resource_mapping))


def test_materialized_row_rechecks_semantic_hash(monkeypatch):
    row = _valid_row()
    monkeypatch.setattr(
        materialization,
        "resource_payload_sha256_for_contract",
        lambda *_args: (_ for _ in ()).throw(ValueError("secret")),
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError):
        replace(row)

    monkeypatch.undo()
    resource_mapping = row.dataset_resource
    resource_mapping["payload_hash"] = "0" * 64
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError):
        replace(row, _dataset_resource_json=materialization._canonical_json(resource_mapping))


def test_normalized_result_rejects_invalid_expected_resource_id():
    query_result = _result(_practitioner())
    context = materialization._materialization_context(
        dataset_id=DATASET_ID,
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        run_id=RUN_ID,
        semantic_projection_as_of=PROJECTION_DATE,
        requested_npi=REQUESTED_NPI,
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError) as error_info:
        materialization._normalized_result_payload(
            query_result,
            context,
            "bad/id",
            _practitioner(),
            {},
        )
    assert error_info.value.code == "resource_id_invalid"


@pytest.mark.parametrize("parsed_resource", [None, (), (ProviderDirectoryPractitioner,)])
def test_materialization_rejects_invalid_parser_result_shape(monkeypatch, parsed_resource):
    monkeypatch.setattr(
        materialization,
        "parse_fhir_resource",
        lambda *_args, **_kwargs: parsed_resource,
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError) as error_info:
        _materialize(_result(_practitioner()))
    assert error_info.value.code == "resource_model_drift"


def test_materialization_rejects_nonmapping_or_wrong_source_parser_rows(monkeypatch):
    monkeypatch.setattr(
        materialization,
        "parse_fhir_resource",
        lambda *_args, **_kwargs: (ProviderDirectoryPractitioner, []),
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError) as error_info:
        _materialize(_result(_practitioner()))
    assert error_info.value.code == "normalized_payload_invalid"

    monkeypatch.setattr(
        materialization,
        "parse_fhir_resource",
        lambda *_args, **_kwargs: (
            ProviderDirectoryPractitioner,
            {
                "resource_id": "practitioner-a",
                "npi": REQUESTED_NPI,
                "source_id": "wrong",
                "last_seen_run_id": RUN_ID,
            },
        ),
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError) as error_info:
        _materialize(_result(_practitioner()))
    assert error_info.value.code == "source_drift"


def test_materialized_result_rejects_resource_projection_count_drift(monkeypatch):
    monkeypatch.setattr(
        UHCFlexPractitionerQueryResult,
        "resource_sha256_by_id",
        property(lambda _self: ()),
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError) as error_info:
        _materialize(_result(_practitioner()))
    assert error_info.value.code == "result_invalid"


def test_deduplication_rejects_same_id_with_changed_semantics(monkeypatch):
    materialized_rows = iter(
        (
            SimpleNamespace(dataset_resource={"payload_hash": "a", "payload_json": {"v": 1}}),
            SimpleNamespace(dataset_resource={"payload_hash": "b", "payload_json": {"v": 2}}),
        )
    )
    monkeypatch.setattr(
        materialization,
        "_materialized_practitioner_row",
        lambda *_args, **_kwargs: next(materialized_rows),
    )
    fake_result = SimpleNamespace(
        resource_ids=("same-id", "same-id"),
        resource_count=2,
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError) as error_info:
        materialization._deduplicated_materialized_rows(
            fake_result,
            object(),
            ({}, {}),
            {},
        )
    assert error_info.value.code == "semantic_collision"


def test_deduplication_rechecks_exact_result_count(monkeypatch):
    monkeypatch.setattr(
        materialization,
        "_materialized_practitioner_row",
        lambda *_args, **_kwargs: SimpleNamespace(
            dataset_resource={"payload_hash": "a", "payload_json": {"v": 1}}
        ),
    )
    fake_result = SimpleNamespace(resource_ids=("one",), resource_count=2)
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError) as error_info:
        materialization._deduplicated_materialized_rows(fake_result, object(), ({},), {})
    assert error_info.value.code == "semantic_collision"


def test_public_materializer_requires_exact_result_type():
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError):
        _materialize(object())


def test_stored_resource_facade_materializes_one_exact_row():
    row = _stored_materialize(_stored_resource())

    assert row.resource_id == "practitioner-a"
    assert row.requested_npi == REQUESTED_NPI


def test_stored_resource_facade_rejects_wrong_type_and_validation_failure(monkeypatch):
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError):
        _stored_materialize(object())

    monkeypatch.setattr(
        materialization,
        "validate_uhc_flex_practitioner_search_bundle",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("secret")),
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError) as error_info:
        _stored_materialize(_stored_resource())
    assert error_info.value.code == "result_invalid"


def test_stored_resource_facade_rechecks_raw_identity(monkeypatch):
    different_result = _result(_practitioner("practitioner-b"))
    monkeypatch.setattr(
        materialization,
        "validate_uhc_flex_practitioner_search_bundle",
        lambda *_args, **_kwargs: different_result,
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError) as error_info:
        _stored_materialize(_stored_resource())
    assert error_info.value.code == "raw_content_drift"


def test_stored_resource_facade_requires_one_materialized_row(monkeypatch):
    monkeypatch.setattr(
        materialization,
        "materialize_uhc_flex_practitioner_result",
        lambda *_args, **_kwargs: (),
    )
    with pytest.raises(materialization.UHCFlexPractitionerMaterializationError) as error_info:
        _stored_materialize(_stored_resource())
    assert error_info.value.code == "result_invalid"
