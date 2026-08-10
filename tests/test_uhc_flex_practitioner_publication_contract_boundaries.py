# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import uhc_flex_practitioner_publication as publication
from process import uhc_flex_practitioner_publication_store as store
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from tests.test_uhc_flex_practitioner_publication import _admission


def _readiness(resource_count: int = 1):
    admission = _admission(resource_count=resource_count)
    identity = publication.build_uhc_flex_practitioner_dataset_identity(
        admission
    )
    return publication.UHCFlexPractitionerDatasetReadiness(
        dataset_id=identity.dataset_id,
        previous_dataset_id=None,
        admission_id=admission.admission_id,
        candidate_acquisition_id=admission.candidate_acquisition_id,
        cohort_id=admission.cohort_id,
        dataset_intent_id=admission.dataset_intent_id,
        endpoint_id=identity.endpoint_id,
        semantic_projection_as_of=admission.semantic_projection_as_of,
        operation_key=admission.operation_key,
        dataset_hash=hashlib.sha256(b"").hexdigest(),
        resource_count=resource_count,
        source_id=UHC_FLEX_PRACTITIONER_SOURCE_ID,
        source_authority_id="unitedhealthcare",
        cohort_complete=True,
        endpoint_collection_complete=False,
        endpoint_complete=False,
    )


def test_publication_error_codes_are_bounded() -> None:
    expected_codes = {
        "admission",
        "content",
        "foreign_current",
        "replay",
        "source_drift",
        "state",
    }
    for error_code in expected_codes:
        error = publication.UHCFlexPractitionerPublicationError(error_code)
        assert error.code == error_code
        assert "Flex Practitioner" in str(error)
    assert publication.UHCFlexPractitionerPublicationError("unknown").code == (
        "state"
    )


def test_schema_json_and_row_helpers_fail_closed(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "fixture")
    monkeypatch.setenv("DB_SCHEMA", "other")
    with pytest.raises(publication.UHCFlexPractitionerPublicationError):
        publication._schema_name()

    monkeypatch.delenv("DB_SCHEMA")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "bad-schema")
    with pytest.raises(publication.UHCFlexPractitionerPublicationError):
        publication._schema_name()

    monkeypatch.delenv("HLTHPRT_DB_SCHEMA")
    assert publication._table("resource") == '"mrf"."resource"'
    assert publication._function("ready") == '"mrf"."ready"'
    assert publication._canonical_json({"b": 2, "a": 1}) == (
        '{"a":1,"b":2}'
    )
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="content is invalid",
    ):
        publication._canonical_json({object()})

    assert publication._row_fields(None) == {}
    assert publication._row_fields({"value": 1}) == {"value": 1}
    assert publication._row_fields(
        SimpleNamespace(_mapping={"value": 2})
    ) == {"value": 2}
    with pytest.raises(publication.UHCFlexPractitionerPublicationError):
        publication._row_fields(SimpleNamespace(_mapping=[]))


def test_identity_metadata_and_result_types_reject_drift() -> None:
    admission = _admission()
    identity = publication.build_uhc_flex_practitioner_dataset_identity(
        admission
    )
    with pytest.raises(ValueError, match="admission is invalid"):
        publication.build_uhc_flex_practitioner_dataset_identity(object())
    with pytest.raises(ValueError, match="endpoint ID is invalid"):
        publication.build_uhc_flex_practitioner_dataset_identity(
            admission,
            endpoint_id="0" * 64,
        )
    with pytest.raises(ValueError, match="dataset identity is invalid"):
        replace(identity, semantic_projection_as_of="not-a-date")
    with pytest.raises(ValueError, match="dataset identity is invalid"):
        replace(identity, dataset_id="pdufpd_" + "0" * 48)

    with pytest.raises(ValueError, match="publication identity is invalid"):
        publication.uhc_flex_practitioner_publication_metadata(
            object(),
            admission,
        )
    with pytest.raises(ValueError, match="publication identity is invalid"):
        publication.uhc_flex_practitioner_publication_metadata(
            identity,
            _admission(resource_count=2),
        )

    readiness = _readiness()
    with pytest.raises(ValueError, match="dataset readiness is invalid"):
        replace(readiness, semantic_projection_as_of="not-a-date")
    with pytest.raises(ValueError, match="dataset readiness is invalid"):
        replace(readiness, endpoint_complete=True)
    with pytest.raises(ValueError, match="publication result is invalid"):
        publication.UHCFlexPractitionerPublicationResult(object(), False)
    with pytest.raises(ValueError, match="publication result is invalid"):
        publication.UHCFlexPractitionerPublicationResult(readiness, 1)


@pytest.mark.asyncio
async def test_readiness_loaders_validate_and_delegate(monkeypatch) -> None:
    readiness = _readiness()
    dataset_loader = AsyncMock(return_value=readiness)
    current_loader = AsyncMock(return_value=readiness)
    monkeypatch.setattr(store, "load_dataset_readiness", dataset_loader)
    monkeypatch.setattr(store, "load_current_readiness", current_loader)
    database = object()

    with pytest.raises(ValueError, match="dataset ID is invalid"):
        await publication.load_uhc_flex_practitioner_dataset_readiness("bad")
    assert await publication.load_uhc_flex_practitioner_dataset_readiness(
        readiness.dataset_id,
        database=database,
    ) is readiness
    assert await publication.load_current_uhc_flex_dataset_readiness(
        database=database
    ) is readiness
    dataset_loader.assert_awaited_once_with(
        readiness.dataset_id,
        database=database,
    )
    current_loader.assert_awaited_once_with(database=database)


@pytest.mark.asyncio
async def test_publish_api_rejects_bad_inputs_and_source_drift(
    monkeypatch,
) -> None:
    with pytest.raises(ValueError, match="candidate acquisition ID"):
        await publication.publish_uhc_flex_practitioner_dataset("bad")
    candidate_id = _admission().candidate_acquisition_id
    with pytest.raises(ValueError, match="batch size"):
        await publication.publish_uhc_flex_practitioner_dataset(
            candidate_id,
            batch_size=0,
        )

    endpoint_id = publication.uhc_flex_practitioner_endpoint_identity().endpoint_id
    register = AsyncMock(return_value=SimpleNamespace(endpoint_id="0" * 64))
    monkeypatch.setattr(
        publication,
        "register_uhc_flex_practitioner_source",
        register,
    )
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="source has drifted",
    ):
        await publication.publish_uhc_flex_practitioner_dataset(candidate_id)

    readiness = _readiness()
    expected = publication.UHCFlexPractitionerPublicationResult(
        readiness,
        replayed=False,
    )
    register.return_value = SimpleNamespace(endpoint_id=endpoint_id)
    publisher = AsyncMock(return_value=expected)
    monkeypatch.setattr(
        store,
        "publish_registered_uhc_flex_dataset",
        publisher,
    )
    database = object()
    publication_result = await publication.publish_uhc_flex_practitioner_dataset(
        candidate_id,
        database=database,
        batch_size=7,
    )
    assert publication_result is expected
    publisher.assert_awaited_once_with(
        candidate_id,
        endpoint_id,
        7,
        database=database,
    )
