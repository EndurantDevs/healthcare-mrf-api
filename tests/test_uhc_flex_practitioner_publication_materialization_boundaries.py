# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.provider_directory_resource_hash import (
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    resource_payload_sha256_for_contract,
)
from process import uhc_flex_practitioner_publication as publication
from process import (
    uhc_flex_practitioner_publication_materialization as materialization,
)
from tests.test_uhc_flex_practitioner_publication import _admission


def _identity_and_admission(resource_count: int = 1):
    admission = _admission(resource_count=resource_count)
    identity = publication.build_uhc_flex_practitioner_dataset_identity(
        admission
    )
    return identity, admission


@pytest.mark.asyncio
async def test_page_insert_requires_exact_resource_and_provenance_counts(monkeypatch) -> None:
    page_rows = [{
        "dataset_id": "dataset", "resource_type": "Practitioner", "resource_id": "resource",
        "payload_hash": "a" * 64, "payload_json": {"resource_id": "resource"},
        "requested_npi": 1000000001, "candidate_acquisition_id": "candidate",
        "acquired_resource_sha256": "b" * 64,
    }]
    copier = AsyncMock(return_value=0)
    monkeypatch.setattr(materialization, "copy_dataset_candidate_rows", copier)
    database = object()
    await materialization._insert_materialized_page(database, [])
    copier.assert_not_awaited()
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="content is invalid",
    ):
        await materialization._insert_materialized_page(
            database,
            page_rows,
        )

    copier.reset_mock()
    copier.side_effect = [1, 0]
    with pytest.raises(publication.UHCFlexPractitionerPublicationError):
        await materialization._insert_materialized_page(
            database,
            page_rows,
        )

    copier.reset_mock()
    copier.side_effect = [1, 1]
    await materialization._insert_materialized_page(
        database,
        page_rows,
    )
    assert copier.await_count == 2
    assert copier.await_args_list[0].args[3][0][-1] is None
    assert copier.await_args_list[1].args[3][0][-1] == "b" * 64


def _patch_candidate_dependencies(monkeypatch, stored_pages):
    stored_by_field = {"npi": 1000000001, "resource_id": "one"}
    materialized_by_field = {"resource_id": stored_by_field["resource_id"]}
    reader = AsyncMock(side_effect=stored_pages(stored_by_field))
    inserter = AsyncMock()
    facade = Mock(return_value=materialized_by_field)
    monkeypatch.setattr(
        materialization,
        "_resource_page_records",
        reader,
    )
    monkeypatch.setattr(
        materialization,
        "_materialize_admitted_practitioner_resource",
        facade,
    )
    monkeypatch.setattr(
        materialization,
        "_insert_materialized_page",
        inserter,
    )
    return stored_by_field, reader, inserter


@pytest.mark.asyncio
async def test_candidate_materialization_pages_exactly(monkeypatch) -> None:
    identity, admission = _identity_and_admission()
    stored, reader, inserter = _patch_candidate_dependencies(
        monkeypatch,
        lambda stored_resource: [[stored_resource], []],
    )

    assert await materialization._materialize_candidate(
        object(),
        identity,
        admission,
        25,
    ) == 1
    assert reader.await_args_list[1].args[2] == (
        stored["npi"]
    )
    assert reader.await_args_list[1].args[3] == "one"
    assert inserter.await_args.args[1][0]["candidate_acquisition_id"] == (
        admission.candidate_acquisition_id
    )


@pytest.mark.asyncio
async def test_candidate_materialization_rejects_census(monkeypatch) -> None:
    identity, admission = _identity_and_admission()
    _stored, reader, _inserter = _patch_candidate_dependencies(
        monkeypatch,
        lambda stored_resource: [[stored_resource]],
    )
    zero_identity, zero_admission = _identity_and_admission(resource_count=0)
    with pytest.raises(publication.UHCFlexPractitionerPublicationError):
        await materialization._materialize_candidate(
            object(),
            zero_identity,
            zero_admission,
            25,
        )

    reader.reset_mock(side_effect=True)
    reader.side_effect = [[]]
    with pytest.raises(publication.UHCFlexPractitionerPublicationError):
        await materialization._materialize_candidate(
            object(),
            identity,
            admission,
            25,
        )


def test_semantic_resource_identity_recomputes_hash_and_rejects_tamper() -> None:
    semantic_payload_by_field = {"resource_id": "synthetic"}
    payload_hash = resource_payload_sha256_for_contract(
        semantic_payload_by_field,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    resource_by_field = {
        "resource_type": "Practitioner",
        "resource_id": "synthetic",
        "payload_hash": payload_hash,
        "payload_json": semantic_payload_by_field,
        "acquired_resource_sha256": None,
    }
    assert materialization._semantic_resource_identity(resource_by_field) == (
        "Practitioner",
        "synthetic",
        payload_hash,
    )
    assert materialization._semantic_resource_identity(
        {**resource_by_field, "payload_json": '{"resource_id":"synthetic"}'}
    )[2] == payload_hash

    rejected_fields = (
        {**resource_by_field, "payload_hash": "0" * 64},
        {**resource_by_field, "payload_json": {"resource_id": "changed"}},
    )
    for resource_fields_with_drift in rejected_fields:
        with pytest.raises(
            publication.UHCFlexPractitionerPublicationError,
            match="content is invalid",
        ):
            materialization._semantic_resource_identity(
                resource_fields_with_drift
            )


@pytest.mark.asyncio
async def test_semantic_dataset_proof_pages_in_canonical_order() -> None:
    first_payload_by_field = {"resource_id": "one"}
    second_payload_by_field = {"resource_id": "two"}

    def resource_row(resource_id: str, payload_by_field: dict[str, str]):
        return {
            "resource_type": "Practitioner",
            "resource_id": resource_id,
            "payload_hash": resource_payload_sha256_for_contract(
                payload_by_field,
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            ),
            "payload_json": payload_by_field,
            "acquired_resource_sha256": None,
        }

    database_rows = [
        resource_row("one", first_payload_by_field),
        resource_row("two", second_payload_by_field),
    ]
    database = SimpleNamespace(all=AsyncMock(side_effect=[database_rows, []]))
    dataset_hash, resource_count = await materialization._semantic_dataset_proof(
        database,
        "dataset",
        10,
    )
    semantic_lines = [
        publication._canonical_json(
            [
                database_resource["resource_type"],
                database_resource["resource_id"],
                database_resource["payload_hash"],
            ]
        )
        for database_resource in database_rows
    ]
    assert dataset_hash == hashlib.sha256(
        "\n".join(semantic_lines).encode()
    ).hexdigest()
    assert resource_count == 2
    assert database.all.await_args_list[1].kwargs["after_resource_id"] == "two"


@pytest.mark.asyncio
async def test_validation_requires_exact_census_updates_and_database_proof(
    monkeypatch,
) -> None:
    identity, admission = _identity_and_admission()
    proof = AsyncMock(return_value=("f" * 64, 0))
    monkeypatch.setattr(materialization, "_semantic_dataset_proof", proof)
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="content is invalid",
    ):
        await materialization._validate_candidate(
            database,
            identity,
            admission,
            10,
        )

    proof.return_value = ("f" * 64, 1)
    for update_counts in ((0, 1), (1, 0)):
        database.status = AsyncMock(side_effect=update_counts)
        with pytest.raises(
            publication.UHCFlexPractitionerPublicationError,
            match="state is invalid",
        ):
            await materialization._validate_candidate(
                database,
                identity,
                admission,
                10,
            )

    database.status = AsyncMock(side_effect=[1, 1])
    database.scalar = AsyncMock(return_value=False)
    with pytest.raises(
        publication.UHCFlexPractitionerPublicationError,
        match="content is invalid",
    ):
        await materialization._validate_candidate(
            database,
            identity,
            admission,
            10,
        )

    database.status = AsyncMock(side_effect=[1, 1])
    database.scalar = AsyncMock(return_value=True)
    assert await materialization._validate_candidate(
        database,
        identity,
        admission,
        10,
    ) == "f" * 64
