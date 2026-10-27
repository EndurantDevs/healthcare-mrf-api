# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SOURCE selection never asks for legacy payload-function authority."""

import asyncio
import importlib
from types import SimpleNamespace

import pytest

from process import provider_directory_generic_source_policy as policy
from process.provider_directory_generic_candidate import GenericCandidateError

fhir = importlib.import_module("process.provider_directory_fhir")
LEGACY_HASH = "provider_directory_subset_payload_sha256"


def _queries():
    """Reach both source selection modes and both original fence modes."""
    return (
        *fhir._artifact_dataset_option_projection_parts(True),
        fhir._provider_directory_artifact_dataset_selection_sql(["source_a"], should_select_validated_candidates=True),
        fhir._provider_directory_artifact_dataset_selection_sql(["source_a"]),
        fhir._provider_directory_artifact_dataset_selection_sql(None),
        fhir._artifact_fence_dataset_rows_sql(for_update=False),
        fhir._artifact_fence_dataset_rows_sql(for_update=True),
        fhir._artifact_fence_dataset_detail_ctes(),
        fhir._artifact_legacy_dataset_projection_sql(),
    )


@pytest.mark.asyncio
async def test_trusted_factory_omits_legacy_function_from_every_constructor(monkeypatch):
    monkeypatch.setattr(fhir, "_schema", lambda: "source_schema")
    legacy_queries = _queries()
    assert any(LEGACY_HASH in query for query in legacy_queries)
    factory = SimpleNamespace(transaction=lambda *_args, **_kwargs: None)
    async with policy.bind_generic_source_transaction_factory(factory):
        for query in _queries():
            assert LEGACY_HASH not in query
        option, _lateral = fhir._artifact_dataset_option_projection_parts(True)
        assert "dataset.publication_metadata_sha256" in option
        assert "AS normalized_receipt_present" in option
        assert "dataset.content_proof_admission_sha256" in option
        assert "publication_metadata_sha256" in fhir._artifact_fence_dataset_rows_sql(for_update=True)
    assert _queries() == legacy_queries


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    (
        {},
        {"publication_metadata_hash": None},
        {
            "publication_metadata_hash": "a" * 64,
            "normalized_receipt_present": True,
            "content_proof_contract_id": "legacy",
            "content_proof_sha256": "b" * 64,
        },
    ),
)
async def test_source_rejects_missing_unsealed_or_normalized_legacy_hash(row):
    factory = SimpleNamespace(transaction=lambda *_args, **_kwargs: None)
    async with policy.bind_generic_source_transaction_factory(factory):
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="publication_metadata_hash_invalid"):
            fhir._artifact_publication_metadata_hash_from_row(row, {"source_ids": ["source_a"]})


@pytest.mark.asyncio
async def test_source_keeps_the_stored_digest_without_python_fallback():
    factory = SimpleNamespace(transaction=lambda *_args, **_kwargs: None)
    async with policy.bind_generic_source_transaction_factory(factory):
        assert (
            fhir._artifact_publication_metadata_hash_from_row(
                {"publication_metadata_hash": "a" * 64, "normalized_receipt_present": False},
                {"source_ids": ["source_a"]},
            )
            == "a" * 64
        )


@pytest.mark.asyncio
async def test_source_projection_drops_legacy_receipt_authority_but_keeps_seal_checks():
    factory = SimpleNamespace(transaction=lambda *_args, **_kwargs: None)
    options = fhir._ArtifactDatasetProjectionOptions(
        metadata_sealed="trusted_seal",
        content_proof_sealed="trusted_content_seal",
        legacy_bounded="legacy_authority_probe",
        normalized_receipt_state="legacy_receipt_probe",
        seal_consistent="receipt_consistent",
    )
    async with policy.bind_generic_source_transaction_factory(factory):
        projection = fhir._artifact_dataset_option_projection_sql("metadata", options=options)
        assert "legacy_authority_probe" not in projection
        assert "legacy_receipt_probe" not in projection
        assert "WHEN (trusted_seal) AND (receipt_consistent) THEN dataset.publication_metadata_sha256" in projection
        assert "trusted_content_seal" in projection
        for for_update in (False, True):
            fence = fhir._artifact_fence_dataset_rows_sql(for_update=for_update)
            assert ":normalized_dataset_ids" not in fence
            assert "publication_metadata_sha256" in fence
            assert "content_proof_admission_version" in fence


@pytest.mark.asyncio
async def test_same_task_physical_binding_selects_source_without_a_factory(monkeypatch):
    publisher = SimpleNamespace(task=asyncio.current_task(), factory=None, storage_binding=None)
    monkeypatch.setattr(policy, "_PUBLISHER", policy.ContextVar("source_test_publisher", default=publisher))
    monkeypatch.setattr(policy, "_FACTORY", policy.ContextVar("source_test_factory", default=None))
    assert policy.is_source_storage_required() is True
    assert LEGACY_HASH not in fhir._artifact_fence_dataset_rows_sql(for_update=True)


@pytest.mark.asyncio
async def test_inherited_physical_publisher_refuses_even_with_a_factory(monkeypatch):
    publisher = SimpleNamespace(task=asyncio.current_task())
    monkeypatch.setattr(policy, "_PUBLISHER", policy.ContextVar("source_test_publisher", default=publisher))
    monkeypatch.setattr(policy, "_FACTORY", policy.ContextVar("source_test_factory", default=object()))

    async def child():
        with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
            fhir._artifact_fence_dataset_rows_sql(for_update=True)

    await asyncio.create_task(child())
