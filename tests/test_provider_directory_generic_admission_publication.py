import datetime as dt
import importlib
import json
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_source_catalog_outcomes as catalog_outcomes
from process import provider_directory_validated_publication_catalog as publication_catalog
from process.provider_directory_validated_publication_contract import (
    AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY,
    AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY,
    ValidatedPublicationCandidate,
)
from tests.test_provider_directory_source_outcomes import (
    _catalog,
    _MappingResult,
    _metadata,
)
from tests.test_provider_directory_dataset_selection_receipt_db import (
    _large_metadata_with_normalized_receipt,
    _receipt_candidate_and_proof,
)
from tests.test_provider_directory_validated_publication_catalog import (
    _catalog_dataset_rows,
)
from tests.test_provider_directory_validated_publication_contract import (
    CANDIDATE_HASH,
    CANDIDATE_ID,
    CANDIDATE_ROOT,
    CURRENT_DATASET_HASH,
    CURRENT_DATASET_ID,
    CURRENT_ROOT_RUN_ID,
    ENDPOINT_ID,
    SOURCE_ID,
    VALIDATED_AT,
)


importer = importlib.import_module("process.provider_directory_fhir")


PUBLICATION_METADATA_HASH = "e" * 64
ADMISSION_SHA256 = "c" * 64
CATALOG_ENTRY_ID = "example-directory"
CATALOG_DIGEST = "d" * 64


def _generic_candidate_map(*, first_publication=False):
    candidate_map = {
        "source_id": SOURCE_ID,
        "endpoint_id": ENDPOINT_ID,
        "dataset_id": CANDIDATE_ID,
        "dataset_hash": CANDIDATE_HASH,
        "acquisition_root_run_id": CANDIDATE_ROOT,
        "validated_at": VALIDATED_AT,
        "automatic_publication_policy": (
            AUTOMATIC_GENERIC_BOOTSTRAP_PUBLICATION_POLICY
            if first_publication
            else AUTOMATIC_GENERIC_ADMISSION_PUBLICATION_POLICY
        ),
        "content_proof_admission_sha256": ADMISSION_SHA256,
        "expected_current": (
            None
            if first_publication
            else {
                "endpoint_id": ENDPOINT_ID,
                "dataset_id": CURRENT_DATASET_ID,
                "dataset_hash": CURRENT_DATASET_HASH,
                "acquisition_root_run_id": CURRENT_ROOT_RUN_ID,
            }
        ),
    }
    if first_publication:
        candidate_map.update(
            source_catalog_entry_id=CATALOG_ENTRY_ID,
            source_catalog_digest_sha256=CATALOG_DIGEST,
        )
    return candidate_map


def _generic_canonical_dataset():
    return SimpleNamespace(
        source_id=SOURCE_ID,
        endpoint_id=ENDPOINT_ID,
        dataset_id=CANDIDATE_ID,
        dataset_hash=CANDIDATE_HASH,
        evidence_run_id=CANDIDATE_ROOT,
        validated_at=VALIDATED_AT,
        status="validated",
        is_current=False,
        expected_incumbent_dataset_id=CURRENT_DATASET_ID,
        completion_proof_required_version=None,
        completion_proof_sha256=None,
        completion_proof_cutoff=None,
        verification_source_status=None,
        verification_campaign_id=None,
        verification_source_scope_hash=None,
        verification_source_ids=(SOURCE_ID,),
        reviewed_root_policy=None,
        publication_metadata_hash=PUBLICATION_METADATA_HASH,
        content_proof_admission_sha256=ADMISSION_SHA256,
        generic_admission_sealed=True,
        artifact_selection_receipt_present=True,
    )


def _generic_catalog_fixture(monkeypatch):
    monkeypatch.setattr(
        publication_catalog,
        "bootstrap_catalog_authority",
        lambda source_id: (
            (CATALOG_ENTRY_ID, CATALOG_DIGEST)
            if source_id == SOURCE_ID
            else None
        ),
    )
    dataset_rows = _catalog_dataset_rows(False)
    dataset_rows[1]["publication_metadata"] = _metadata(source_ids=(SOURCE_ID,))
    execute = AsyncMock(return_value=_MappingResult(dataset_rows))
    monkeypatch.setattr(catalog_outcomes.db, "execute", execute)
    canonical_dataset = _generic_canonical_dataset()
    resolve_candidate = AsyncMock(return_value={SOURCE_ID: canonical_dataset})
    monkeypatch.setattr(
        catalog_outcomes,
        "_canonical_validated_datasets_by_source_id",
        resolve_candidate,
    )
    catalog_map = deepcopy(_catalog(source_ids=(SOURCE_ID,)))
    catalog_map["items"][0]["classification"] = "acquisition"
    return catalog_map, canonical_dataset, dataset_rows, execute, resolve_candidate


def _generic_publication_fence(
    *,
    publication_metadata_hash=PUBLICATION_METADATA_HASH,
    admission_sha256=ADMISSION_SHA256,
    first_publication=False,
):
    candidate = ValidatedPublicationCandidate.from_payload(
        _generic_candidate_map(first_publication=first_publication)
    )
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id=SOURCE_ID,
        endpoint_id=ENDPOINT_ID,
        dataset_id=CANDIDATE_ID,
        evidence_run_id=CANDIDATE_ROOT,
        status=importer.ENDPOINT_DATASET_VALIDATED,
        is_current=False,
        previous_dataset_id=None if first_publication else CURRENT_DATASET_ID,
        expected_incumbent_dataset_id=(
            None if first_publication else CURRENT_DATASET_ID
        ),
        promote_on_cutover=True,
        dataset_hash=CANDIDATE_HASH,
        validated_at=VALIDATED_AT,
        publication_metadata_hash=publication_metadata_hash,
        content_proof_admission_sha256=admission_sha256,
        generic_admission_sealed=True,
        artifact_selection_receipt_present=True,
        verification_source_ids=(SOURCE_ID,),
    )
    return importer.ProviderDirectoryArtifactDatasetFence(
        (dataset,),
        should_select_validated_candidates=True,
        validated_publication_candidate=candidate,
    )


def _locked_candidate_row_map():
    return {
        "dataset_id": CANDIDATE_ID,
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": CANDIDATE_ROOT,
        "status": importer.ENDPOINT_DATASET_VALIDATED,
        "is_current": False,
        "previous_dataset_id": CURRENT_DATASET_ID,
        "dataset_hash": CANDIDATE_HASH,
        "validated_at": VALIDATED_AT,
        "superseded_at": None,
        "completion_proof_required_version": None,
        "completion_proof_sha256": None,
        "publication_metadata_json": {"source_ids": [SOURCE_ID]},
        "publication_metadata_hash": PUBLICATION_METADATA_HASH,
        "content_proof_admission_sha256": ADMISSION_SHA256,
        "generic_admission_sealed": True,
        "artifact_selection_receipt_present": True,
        "locked_current_dataset_ids": [CURRENT_DATASET_ID],
    }


def _locked_incumbent_row_map():
    return {
        "dataset_id": CURRENT_DATASET_ID,
        "endpoint_id": ENDPOINT_ID,
        "dataset_hash": CURRENT_DATASET_HASH,
        "acquisition_root_run_id": CURRENT_ROOT_RUN_ID,
        "status": importer.ENDPOINT_DATASET_PUBLISHED,
        "is_current": True,
        "published_at": dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
        "superseded_at": None,
        "locked_current_dataset_ids": [CURRENT_DATASET_ID],
    }


def _sealed_refresh_context():
    candidate, content_proof = _receipt_candidate_and_proof()
    metadata = _large_metadata_with_normalized_receipt()
    seal = importer.admission_seal_from_validated_metadata(metadata)
    assert seal is not None
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id=candidate.source_ids[0],
        endpoint_id=candidate.endpoint_id,
        dataset_id=candidate.dataset_id,
        evidence_run_id=candidate.acquisition_root_run_id,
        selected_resources=candidate.selected_resources,
        expected_resources=candidate.expected_resources,
        status=importer.ENDPOINT_DATASET_VALIDATED,
        is_current=False,
        previous_dataset_id=candidate.previous_dataset_id,
        dataset_hash=content_proof.dataset_hash,
        resource_count=content_proof.resource_count,
        content_proof_admission_sha256=seal.proof_sha256,
        generic_admission_sealed=True,
        artifact_selection_receipt_present=True,
    )
    relation_proof_by_name = {
        metadata_key: {"complete": True, "edge_count": 0}
        for metadata_key in (
            importer.PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_METADATA_KEY,
            importer.PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_METADATA_KEY,
        )
    }
    return dataset, candidate, seal, relation_proof_by_name


def test_generic_policies_are_closed_and_bind_incumbent_state():
    candidate_by_field = _generic_candidate_map()

    assert (
        ValidatedPublicationCandidate.from_payload(candidate_by_field).to_payload()
        == candidate_by_field
    )
    with pytest.raises(ValueError, match="schema_invalid"):
        ValidatedPublicationCandidate.from_payload(
            {**candidate_by_field, "completion_proof_required_version": None}
        )
    with pytest.raises(ValueError, match="identity_invalid"):
        ValidatedPublicationCandidate.from_payload(
            {**candidate_by_field, "content_proof_admission_sha256": "C" * 64}
        )


def test_locked_sql_recomputes_generic_seal_and_stored_receipt():
    locked_sql = importer._artifact_fence_dataset_rows_sql(for_update=True)

    assert "AS generic_admission_sealed" in locked_sql
    assert "content_proof_admission_kind =" in locked_sql
    assert "provider_directory_endpoint_dataset_admission_metadata_sha256" in (
        locked_sql
    )
    assert "artifact_selection_receipt_json IS NOT NULL" in locked_sql
    assert "AS artifact_selection_receipt_present" in locked_sql


@pytest.mark.parametrize(
    ("field_name", "drifted_value"),
    (
        ("publication_metadata_hash", "f" * 64),
        ("content_proof_admission_sha256", "f" * 64),
        ("generic_admission_sealed", False),
        ("artifact_selection_receipt_present", False),
    ),
)
def test_locked_generic_fence_rejects_authority_drift(
    field_name,
    drifted_value,
):
    candidate_row_map = _locked_candidate_row_map()
    candidate_row_map[field_name] = drifted_value

    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        importer._assert_locked_artifact_fence_datasets(
            _generic_publication_fence(),
            [candidate_row_map, _locked_incumbent_row_map()],
            {ENDPOINT_ID: [CANDIDATE_ID]},
        )


def test_generic_authority_is_not_part_of_unrelated_dataset_identity():
    dataset = replace(
        _generic_publication_fence().datasets[0],
        content_proof_admission_sha256=None,
        generic_admission_sealed=False,
        artifact_selection_receipt_present=False,
    )

    assert importer._is_artifact_fence_dataset_row_exact(
        dataset,
        _locked_candidate_row_map(),
    )


def test_generic_fence_accepts_refreshed_mutable_summary_digest():
    expected_fence = _generic_publication_fence()
    refreshed_fence = _generic_publication_fence(publication_metadata_hash="f" * 64)
    refreshed_candidate_row = _locked_candidate_row_map()
    refreshed_candidate_row["publication_metadata_hash"] = "f" * 64

    importer._assert_artifact_fence_selection_unchanged(
        expected_fence,
        refreshed_fence,
    )
    importer._assert_locked_artifact_fence_datasets(
        refreshed_fence,
        [refreshed_candidate_row, _locked_incumbent_row_map()],
        {ENDPOINT_ID: [CANDIDATE_ID]},
    )

    drifted_fence = _generic_publication_fence(admission_sha256="f" * 64)
    refreshed_candidate_row["content_proof_admission_sha256"] = "f" * 64
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        importer._assert_locked_artifact_fence_datasets(
            drifted_fence,
            [refreshed_candidate_row, _locked_incumbent_row_map()],
            {ENDPOINT_ID: [CANDIDATE_ID]},
        )


@pytest.mark.asyncio
async def test_sealed_refresh_recreates_receipt_from_admission_summary(
    monkeypatch,
):
    dataset, candidate, seal, relation_proofs = _sealed_refresh_context()
    metadata_summary = deepcopy(seal.metadata_summary)
    store_receipt = AsyncMock(return_value="UPDATE 1")
    monkeypatch.setattr(importer.db, "status", store_receipt)

    await importer._refresh_current_artifact_dataset_source_summary(
        dataset,
        candidate,
        metadata_summary,
        relation_proofs,
    )

    assert importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY not in (
        metadata_summary
    )
    stored_receipt = json.loads(store_receipt.await_args.kwargs["receipt_json"])
    assert stored_receipt[importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY] == {
        "complete": True,
        "contract_id": (importer.PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID),
        "proof_sha256": seal.proof_sha256,
    }
    assert store_receipt.await_args.kwargs["sealed_proof_sha256"] == (seal.proof_sha256)
    assert "content_proof_admission_kind = :generic_admission_kind" in (
        store_receipt.await_args.args[0]
    )


@pytest.mark.asyncio
async def test_sealed_refresh_rejects_missing_admission_summary(monkeypatch):
    dataset, candidate, seal, relation_proofs = _sealed_refresh_context()
    metadata_summary = deepcopy(seal.metadata_summary)
    metadata_summary.pop(importer.ADMISSION_GENERIC_PROOF_SUMMARY_KEY)
    store_receipt = AsyncMock(return_value="UPDATE 1")
    monkeypatch.setattr(importer.db, "status", store_receipt)

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="artifact_selection_receipt_invalid",
    ):
        await importer._refresh_current_artifact_dataset_source_summary(
            dataset,
            candidate,
            metadata_summary,
            relation_proofs,
        )

    store_receipt.assert_not_awaited()


@pytest.mark.asyncio
async def test_sealed_refresh_rejects_missing_admission_sha(monkeypatch):
    dataset, candidate, seal, relation_proofs = _sealed_refresh_context()
    dataset = replace(
        dataset,
        content_proof_admission_sha256=None,
    )
    store_receipt = AsyncMock(return_value="UPDATE 1")
    monkeypatch.setattr(importer.db, "status", store_receipt)

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="artifact_selection_receipt_invalid",
    ):
        await importer._refresh_current_artifact_dataset_source_summary(
            dataset,
            candidate,
            deepcopy(seal.metadata_summary),
            relation_proofs,
        )

    store_receipt.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("classification", ("acquisition", "bulk_acquisition"))
async def test_catalog_exposes_exact_generic_admission_candidate(
    monkeypatch,
    classification,
):
    catalog_map, canonical_dataset, _, _, resolve_candidate = _generic_catalog_fixture(
        monkeypatch
    )
    catalog_map["items"][0]["classification"] = classification

    enriched_catalog = await catalog_outcomes.enrich_provider_directory_source_catalog(
        catalog_map
    )

    assert (
        enriched_catalog["items"][0]["validated_publication_candidate"]
        == _generic_candidate_map()
    )
    assert resolve_candidate.await_args.args == ([SOURCE_ID],)

    canonical_dataset.publication_metadata_hash = "f" * 64
    refreshed_catalog = await catalog_outcomes.enrich_provider_directory_source_catalog(
        catalog_map
    )
    assert (
        refreshed_catalog["items"][0]["validated_publication_candidate"]
        == _generic_candidate_map()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "drifted_value"),
    (
        ("content_proof_admission_sha256", "F" * 64),
        ("generic_admission_sealed", False),
        ("artifact_selection_receipt_present", False),
        ("completion_proof_cutoff", "2026-08-11"),
    ),
)
async def test_catalog_rejects_generic_authority_drift(
    monkeypatch,
    field_name,
    drifted_value,
):
    catalog_map, canonical_dataset, _, _, _ = _generic_catalog_fixture(monkeypatch)
    setattr(canonical_dataset, field_name, drifted_value)

    enriched_catalog = await catalog_outcomes.enrich_provider_directory_source_catalog(
        catalog_map
    )

    assert "validated_publication_candidate" not in enriched_catalog["items"][0]
