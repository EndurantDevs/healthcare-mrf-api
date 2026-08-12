import datetime as dt
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_source_catalog_outcomes as catalog_outcomes
from process.provider_directory_validated_publication_contract import (
    AUTOMATIC_VALIDATED_PUBLICATION_EXHAUSTIVE_SOURCE_STATUS,
    AUTOMATIC_VALIDATED_PUBLICATION_REVIEWED_SOURCE_STATUS,
    ValidatedPublicationCandidate,
    validated_publication_source_status,
)
from process.provider_directory_fhir_subset_completion import (
    SERVER_ISSUED_SUBSET_REQUIRED_VERSION,
)
from tests.test_provider_directory_source_outcomes import (
    _catalog,
    _dataset_row,
    _MappingResult,
    _metadata,
)
from tests.test_provider_directory_validated_publication_contract import (
    CAMPAIGN_ID,
    CANDIDATE_HASH,
    CANDIDATE_ID,
    CANDIDATE_ROOT,
    CURRENT_DATASET_ID,
    ENDPOINT_ID,
    PROOF_HASH,
    SCOPE_HASH,
    SOURCE_ID,
    VALIDATED_AT,
    _candidate_payload,
)


def _catalog_dataset_rows(first_publication):
    incumbent_row_map = _dataset_row(
        publication_metadata=_metadata(source_ids=(SOURCE_ID,)),
        current_source_ids=(SOURCE_ID,),
    )
    candidate_row_map = _dataset_row(
        dataset_id=CANDIDATE_ID,
        acquisition_root_run_id=CANDIDATE_ROOT,
        previous_dataset_id=(
            None if first_publication else CURRENT_DATASET_ID
        ),
        dataset_hash=CANDIDATE_HASH,
        status="validated",
        is_current=False,
        validated_at=dt.datetime(2026, 8, 11, tzinfo=dt.UTC),
        published_at=None,
        publication_metadata=_metadata(
            source_ids=(SOURCE_ID,),
            requires_twin_root_verification=True,
            verification_role="verification_candidate",
        ),
        current_source_ids=(SOURCE_ID,),
    )
    return (
        [candidate_row_map]
        if first_publication
        else [incumbent_row_map, candidate_row_map]
    )


def _canonical_publication_dataset(
    proof_version,
    proof_sha256,
    first_publication,
):
    candidate = ValidatedPublicationCandidate.from_payload(
        _candidate_payload(
            completion_proof_required_version=proof_version,
            completion_proof_sha256=proof_sha256,
            first_publication=first_publication,
        )
    )
    return SimpleNamespace(
        source_id=SOURCE_ID,
        endpoint_id=ENDPOINT_ID,
        dataset_id=CANDIDATE_ID,
        dataset_hash=CANDIDATE_HASH,
        evidence_run_id=CANDIDATE_ROOT,
        validated_at=VALIDATED_AT,
        status="validated",
        is_current=False,
        expected_incumbent_dataset_id=(
            None if first_publication else CURRENT_DATASET_ID
        ),
        completion_proof_required_version=proof_version,
        completion_proof_sha256=proof_sha256,
        verification_source_status=validated_publication_source_status(
            candidate
        ),
        verification_campaign_id=CAMPAIGN_ID,
        verification_source_scope_hash=SCOPE_HASH,
        verification_source_ids=(SOURCE_ID,),
        reviewed_root_policy=None,
    )


def _catalog_publication_fixture(
    monkeypatch,
    proof_version,
    proof_sha256,
    *,
    first_publication=False,
):
    """Install one exact catalog candidate and its canonical resolver result."""

    monkeypatch.setattr(
        catalog_outcomes.db,
        "execute",
        AsyncMock(
            return_value=_MappingResult(
                _catalog_dataset_rows(first_publication)
            )
        ),
    )
    canonical_dataset = _canonical_publication_dataset(
        proof_version,
        proof_sha256,
        first_publication,
    )
    resolve_candidate = AsyncMock(
        return_value={SOURCE_ID: canonical_dataset}
    )
    monkeypatch.setattr(
        catalog_outcomes,
        "_canonical_validated_datasets_by_source_id",
        resolve_candidate,
    )
    catalog = deepcopy(_catalog(source_ids=(SOURCE_ID,)))
    catalog["items"][0]["classification"] = "acquisition"
    return catalog, canonical_dataset, resolve_candidate


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("proof_version", "proof_sha256"),
    (
        (SERVER_ISSUED_SUBSET_REQUIRED_VERSION, PROOF_HASH),
        (None, None),
    ),
)
async def test_catalog_exposes_only_an_exact_proven_pair(
    monkeypatch,
    proof_version,
    proof_sha256,
):
    catalog, canonical_dataset, resolve_candidate = (
        _catalog_publication_fixture(
            monkeypatch,
            proof_version,
            proof_sha256,
        )
    )

    enriched = await catalog_outcomes.enrich_provider_directory_source_catalog(
        catalog
    )

    assert enriched["items"][0]["validated_publication_candidate"] == (
        _candidate_payload(
            completion_proof_required_version=proof_version,
            completion_proof_sha256=proof_sha256,
        )
    )
    canonical_dataset.verification_source_status = (
        AUTOMATIC_VALIDATED_PUBLICATION_EXHAUSTIVE_SOURCE_STATUS
        if proof_version is not None
        else AUTOMATIC_VALIDATED_PUBLICATION_REVIEWED_SOURCE_STATUS
    )
    drifted = await catalog_outcomes.enrich_provider_directory_source_catalog(
        catalog
    )
    assert "validated_publication_candidate" not in drifted["items"][0]
    assert resolve_candidate.await_args_list[0].args == ([SOURCE_ID],)
    assert resolve_candidate.await_count == 2


@pytest.mark.asyncio
async def test_catalog_exposes_first_publication_only_without_current(
    monkeypatch,
):
    catalog, canonical_dataset, _ = _catalog_publication_fixture(
        monkeypatch,
        None,
        None,
        first_publication=True,
    )

    enriched = await catalog_outcomes.enrich_provider_directory_source_catalog(
        catalog
    )
    assert enriched["items"][0]["validated_publication_candidate"] == (
        _candidate_payload(
            completion_proof_required_version=None,
            completion_proof_sha256=None,
            first_publication=True,
        )
    )

    canonical_dataset.expected_incumbent_dataset_id = CURRENT_DATASET_ID
    drifted = await catalog_outcomes.enrich_provider_directory_source_catalog(
        catalog
    )
    assert "validated_publication_candidate" not in drifted["items"][0]


def _legacy_incumbent_fixture(monkeypatch, **incumbent_overrides):
    candidate_row = _catalog_dataset_rows(False)[1]
    candidate = catalog_outcomes.outcomes._current_dataset_from_row(
        candidate_row,
        {(SOURCE_ID,)},
    )
    assert candidate is not None
    monkeypatch.setattr(
        catalog_outcomes.outcomes,
        "_current_published_dataset_by_source_ids",
        AsyncMock(return_value={(SOURCE_ID,): candidate}),
    )
    monkeypatch.setattr(
        catalog_outcomes,
        "_profile_current_dataset_by_source_ids",
        AsyncMock(return_value={}),
    )
    incumbent_row = _dataset_row(
        endpoint_id=ENDPOINT_ID,
        dataset_id=CURRENT_DATASET_ID,
        acquisition_root_run_id="root-current",
        dataset_hash="a" * 64,
        publication_metadata=None,
        current_source_ids=None,
    )
    incumbent_row.update(incumbent_overrides)
    execute = AsyncMock(
        return_value=_MappingResult([incumbent_row])
    )
    monkeypatch.setattr(catalog_outcomes.db, "execute", execute)
    monkeypatch.setattr(
        catalog_outcomes,
        "_canonical_validated_datasets_by_source_id",
        AsyncMock(
            return_value={
                SOURCE_ID: _canonical_publication_dataset(None, None, False)
            }
        ),
    )
    catalog = deepcopy(_catalog(source_ids=(SOURCE_ID,)))
    catalog["items"][0]["classification"] = "acquisition"
    return catalog, execute


@pytest.mark.asyncio
async def test_catalog_uses_scalar_identity_for_legacy_unsealed_incumbent(
    monkeypatch,
):
    """Use an exact scalar incumbent without reading its legacy proof."""

    catalog, execute = _legacy_incumbent_fixture(monkeypatch)

    enriched = await catalog_outcomes.enrich_provider_directory_source_catalog(
        catalog
    )

    assert enriched["items"][0]["validated_publication_candidate"] == (
        _candidate_payload(
            completion_proof_required_version=None,
            completion_proof_sha256=None,
        )
    )
    assert enriched["items"][0]["current_outcome_summary"] == {
        "dataset_id": CURRENT_DATASET_ID,
        "status": "published",
        "is_current": True,
        "endpoint_id": ENDPOINT_ID,
        "acquisition_root_run_id": "root-current",
        "dataset_hash": "a" * 64,
    }
    incumbent_sql = str(execute.await_args.args[0])
    for raw_relation in (
        "publication_metadata_json",
        "publication_metadata_summary_json",
        "artifact_selection_receipt_json",
        "provider_directory_dataset_resource",
    ):
        assert raw_relation not in incumbent_sql
    for state_fence in (
        "status =",
        "is_current IS true",
        "published_at IS NOT NULL",
        "superseded_at IS NULL",
    ):
        assert state_fence in incumbent_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "field_value"),
    (
        ("endpoint_id", "different-endpoint"),
        ("dataset_id", "different-dataset"),
        ("status", "superseded"),
        ("is_current", False),
        ("published_at", None),
        ("superseded_at", dt.datetime(2026, 8, 12, tzinfo=dt.UTC)),
        ("dataset_hash", "not-a-hash"),
        ("acquisition_root_run_id", None),
    ),
)
async def test_catalog_rejects_drifted_legacy_incumbent(
    monkeypatch,
    field_name,
    field_value,
):
    """Reject scalar incumbents whose identity or serving state drifted."""

    catalog, _ = _legacy_incumbent_fixture(
        monkeypatch,
        **{field_name: field_value},
    )

    enriched = await catalog_outcomes.enrich_provider_directory_source_catalog(
        catalog
    )

    assert "current_outcome_summary" not in enriched["items"][0]
    assert "validated_publication_candidate" not in enriched["items"][0]
