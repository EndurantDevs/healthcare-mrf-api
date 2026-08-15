# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed single-root automatic publication tests."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import provider_directory_source_catalog_outcomes as catalog_outcomes
from process.provider_directory_validated_publication_contract import (
    AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY,
    AUTOMATIC_REVIEWED_TWIN_ROOT_PUBLICATION_POLICY,
    ValidatedPublicationCandidate,
)
from tests.test_provider_directory_generic_admission_publication import (
    ADMISSION_SHA256,
    _generic_candidate_map,
    _generic_catalog_fixture,
    _generic_publication_fence,
    _locked_candidate_row_map,
    importer,
)
from tests.test_provider_directory_source_outcomes import _MappingResult
from tests.test_provider_directory_validated_publication_contract import (
    CANDIDATE_HASH,
    CANDIDATE_ID,
    CANDIDATE_ROOT,
    CAMPAIGN_ID,
    CURRENT_DATASET_ID,
    ENDPOINT_ID,
    PROOF_HASH,
    SCOPE_HASH,
    SOURCE_ID,
    VALIDATED_AT,
)


def _reviewed_candidate_map(*, root_count=1, first_publication=None):
    is_first_publication = (
        root_count == 1
        if first_publication is None
        else first_publication
    )
    candidate_map = {
        **_generic_candidate_map(first_publication=is_first_publication),
        "automatic_publication_policy": (
            AUTOMATIC_REVIEWED_SINGLE_ROOT_PUBLICATION_POLICY
            if root_count == 1
            else AUTOMATIC_REVIEWED_TWIN_ROOT_PUBLICATION_POLICY
        ),
        "completion_proof_required_version": (
            importer.SERVER_ISSUED_SUBSET_REQUIRED_VERSION
        ),
        "completion_proof_sha256": PROOF_HASH,
        "verification_campaign_id": CAMPAIGN_ID,
        "verification_source_scope_sha256": SCOPE_HASH,
    }
    candidate_map.pop("source_catalog_entry_id", None)
    candidate_map.pop("source_catalog_digest_sha256", None)
    if root_count == 2:
        candidate_map.pop("content_proof_admission_sha256")
    return candidate_map


def _reviewed_publication_fence(*, root_count=1, first_publication=None):
    candidate = ValidatedPublicationCandidate.from_payload(
        _reviewed_candidate_map(
            root_count=root_count,
            first_publication=first_publication,
        )
    )
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id=SOURCE_ID,
        endpoint_id=ENDPOINT_ID,
        dataset_id=CANDIDATE_ID,
        evidence_run_id=CANDIDATE_ROOT,
        status=importer.ENDPOINT_DATASET_VALIDATED,
        is_current=False,
        previous_dataset_id=(
            candidate.expected_current.dataset_id
            if candidate.expected_current is not None
            else None
        ),
        expected_incumbent_dataset_id=(
            candidate.expected_current.dataset_id
            if candidate.expected_current is not None
            else None
        ),
        promote_on_cutover=True,
        dataset_hash=CANDIDATE_HASH,
        validated_at=VALIDATED_AT,
        content_proof_admission_sha256=(
            ADMISSION_SHA256 if root_count == 1 else None
        ),
        generic_admission_sealed=root_count == 1,
        artifact_selection_receipt_present=root_count == 1,
        completion_proof_required_version=(
            importer.SERVER_ISSUED_SUBSET_REQUIRED_VERSION
        ),
        completion_proof_sha256=PROOF_HASH,
        verification_campaign_id=CAMPAIGN_ID,
        verification_source_scope_hash=SCOPE_HASH,
        verification_source_status=(
            importer.PROVIDER_DIRECTORY_ROOT_POLICY_VERIFIED
        ),
        verification_source_ids=(SOURCE_ID,),
        reviewed_root_policy=importer.ReviewedRootPolicy(root_count),
    )
    return importer.ProviderDirectoryArtifactDatasetFence(
        (dataset,),
        should_select_validated_candidates=True,
        validated_publication_candidate=candidate,
    )


@pytest.mark.asyncio
async def test_reviewed_candidate_resolution_isolated_and_activation_bound(
    monkeypatch,
):
    dataset = _reviewed_publication_fence().datasets[0]

    async def resolve(source_ids, **_kwargs):
        if source_ids == ["source-pending"]:
            raise RuntimeError("pending")
        return SimpleNamespace(datasets=(dataset,))

    monkeypatch.setattr(
        importer,
        "_resolve_provider_directory_artifact_datasets",
        resolve,
    )
    is_activation_valid = AsyncMock(return_value=True)
    monkeypatch.setattr(catalog_outcomes.db, "scalar", is_activation_valid)

    assert await catalog_outcomes._canonical_validated_datasets_by_source_id(
        ["source-pending", SOURCE_ID]
    ) == {SOURCE_ID: dataset}
    is_activation_valid.return_value = False
    assert await catalog_outcomes._canonical_validated_datasets_by_source_id(
        [SOURCE_ID]
    ) == {}


@pytest.mark.asyncio
async def test_reviewed_resolution_rejects_cross_source_and_isolates_errors(
    monkeypatch,
):
    dataset = _reviewed_publication_fence().datasets[0]
    error_dataset = replace(
        dataset,
        source_id="source-error",
        verification_source_ids=("source-error",),
    )

    async def resolve(source_ids, **_kwargs):
        return SimpleNamespace(
            datasets=(
                error_dataset if source_ids == ["source-error"] else dataset,
            )
        )

    async def is_activation_valid(_query, *, source_id):
        if source_id == "source-error":
            raise RuntimeError("unavailable")
        return True

    monkeypatch.setattr(
        importer,
        "_resolve_provider_directory_artifact_datasets",
        resolve,
    )
    monkeypatch.setattr(
        catalog_outcomes.db,
        "scalar",
        is_activation_valid,
    )

    assert await catalog_outcomes._canonical_validated_datasets_by_source_id(
        ["source-cross", "source-error", SOURCE_ID]
    ) == {SOURCE_ID: dataset}


def test_reviewed_policy_is_closed_and_allows_first_publication():
    candidate_by_field = _reviewed_candidate_map()

    assert (
        ValidatedPublicationCandidate.from_payload(
            candidate_by_field
        ).to_payload()
        == candidate_by_field
    )
    for invalid_version in (None, 2, "3", 3.0, True):
        with pytest.raises(ValueError, match="completion_proof_pair_invalid"):
            ValidatedPublicationCandidate.from_payload(
                {
                    **candidate_by_field,
                    "completion_proof_required_version": invalid_version,
                }
            )


def _configure_reviewed_canonical_dataset(
    canonical_dataset,
    *,
    root_count,
    is_first_publication,
):
    incumbent_id = None if is_first_publication else CURRENT_DATASET_ID
    canonical_dataset.expected_incumbent_dataset_id = incumbent_id
    canonical_dataset.previous_dataset_id = incumbent_id
    canonical_dataset.resource_count = 123
    canonical_dataset.reviewed_root_policy = importer.ReviewedRootPolicy(root_count)
    canonical_dataset.completion_proof_required_version = (
        importer.SERVER_ISSUED_SUBSET_REQUIRED_VERSION
    )
    canonical_dataset.completion_proof_sha256 = PROOF_HASH
    canonical_dataset.completion_proof_cutoff = "2026-08-13T00:00:00+00:00"
    canonical_dataset.verification_campaign_id = CAMPAIGN_ID
    canonical_dataset.verification_source_scope_hash = SCOPE_HASH
    canonical_dataset.verification_source_status = (
        importer.PROVIDER_DIRECTORY_ROOT_POLICY_VERIFIED
    )
    canonical_dataset.promote_on_cutover = True


def _reviewed_catalog_fixture(
    monkeypatch,
    *,
    root_count=1,
    first_publication=True,
):
    canonical_resolver = (
        catalog_outcomes._canonical_validated_datasets_by_source_id
    )
    catalog_map, canonical_dataset, dataset_rows, execute, _ = (
        _generic_catalog_fixture(monkeypatch)
    )
    catalog_map["items"][0].update(
        classification="manual_acquisition",
        runnable=False,
    )
    _configure_reviewed_canonical_dataset(
        canonical_dataset,
        root_count=root_count,
        is_first_publication=first_publication,
    )
    if first_publication:
        dataset_rows[0]["endpoint_id"] = "endpoint-serving-old"
        dataset_rows[1]["previous_dataset_id"] = None
    execute.return_value = _MappingResult(dataset_rows)
    resolve_candidate = AsyncMock(
        return_value=SimpleNamespace(datasets=(canonical_dataset,))
    )
    is_activation_valid = AsyncMock(return_value=True)
    monkeypatch.setattr(
        catalog_outcomes,
        "_canonical_validated_datasets_by_source_id",
        canonical_resolver,
    )
    monkeypatch.setattr(
        importer,
        "_resolve_provider_directory_artifact_datasets",
        resolve_candidate,
    )
    monkeypatch.setattr(
        catalog_outcomes.db,
        "scalar",
        is_activation_valid,
    )
    return catalog_map, canonical_dataset, resolve_candidate, is_activation_valid


@pytest.mark.asyncio
async def test_catalog_exposes_cross_endpoint_reviewed_first_publication(
    monkeypatch,
):
    catalog_map, _, resolve_candidate, is_activation_valid = (
        _reviewed_catalog_fixture(monkeypatch)
    )

    enriched_catalog = (
        await catalog_outcomes.enrich_provider_directory_source_catalog(
            catalog_map
        )
    )

    assert enriched_catalog["items"][0]["validated_publication_candidate"] == (
        _reviewed_candidate_map()
    )
    assert enriched_catalog["items"][0]["outcome_summary"] == {
        "dataset_id": CANDIDATE_ID,
        "status": "validated",
        "is_current": False,
        "total_resources": 123,
        "validated_at": VALIDATED_AT,
    }
    assert enriched_catalog["items"][0]["current_outcome_summary"][
        "endpoint_id"
    ] == "endpoint-serving-old"
    assert resolve_candidate.await_args.args == ([SOURCE_ID],)
    assert is_activation_valid.await_args.kwargs["source_id"] == SOURCE_ID


@pytest.mark.asyncio
async def test_catalog_rejects_reviewed_candidate_on_ordinary_lane(monkeypatch):
    catalog_map, _, _, _ = _reviewed_catalog_fixture(monkeypatch)
    catalog_map["items"][0].update(
        classification="acquisition",
        runnable=True,
    )

    enriched_catalog = (
        await catalog_outcomes.enrich_provider_directory_source_catalog(
            catalog_map
        )
    )

    assert "validated_publication_candidate" not in enriched_catalog["items"][0]


def _recording_async_step(events, name, return_value=None):
    return AsyncMock(
        side_effect=lambda *_args, **_kwargs: (
            events.append(name),
            return_value,
        )[1]
    )


def _stub_locked_fence_path(monkeypatch, events):
    for function_name, event_name, return_value in (
        ("_lock_artifact_fence_endpoint_advisories", "advisory", None),
        ("_lock_artifact_fence_endpoints", "endpoints", None),
        ("_lock_artifact_fence_aliases", "aliases", []),
        ("_artifact_fence_dataset_rows", "dataset_rows", []),
        ("_artifact_eligible_validated_ids", "eligible", {}),
        ("_assert_uhc_flex_profile_fence_ready", "readiness", None),
    ):
        monkeypatch.setattr(
            importer,
            function_name,
            _recording_async_step(events, event_name, return_value),
        )
    monkeypatch.setattr(
        importer,
        "_assert_locked_artifact_fence_aliases",
        lambda *_args: events.append("alias_assert"),
    )
    monkeypatch.setattr(
        importer,
        "_assert_locked_artifact_fence_datasets",
        lambda *_args: events.append("dataset_assert"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("root_count", (1, 2))
async def test_locked_reviewed_activation_is_rechecked(monkeypatch, root_count):
    fence = _reviewed_publication_fence(root_count=root_count)
    events: list[str] = []
    _stub_locked_fence_path(monkeypatch, events)
    executor = SimpleNamespace(
        scalar=_recording_async_step(events, "activation", True)
    )

    await importer._lock_and_verify_artifact_dataset_fence(fence, executor)
    assert events[-3:] == ["dataset_assert", "activation", "readiness"]

    executor.scalar.return_value = False
    executor.scalar.side_effect = None
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        await importer._assert_reviewed_activation_ready(fence, executor)
    executor.scalar.side_effect = RuntimeError("unavailable")
    with pytest.raises(RuntimeError, match="unavailable"):
        await importer._assert_reviewed_activation_ready(fence, executor)

    executor.scalar.reset_mock(side_effect=True)
    await importer._assert_reviewed_activation_ready(
        _generic_publication_fence(),
        executor,
    )
    executor.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field_name", "drifted_value"),
    (
        ("verification_source_status", None),
        ("verification_source_status", "pending_reviewed_subset_acquisition"),
        ("generic_admission_sealed", False),
        ("artifact_selection_receipt_present", False),
        ("completion_proof_required_version", 2),
        ("completion_proof_sha256", "F" * 64),
        ("verification_campaign_id", None),
        ("verification_source_scope_hash", None),
    ),
)
async def test_catalog_rejects_reviewed_authority_drift(
    monkeypatch,
    field_name,
    drifted_value,
):
    catalog_map, canonical_dataset, _, _ = _reviewed_catalog_fixture(
        monkeypatch
    )
    setattr(canonical_dataset, field_name, drifted_value)

    enriched_catalog = (
        await catalog_outcomes.enrich_provider_directory_source_catalog(
            catalog_map
        )
    )

    assert "validated_publication_candidate" not in enriched_catalog["items"][0]


def _reviewed_locked_candidate_row(*, first_publication=True, **changes):
    return {
        **_locked_candidate_row_map(),
        "previous_dataset_id": (
            None if first_publication else CURRENT_DATASET_ID
        ),
        "completion_proof_required_version": (
            importer.SERVER_ISSUED_SUBSET_REQUIRED_VERSION
        ),
        "completion_proof_sha256": PROOF_HASH,
        "locked_current_dataset_ids": (
            [] if first_publication else [CURRENT_DATASET_ID]
        ),
        **changes,
    }


def test_locked_reviewed_first_publication_is_fail_closed():
    fence = _reviewed_publication_fence()
    candidate_row_map = _reviewed_locked_candidate_row()

    importer._assert_locked_artifact_fence_datasets(
        fence,
        [candidate_row_map],
        {ENDPOINT_ID: [CANDIDATE_ID]},
    )
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        importer._assert_locked_artifact_fence_datasets(
            replace(
                fence,
                datasets=(
                    replace(fence.datasets[0], reviewed_root_policy=None),
                ),
            ),
            [candidate_row_map],
            {ENDPOINT_ID: [CANDIDATE_ID]},
        )


@pytest.mark.parametrize(
    ("dataset_changes", "row_changes"),
    (
        ({"verification_source_status": None}, {}),
        (
            {
                "verification_source_status": (
                    "pending_reviewed_subset_acquisition"
                )
            },
            {},
        ),
        ({"reviewed_root_policy": importer.ReviewedRootPolicy(2)}, {}),
        ({"verification_campaign_id": "other-campaign"}, {}),
        ({"content_proof_admission_sha256": "f" * 64}, {}),
        ({}, {"completion_proof_sha256": "f" * 64}),
    ),
)
def test_locked_reviewed_rejects_authority_drift(
    dataset_changes,
    row_changes,
):
    fence = _reviewed_publication_fence()

    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        importer._assert_locked_artifact_fence_datasets(
            replace(
                fence,
                datasets=(replace(fence.datasets[0], **dataset_changes),),
            ),
            [_reviewed_locked_candidate_row(**row_changes)],
            {ENDPOINT_ID: [CANDIDATE_ID]},
        )
