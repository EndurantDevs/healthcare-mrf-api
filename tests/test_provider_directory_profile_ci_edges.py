# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile as profile


importer = importlib.import_module("process.provider_directory_fhir")


def _build() -> importer._ProviderDirectoryProfileBuild:
    build_id = f"pdpb_{'a' * 32}"
    return importer._ProviderDirectoryProfileBuild(
        schema="mrf",
        generation_id="generation-a",
        source_ids=("source-a",),
        retained_source_ids=("source-a",),
        dataset_ids=("dataset-a",),
        profile_as_of="2026-07-20",
        evidence_stage=profile.profile_evidence_stage_table_name(build_id),
        profile_stage=profile.profile_stage_table_name(build_id),
        resume_lineage_hash="a" * 64,
        build_id=build_id,
        owner_run_id="run-a",
    )


def _dataset(
    *,
    expected_resources: tuple[str, ...] = ("Practitioner",),
    source_verification_contract: tuple[object, ...] = (),
    source_verification_contract_hash: str | None = None,
) -> importer.ProviderDirectoryArtifactDataset:
    return importer.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        evidence_run_id="run-a",
        selected_resources=expected_resources,
        expected_resources=expected_resources,
        recorded_expected_resources=None,
        source_verification_contract=source_verification_contract,
        source_verification_contract_hash=source_verification_contract_hash,
    )


def _checkpoint_map(
    build: importer._ProviderDirectoryProfileBuild,
) -> dict[str, object]:
    return {
        "build_id": build.build_id,
        "evidence_stage": build.evidence_stage,
        "profile_stage": build.profile_stage,
        "evidence_stage_oid": None,
        "profile_stage_oid": 32,
    }


def test_profile_source_context_validation_edges():
    assert importer._profile_source_context_from_row({"source_id": None}) is None

    with pytest.raises(RuntimeError, match="source_context_invalid"):
        importer._profile_source_context_from_row({"source_id": "source-a"})

    with pytest.raises(RuntimeError, match="source_context_missing"):
        importer._profile_source_context_from_row(
            {
                "source_id": "source-a",
                "endpoint_id": "endpoint-a",
                "canonical_api_base": "https://example.test/fhir",
                "org_name": "Example Health",
            }
        )

    with pytest.raises(RuntimeError, match="source_context_invalid"):
        importer._profile_source_context_from_row(
            {
                "source_id": "source-a",
                "endpoint_id": "endpoint-a",
                "canonical_api_base": "https://example.test/fhir",
                "org_name": None,
                "plan_name": None,
            }
        )


@pytest.mark.asyncio
async def test_profile_scope_blank_and_duplicate_rows(monkeypatch):
    valid_source_by_field = {
        "source_id": "source-a",
        "endpoint_id": "endpoint-a",
        "canonical_api_base": "https://example.test/fhir",
        "org_name": "Example Health",
        "plan_name": None,
    }
    source_query = AsyncMock(
        return_value=[{"source_id": None}, valid_source_by_field]
    )
    monkeypatch.setattr(importer.db, "all", source_query)
    monkeypatch.setattr(
        profile,
        "configured_profile_source_ids",
        lambda: ("source-a",),
    )

    selected_ids, retained_ids, contexts = (
        await importer._provider_directory_profile_scope_source_ids(
            "mrf",
            {"source-a"},
        )
    )
    assert selected_ids == ["source-a"]
    assert retained_ids == ["source-a"]
    assert contexts[0].org_name == "Example Health"

    source_query.return_value = [
        valid_source_by_field,
        valid_source_by_field,
    ]
    with pytest.raises(RuntimeError, match="source_context_ambiguous"):
        await importer._provider_directory_profile_scope_source_ids(
            "mrf",
            {"source-a"},
        )


@pytest.mark.asyncio
async def test_profile_cleanup_and_stale_oid_rejection(monkeypatch):
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)
    await importer._remove_provider_directory_profile_stage_table(
        "mrf",
        "profile_stage",
    )
    status.assert_awaited_once()
    assert '"mrf"."profile_stage"' in str(status.await_args.args[0])

    @contextlib.asynccontextmanager
    async def transaction():
        yield

    build = _build()
    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(
        importer.db,
        "all",
        AsyncMock(return_value=[{"build_id": build.build_id}]),
    )
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value=_checkpoint_map(build)),
    )
    with pytest.raises(RuntimeError, match="stale_checkpoint_oid_invalid"):
        await importer._reap_stale_provider_directory_profile_builds(
            "mrf",
            current_build_id=f"pdpb_{'b' * 32}",
        )


@pytest.mark.asyncio
async def test_profile_cutover_deletes_exact_checkpoint(monkeypatch):
    checkpoint = ("mrf", "profile-build-a")
    stages = (
        SimpleNamespace(stage_table="evidence-stage", resume_checkpoint=checkpoint),
        SimpleNamespace(stage_table="profile-stage", resume_checkpoint=checkpoint),
    )
    promote = AsyncMock()
    delete_checkpoint = AsyncMock()
    remove_stage = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_retry_provider_directory_artifact_bundle_promotion",
        promote,
    )
    monkeypatch.setattr(
        importer,
        "_delete_provider_directory_profile_build_checkpoint",
        delete_checkpoint,
    )
    monkeypatch.setattr(
        importer,
        "_remove_provider_directory_artifact_stage",
        remove_stage,
    )

    metrics_by_name = {"profile_rows": 2}
    assert await importer._finalize_provider_directory_profile_stages(
        metrics_by_name,
        stages,
        defer_cutover=False,
    ) == metrics_by_name
    delete_checkpoint.assert_awaited_once_with(*checkpoint)
    assert remove_stage.await_count == 2


@pytest.mark.asyncio
async def test_profile_publish_returns_finalized_metrics(monkeypatch):
    dataset_fence = importer.ProviderDirectoryArtifactDatasetFence((_dataset(),))
    build = _build()
    metrics_by_name = {"profile_rows": 2}
    stages = (
        SimpleNamespace(stage_table="evidence-stage"),
        SimpleNamespace(stage_table="profile-stage"),
    )

    @contextlib.asynccontextmanager
    async def build_guard(_schema, _target):
        yield importer.ProviderDirectoryArtifactBuildFence(target_oid=None)

    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_build_guard",
        build_guard,
    )
    monkeypatch.setattr(
        importer,
        "_resolve_provider_directory_profile_build",
        AsyncMock(return_value=build),
    )
    monkeypatch.setattr(
        importer,
        "_reap_stale_provider_directory_profile_builds",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        importer,
        "_build_provider_directory_profile_stages",
        AsyncMock(return_value=(metrics_by_name, stages)),
    )
    finalize = AsyncMock(return_value=metrics_by_name)
    monkeypatch.setattr(
        importer,
        "_finalize_provider_directory_profile_stages",
        finalize,
    )
    fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(
        dataset_fence
    )
    try:
        assert await importer.publish_provider_directory_profile(
            run_id="run-a"
        ) == metrics_by_name
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(fence_token)
    finalize.assert_awaited_once_with(
        metrics_by_name,
        stages,
        defer_cutover=False,
    )


@pytest.mark.parametrize("raw_profile", ["not-json", {}, ["Practitioner"] * 2])
def test_artifact_resource_profile_rejects_invalid_shapes(raw_profile):
    with pytest.raises(RuntimeError, match="dataset_profile_invalid"):
        importer._artifact_dataset_resource_profile(
            raw_profile,
            dataset_id="dataset-a",
            field_name="expected_resources",
        )


def test_artifact_verification_source_ids_require_unique_values():
    with pytest.raises(RuntimeError, match="candidate_source_ids_invalid"):
        importer._artifact_verification_source_ids(
            {"source_ids": ["source-a", "source-a"]},
            "dataset-a",
            {"source_id": "source-a"},
            is_required=True,
        )


@pytest.mark.asyncio
async def test_artifact_scope_helpers_cover_empty_and_guarded_paths(monkeypatch):
    entered_events = []

    @contextlib.asynccontextmanager
    async def build_guard(schema, target_name):
        entered_events.append((schema, target_name))
        yield importer.ProviderDirectoryArtifactBuildFence(target_oid=None)

    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_build_guard",
        build_guard,
    )
    async with importer._provider_directory_artifact_scope_guard("mrf"):
        entered_events.append(("mrf", "body"))
    assert entered_events == [
        ("mrf", "provider_directory_artifact_scope"),
        ("mrf", "body"),
    ]

    monkeypatch.setattr(importer.os, "getpid", lambda: 7)
    monkeypatch.setattr(importer.time, "time_ns", lambda: 11)
    monkeypatch.setattr(importer.os, "urandom", lambda _size: b"a" * 8)
    table_name = importer._provider_directory_artifact_scope_table_name(
        "provider_directory_dataset_resource",
        "run-a",
    )
    assert table_name.startswith("provider_directory_dataset_resource_artifact_scope_")
    assert len(table_name) <= 63

    model_without_pk = SimpleNamespace(
        __table__=SimpleNamespace(
            primary_key=SimpleNamespace(columns=()),
        )
    )
    assert importer._artifact_scope_pk_sql(
        model_without_pk,
        "mrf",
        "artifact-stage",
    ) == ()

    empty_fence = importer.ProviderDirectoryArtifactDatasetFence(())
    await importer._lock_and_verify_artifact_dataset_fence(empty_fence)


@pytest.mark.asyncio
async def test_artifact_fence_rejects_endpoint_and_source_drift():
    dataset = _dataset()
    dataset_fence = importer.ProviderDirectoryArtifactDatasetFence((dataset,))
    executor = SimpleNamespace(all=AsyncMock(return_value=[]))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="api_endpoint_changed",
    ):
        await importer._lock_artifact_fence_endpoints(dataset_fence, executor)

    locked_alias_by_field = {
        "source_id": "source-a",
        "endpoint_id": "endpoint-a",
        "source_record_json": {
            "source_id": "source-a",
            "endpoint_id": "endpoint-a",
            "expected_resources_json": ["Organization"],
        },
    }
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="expected_metadata_changed",
    ):
        importer._assert_locked_artifact_fence_aliases(
            dataset_fence,
            [locked_alias_by_field],
        )

    invalid_contract_dataset = _dataset(
        source_verification_contract=("contract",),
        source_verification_contract_hash="not-the-contract-hash",
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="verification_contract_invalid",
    ):
        importer._assert_locked_artifact_source_contract(
            invalid_contract_dataset,
            {},
        )


def test_artifact_dataset_identity_helpers():
    dataset = _dataset()
    dataset_fence = importer.ProviderDirectoryArtifactDatasetFence((dataset,))
    assert dataset_fence.source_endpoint_dataset_tuples == (
        ("source-a", "endpoint-a", "dataset-a"),
    )
    assert importer._validated_artifact_dataset_ids(
        [
            {
                "dataset_id": "dataset-a",
                "status": importer.ENDPOINT_DATASET_VALIDATED,
                "is_current": False,
                "superseded_at": None,
            }
        ]
    ) == ["dataset-a"]
