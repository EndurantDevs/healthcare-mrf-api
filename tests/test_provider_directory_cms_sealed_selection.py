# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Truthful presealed CMS selection; shared lifecycle-guard adoption is separate."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import text

from process import provider_directory_cms_desired_fence as desired_fence
from process import provider_directory_cms_npd_recovery as recovery
from process import provider_directory_profile_selection_contract as selection
from process.provider_directory_profile_selection_dataset import _cms_dataset_pair
from tests.test_provider_directory_artifact_eligibility_fence import _dataset_row
from tests.test_provider_directory_cms_desired_fence import _backend, _fixture, _pair, fhir
from tests.test_provider_directory_profile_selection_desired import _desired, _rows, _selection_rows, snapshot


def _sealed():
    cms, other, execution = _fixture()
    cms = replace(cms, status="published", published_at="2026-10-29 00:00:00")
    execution.attestation.desired_cms_dataset = _pair(cms)
    execution.attestation.pairs = (_pair(cms), _pair(other))
    return cms, other, execution


@pytest.mark.parametrize("incumbent", [True, False])
def test_published_noncurrent_is_a_replacement_not_current_refresh(incumbent):
    desired = _desired(incumbent=incumbent)
    desired["desired_cms_dataset"]["publication_status"] = "published"
    validated = selection._validated_desired_selection(desired)
    assert validated == desired
    with pytest.raises(selection.ProviderDirectoryProfileSelectionError, match="publication authority"):
        selection._validated_pair(desired["desired_cms_dataset"])
    row = _rows(desired["desired_cms_dataset"])
    row["published_at"] = "2026-10-29 00:00:00"
    assert _cms_dataset_pair(row, allow_desired=True) == desired["desired_cms_dataset"]
    row["published_at"] = None
    with pytest.raises(selection.ProviderDirectoryProfileSelectionDrift):
        _cms_dataset_pair(row, allow_desired=True)


def test_published_noncurrent_preserves_exact_incumbent_and_source_checks():
    desired = _desired()
    desired["desired_cms_dataset"]["publication_status"] = "published"
    for field, value in (("source_id", "other-source"), ("dataset_id", "cms-current"), ("is_current", 1)):
        changed = deepcopy(desired)
        changed["desired_cms_dataset"][field] = value
        with pytest.raises(selection.ProviderDirectoryProfileSelectionError):
            selection._validated_desired_selection(changed)
    current = _desired(current=True)
    current["expected_cms_incumbent"] = None
    with pytest.raises(selection.ProviderDirectoryProfileSelectionError, match="differs from incumbent"):
        selection._validated_desired_selection(current)


@pytest.mark.asyncio
async def test_sealed_desired_route_is_exact_and_does_not_enable_other_candidates(monkeypatch):
    cms, other, execution = _sealed()
    backend = _backend(cms, other)
    resolve = AsyncMock(return_value=fhir.ProviderDirectoryArtifactDatasetFence((cms,)))
    monkeypatch.setattr(desired_fence, "_resolve_sealed_candidate", resolve)
    fence = await desired_fence.resolve_desired_fence(backend, execution)
    assert backend._resolve_provider_directory_artifact_datasets.await_count == 1
    assert backend._resolve_provider_directory_artifact_datasets.await_args.args == (["source-a"],)
    assert backend._resolve_provider_directory_artifact_datasets.await_args.kwargs == {
        "should_select_validated_candidates": False
    }
    resolve.assert_awaited_once_with(backend, execution.attestation.desired_cms_dataset)
    assert fence.promotion_datasets == [cms] and fence.should_select_validated_candidates is False


@pytest.mark.parametrize(
    "change",
    [
        {"source_id": "other"},
        {"published_at": None},
        {"superseded_at": "later"},
        {"is_current": True},
        {"previous_dataset_id": "different"},
    ],
)
def test_only_native_sealed_cms_row_can_be_a_noncurrent_promotion(change):
    row_by_field = {
        "source_id": "cms-npd",
        "status": "published",
        "is_current": False,
        "published_at": "2026-10-29 00:00:00",
        "superseded_at": None,
        "promote_on_cutover": True,
        "current_dataset_count": 1,
        "current_dataset_id": "old",
        "validated_candidate_count": 1,
        "previous_dataset_id": "old",
    }
    accepted = fhir._artifact_dataset_selection_from_row(row_by_field, "new", "endpoint")
    assert accepted["published_at"] == row_by_field["published_at"]
    row_by_field.update(change)
    with pytest.raises(RuntimeError):
        fhir._artifact_dataset_selection_from_row(row_by_field, "new", "endpoint")


def test_sealed_promotion_retains_all_eligibility_and_changes_only_pointer():
    cms, _, _ = _sealed()
    ordinary = replace(cms, status="validated", published_at=None)
    old_sql, old_params = fhir._artifact_dataset_publication_statement(ordinary)
    sealed_sql, sealed_params = fhir._artifact_dataset_publication_statement(cms)
    assert "SET is_current = true," in sealed_sql
    assert "SET status =" not in sealed_sql and "published_at = now()" not in sealed_sql
    assert "AND status = :published_status" in sealed_sql
    assert "AND published_at=CAST(CAST(:cms_sealed_at AS text) AS timestamp)" in sealed_sql
    assert "candidate.eligibility_metadata_jsonb->'source_ids'='[\"cms-npd\"]'::jsonb" in sealed_sql
    for gate in (
        "previous_dataset_id IS NOT DISTINCT FROM :incumbent_dataset_id",
        "dataset_hash IS NOT DISTINCT FROM :dataset_hash",
        "IS NOT DISTINCT FROM :evidence_run_id",
        "AND validated_at IS NOT NULL",
        "AND superseded_at IS NULL",
    ):
        assert gate in sealed_sql
    suffix = "AND (\n                    NOT ("
    assert sealed_sql.split(suffix, 1)[1] == old_sql.split(suffix, 1)[1]
    expected_parameters_by_name = {key: value for key, value in old_params.items() if key != "validated_status"}
    assert sealed_params == {**expected_parameters_by_name, "cms_sealed_at": cms.published_at}
    assert text(sealed_sql).bindparams(**sealed_params) is not None
    assert fhir._artifact_dataset_publication_statement(replace(ordinary, source_id="ordinary")) == (
        old_sql,
        old_params,
    )
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="seal_required"):
        fhir._artifact_dataset_publication_statement(replace(cms, published_at=None))


def test_exact_seal_timestamp_and_supersession_are_part_of_recheck(monkeypatch):
    cms, _, _ = _sealed()
    row_by_field = {"published_at": cms.published_at, "superseded_at": None}
    expected = ("same",)
    monkeypatch.setattr(fhir, "_artifact_fence_dataset_row_identity", lambda _: expected)
    monkeypatch.setattr(fhir, "_expected_artifact_fence_dataset_identity", lambda _: expected)
    assert fhir._is_artifact_fence_dataset_row_exact(cms, row_by_field)
    for change in ({"published_at": "later"}, {"superseded_at": "later"}):
        assert not fhir._is_artifact_fence_dataset_row_exact(cms, {**row_by_field, **change})
    fence = fhir.ProviderDirectoryArtifactDatasetFence((cms,))
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="selection_changed"):
        fhir._assert_artifact_fence_selection_unchanged(
            fence, fhir.ProviderDirectoryArtifactDatasetFence((replace(cms, published_at="later"),))
        )


@pytest.mark.asyncio
async def test_acquisition_recovery_excludes_sealed_noncurrent_state_without_disposition():
    query = AsyncMock(return_value=("current",))
    database = SimpleNamespace(first=query, all=AsyncMock(return_value=[]), status=AsyncMock())
    backend = SimpleNamespace(
        db=database,
        _qt=lambda schema, name: schema + "." + name,
        _schema=lambda: "synthetic",
        ENDPOINT_DATASET_ACQUIRING="acquiring",
        ENDPOINT_DATASET_VALIDATED="validated",
        ENDPOINT_DATASET_PUBLISHED="published",
    )
    identity_by_field = {"source_id": "cms-npd", "vector_sha256": "a" * 64}
    assert await recovery.reusable_vector_candidate(backend, "endpoint", identity_by_field) == "current"
    sql = query.await_args.args[0]
    assert "candidate.status=:published AND candidate.is_current=true" in sql
    assert "candidate.is_current=false" not in sql
    await recovery._repair_candidate_state(backend, "endpoint", identity_by_field)
    repair_sql = database.all.await_args.args[0]
    assert "status=:published AND is_current=true" in repair_sql
    assert "is_current=false" not in repair_sql
    database.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_sealed_query_keeps_exact_identity_proofs_and_ordinary_selector_unchanged(monkeypatch):
    cms, _, _ = _sealed()
    expected = fhir.ProviderDirectoryArtifactDatasetFence((cms,))
    dataset_rows = [{"dataset_id": cms.dataset_id}]
    query = AsyncMock(return_value=dataset_rows)
    validate = Mock(return_value=expected)
    monkeypatch.setattr(fhir.db, "all", query)
    monkeypatch.setattr(fhir, "_validate_provider_directory_artifact_datasets", validate)
    ordinary_sql = fhir._provider_directory_artifact_dataset_selection_sql(["cms-npd"])
    selected_fence = await desired_fence._resolve_sealed_candidate(fhir, _pair(cms))
    assert selected_fence is expected
    validate.assert_called_once_with(dataset_rows, ["cms-npd"])
    assert query.await_args.kwargs == {"dataset_id": cms.dataset_id, "endpoint_id": cms.endpoint_id}
    sql = query.await_args.args[0]
    for gate in (
        "source.source_id='cms-npd'",
        "dataset.dataset_id=:dataset_id",
        "dataset.endpoint_id=:endpoint_id",
        "dataset.status='published'",
        "dataset.is_current IS FALSE",
        "dataset.validated_at IS NOT NULL",
        "dataset.published_at IS NOT NULL",
        "dataset.superseded_at IS NULL",
        "publication.full_metadata_jsonb->'source_ids'='[\"cms-npd\"]'::jsonb",
        "cms_stale.dataset_id = dataset.dataset_id",
        "count(*) OVER () AS validated_candidate_count",
    ):
        assert gate in sql
    metadata = "publication.full_metadata_jsonb"
    dataset_ref = fhir._unscoped_qt(fhir._schema(), fhir.ProviderDirectoryEndpointDataset.__tablename__)
    source_ref = fhir._unscoped_qt(fhir._schema(), fhir.ProviderDirectorySource.__tablename__)
    eligibility = fhir._artifact_reviewed_candidate_eligibility_sql(
        dataset_ref,
        source_ref,
        metadata=metadata,
        content_proof_admitted=fhir._artifact_content_proof_admitted_marker_sql(metadata),
        use_admission_seals=False,
    )
    assert eligibility in sql
    assert set(text(sql)._bindparams) == {"dataset_id", "endpoint_id"}
    assert fhir._provider_directory_artifact_dataset_selection_sql(["cms-npd"]) == ordinary_sql
    assert "dataset.is_current = true" in ordinary_sql
    assert "dataset.dataset_id=:dataset_id" not in ordinary_sql


def test_locked_sealed_fence_preserves_real_state_and_exact_incumbent():
    cms, _, _ = _sealed()
    cms = replace(cms, previous_dataset_id="cms-old")
    selected = _dataset_row(cms.dataset_id, cms.evidence_run_id, "published", False, "cms-old")
    selected.update(endpoint_id=cms.endpoint_id, dataset_hash=cms.dataset_hash, published_at=cms.published_at)
    incumbent = _dataset_row("cms-old", "old-root", "published", True)
    incumbent.update(endpoint_id=cms.endpoint_id)
    fence = fhir.ProviderDirectoryArtifactDatasetFence((cms,))
    fhir._assert_locked_artifact_fence_datasets(fence, [selected, incumbent], {})
    assert selected["status"] == "published" and selected["is_current"] is False
    for changed in ({**selected, "published_at": "drift"}, {**selected, "status": "validated"}):
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="metadata_changed"):
            fhir._assert_locked_artifact_fence_datasets(fence, [changed, incumbent], {})
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="current_changed"):
        fhir._assert_locked_artifact_fence_datasets(fence, [selected], {})


def test_global_desired_attestation_preserves_true_sealed_state_and_timestamp_identity():
    desired = _desired()
    desired["desired_cms_dataset"]["publication_status"] = "published"
    catalog, sources, current_rows, candidate = _selection_rows(desired)
    candidate["published_at"] = "2026-10-29 00:00:00"

    def compute():
        return snapshot._computed_desired_selection_from_rows(
            catalog,
            node_id="synthetic-node",
            source_rows=sources,
            dataset_rows=current_rows,
            desired_dataset_row=candidate,
            desired_selection=desired,
        )

    computed = compute()
    payload = {**computed.identity_payload, "authority_revision": 1}
    payload["proof_id"] = selection._proof_id(payload)
    proof = selection.validated_profile_selection_attestation(payload)
    assert proof.desired_cms_dataset == desired["desired_cms_dataset"]
    assert proof.expected_cms_incumbent == desired["expected_cms_incumbent"]
    candidate["published_at"] = "2026-10-29 00:00:01"
    assert compute().identity_payload != computed.identity_payload
