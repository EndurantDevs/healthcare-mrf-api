# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Narrow edge proofs for bounded Provider Directory Profile deltas."""

from __future__ import annotations

from tests.provider_directory_profile_delta_coverage_support import (
    AsyncMock,
    Mock,
    SimpleNamespace,
    _execution,
    _matching_delta_receipt,
    _matching_delta_serving_state,
    _prepared_delta,
    _ready_delta_checkpoint,
    _valid_serving_row,
    _wal_tracker_admission,
    contextlib,
    dataclasses,
    datetime,
    importer,
    json,
    profile,
    pytest,
)


@pytest.mark.asyncio
async def test_delta_receipt_replay_edges(monkeypatch):
    delta = _prepared_delta()
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    assert not await importer._is_profile_delta_receipt_replay(
        delta,
        "receipt",
    )

    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value=dict(_matching_delta_receipt(delta), build_id="x")),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="receipt_conflict",
    ):
        await importer._is_profile_delta_receipt_replay(delta, "receipt")

    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value=_matching_delta_receipt(delta)),
    )
    monkeypatch.setattr(
        importer,
        "_is_provider_directory_profile_delta_committed",
        AsyncMock(return_value=False),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="receipt_incomplete",
    ):
        await importer._is_profile_delta_receipt_replay(delta, "receipt")

    monkeypatch.setattr(
        importer,
        "_is_provider_directory_profile_delta_committed",
        AsyncMock(return_value=True),
    )
    assert await importer._is_profile_delta_receipt_replay(delta, "receipt")

@pytest.mark.asyncio
async def test_profile_adoption_input_edges(monkeypatch):
    with pytest.raises(RuntimeError, match="adoption_as_of_ambiguous"):
        importer._profile_adoption_as_of({})
    with pytest.raises(RuntimeError, match="adoption_as_of_ambiguous"):
        importer._profile_adoption_as_of({"profile_as_of": "2026-7-3"})
    assert importer._profile_adoption_as_of(
        {"profile_as_of": "2026-07-30"}
    ) == "2026-07-30"

    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="adoption_missing"):
        await importer._profile_adoption_result_row("mrf")

    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value={
                "selection_result": {},
                "finished_at": None,
            }
        ),
    )
    with pytest.raises(RuntimeError, match="adoption_invalid"):
        await importer._profile_adoption_result_row("mrf")

    with pytest.raises(RuntimeError, match="adoption_missing"):
        importer._provider_directory_profile_adoption_result(
            [],
            generation_id="pdprofile_" + "1" * 32,
        )
    with pytest.raises(RuntimeError, match="adoption_invalid"):
        importer._provider_directory_profile_adoption_result(
            {},
            generation_id="pdprofile_" + "1" * 32,
        )

@pytest.mark.asyncio
async def test_profile_adoption_vector_edges(monkeypatch):
    invalid_pairs_by_field = {
        "pairs": [
            {"source_id": "source-a", "dataset_id": "dataset-a"},
            "not-a-pair",
        ],
        "source_context_digest": "a" * 64,
    }
    with pytest.raises(RuntimeError, match="adoption_invalid"):
        await importer._profile_adoption_vectors(
            "mrf",
            invalid_pairs_by_field,
        )

    invalid_digest_by_field = {
        "pairs": [{"source_id": "source-a", "dataset_id": "dataset-a"}],
        "source_context_digest": "bad",
    }
    with pytest.raises(RuntimeError, match="adoption_invalid"):
        await importer._profile_adoption_vectors(
            "mrf",
            invalid_digest_by_field,
        )

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_scope_source_ids",
        AsyncMock(
            return_value=(
                ["source-a"],
                ["source-a"],
                (
                    importer._ProviderDirectoryProfileSourceContext(
                        source_id="source-a",
                        endpoint_id="endpoint-a",
                        canonical_api_base=None,
                        org_name="Payer",
                        plan_name=None,
                    ),
                ),
            )
        ),
    )
    source_vector, context_vector = (
        await importer._profile_adoption_vectors(
            "mrf",
            invalid_digest_by_field
            | {"source_context_digest": "a" * 64},
        )
    )
    assert source_vector == (("source-a", "dataset-a"),)
    assert context_vector[0][0] == "source-a"

def test_profile_adoption_row_count_edges():
    with pytest.raises(RuntimeError, match="adoption_invalid"):
        importer._profile_adoption_attested_row_counts(
            {
                "row_counts": {
                    "profile_rows": True,
                    "profile_source_evidence_rows": 0,
                },
                "generation": 1,
                "proof_id": "a",
                "authority_revision": 1,
                "profile_schema_version": 1,
                "profile_strategy_version": "v",
            }
        )


def _adoption_candidate():
    return importer._ProfileAdoptionCandidate(
        generation_id="pdprofile_" + "1" * 32,
        selection_result={
            "generation": 1,
            "proof_id": "a" * 64,
            "authority_revision": 1,
            "profile_schema_version": 1,
            "profile_strategy_version": profile.PROFILE_BUILD_STRATEGY_VERSION,
        },
        source_vector=(("source-a", "dataset-a"),),
        source_context_vector=(("source-a", "a" * 64),),
        profile_as_of="2026-07-30",
        profile_target_oid=12,
        evidence_target_oid=11,
        profile_rows=1,
        evidence_rows=1,
        published_at="2026-07-30T00:00:00Z",
        executable_plan_hash="b" * 64,
    )


def test_profile_adoption_missing_serving_edge():
    candidate = _adoption_candidate()
    with pytest.raises(RuntimeError, match="adoption_conflict"):
        importer._validate_profile_adoption_serving(None, candidate)


def _adoption_serving(candidate):
    return importer._ProviderDirectoryProfileServingState(
        status="published",
        operation="publish",
        control_generation=1,
        generation_id=candidate.generation_id,
        selection_proof_id="a" * 64,
        authority_revision=1,
        profile_schema_version=1,
        profile_strategy_version=profile.PROFILE_BUILD_STRATEGY_VERSION,
        source_vector=candidate.source_vector,
        source_vector_hash=(
            importer._provider_directory_profile_source_vector_hash(
                candidate.source_vector
            )
        ),
        source_context_vector=candidate.source_context_vector,
        source_context_vector_hash=(
            importer._provider_directory_profile_source_context_vector_hash(
                candidate.source_context_vector
            )
        ),
        executable_plan_hash=candidate.executable_plan_hash,
        evidence_target_oid=candidate.evidence_target_oid,
        profile_target_oid=candidate.profile_target_oid,
        evidence_rows=1,
        profile_rows=1,
        profile_as_of=candidate.profile_as_of,
        published_at=candidate.published_at,
    )


def test_profile_adoption_matching_serving_edge():
    candidate = _adoption_candidate()
    serving = _adoption_serving(candidate)
    assert importer._validate_profile_adoption_serving(
        serving,
        candidate,
    ) == serving

@pytest.mark.asyncio
async def test_toast_projection_identity_edges(monkeypatch):
    assert await importer._provider_directory_profile_toast_chunk_count(
        source_sql="SELECT 1",
        relation_oid=1,
        toast_oid=None,
        toastable_columns=(),
        expected_compression="pglz",
    ) == 0
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="toast_identity_invalid",
    ):
        await importer._provider_directory_profile_toast_chunk_count(
            source_sql="SELECT 1",
            relation_oid=0,
            toast_oid=1,
            toastable_columns=("payload",),
            expected_compression="pglz",
        )
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="toast_projection_missing"):
        await importer._provider_directory_profile_toast_chunk_count(
            source_sql="SELECT 1",
            relation_oid=1,
            toast_oid=2,
            toastable_columns=("payload",),
            expected_compression="pglz",
        )


@pytest.mark.asyncio
async def test_toast_projection_compression_edges(monkeypatch):
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value={
                "compression_methods": ["lz4"],
                "toast_chunk_count": 1,
            }
        ),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="toast_compression_changed",
    ):
        await importer._provider_directory_profile_toast_chunk_count(
            source_sql="SELECT 1",
            relation_oid=1,
            toast_oid=2,
            toastable_columns=("payload",),
            expected_compression="pglz",
        )
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value={
                "compression_methods": ["pglz"],
                "toast_chunk_count": -1,
            }
        ),
    )
    with pytest.raises(RuntimeError, match="toast_projection_invalid"):
        await importer._provider_directory_profile_toast_chunk_count(
            source_sql="SELECT 1",
            relation_oid=1,
            toast_oid=2,
            toastable_columns=("payload",),
            expected_compression="pglz",
        )


def _cutover_forecast():
    return importer._ProviderDirectoryProfileCutoverCapacityForecast(
        target_projection=SimpleNamespace(),
        metadata_projection=SimpleNamespace(),
        forecast_hash="a" * 64,
        forecast_json='{"forecast":1}',
        wal_start_lsn="0/1",
        wal_bytes_before=0,
        evidence_target_bytes_before=1,
        profile_target_bytes_before=1,
    )


@pytest.mark.asyncio
async def test_cutover_forecast_missing_and_conflict_edges(monkeypatch):
    delta = _prepared_delta()
    forecast = _cutover_forecast()
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="forecast_checkpoint_missing",
    ):
        await importer._persist_provider_directory_profile_cutover_forecast(
            delta,
            forecast,
        )

    verified_by_field = {
        "cutover_forecast_status": "verified",
        "cutover_forecast_hash": forecast.forecast_hash,
        "cutover_forecast_json": {"forecast": 1},
    }
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value=verified_by_field),
    )
    await importer._persist_provider_directory_profile_cutover_forecast(
        delta,
        forecast,
    )
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value=dict(
                verified_by_field,
                cutover_forecast_hash="b" * 64,
            )
        ),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="forecast_changed",
    ):
        await importer._persist_provider_directory_profile_cutover_forecast(
            delta,
            forecast,
        )


@pytest.mark.asyncio
async def test_cutover_forecast_status_and_cas_edges(monkeypatch):
    delta = _prepared_delta()
    forecast = _cutover_forecast()
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value={"cutover_forecast_status": "writing"}),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="forecast_status_invalid",
    ):
        await importer._persist_provider_directory_profile_cutover_forecast(
            delta,
            forecast,
        )
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value={"cutover_forecast_status": "not_started"}),
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value="UPDATE 0"))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="forecast_cas_failed",
    ):
        await importer._persist_provider_directory_profile_cutover_forecast(
            delta,
            forecast,
        )

@pytest.mark.asyncio
async def test_resource_npi_candidate_edges(monkeypatch):
    scalar = AsyncMock(side_effect=[False, False])
    monkeypatch.setattr(importer.db, "scalar", scalar)
    await importer._assert_no_resource_npi_candidates("mrf", [])
    scalar.assert_not_awaited()
    await importer._assert_no_resource_npi_candidates(
        "mrf",
        ["source-a"],
    )
    assert scalar.await_count == 2

    scalar.reset_mock(side_effect=True)
    with pytest.raises(RuntimeError, match="backfill_required"):
        await importer._assert_no_resource_npi_candidates(
            "mrf",
            ["source-a"],
        )
