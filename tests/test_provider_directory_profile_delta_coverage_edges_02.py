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
async def test_delta_replay_receipt_edges(monkeypatch):
    """Replay rejects missing, conflicting, and incomplete receipts."""

    delta = _prepared_delta()
    receipt_by_field = _matching_delta_receipt(delta)

    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    assert not await importer._has_provider_directory_profile_delta_replay(
        delta,
        None,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="receipt_missing",
    ):
        await importer._refresh_provider_directory_profile_delta_metrics(
            {},
            delta,
        )

    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value=dict(receipt_by_field, build_id="different")),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="receipt_conflict",
    ):
        await importer._has_provider_directory_profile_delta_replay(
            delta,
            None,
        )

    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value=receipt_by_field),
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
        await importer._has_provider_directory_profile_delta_replay(
            delta,
            None,
        )


@pytest.mark.asyncio
async def test_delta_replay_dataset_and_metrics_edges(monkeypatch):
    """Committed replay requires its dataset pointer before metric refresh."""

    delta = _prepared_delta()
    receipt_by_field = _matching_delta_receipt(delta)
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value=receipt_by_field),
    )
    monkeypatch.setattr(
        importer,
        "_is_provider_directory_profile_delta_committed",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        importer,
        "_is_provider_directory_dataset_cutover_committed",
        AsyncMock(return_value=False),
    )
    fence = importer.ProviderDirectoryArtifactDatasetFence(())
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="dataset_pointer_incomplete",
    ):
        await importer._has_provider_directory_profile_delta_replay(
            delta,
            fence,
        )

    monkeypatch.setattr(
        importer,
        "_is_provider_directory_dataset_cutover_committed",
        AsyncMock(return_value=True),
    )
    assert await importer._has_provider_directory_profile_delta_replay(
        delta,
        fence,
    )
    metrics_by_name: dict[str, object] = {}
    await importer._refresh_provider_directory_profile_delta_metrics(
        metrics_by_name,
        delta,
    )
    assert (
        metrics_by_name["selected_evidence_rows"]
        == delta.expected_evidence_rows
    )

@pytest.mark.asyncio
async def test_delta_stage_content_checks_each_failure(monkeypatch):
    delta = _prepared_delta()
    probes = (
        "_is_delta_evidence_stage_invalid",
        "_is_delta_profile_stage_invalid",
        "_is_delta_affected_stage_invalid",
        "_is_delta_affected_stage_incomplete",
    )
    for failing_probe in probes:
        for probe_name in probes:
            monkeypatch.setattr(
                importer,
                probe_name,
                AsyncMock(return_value=probe_name == failing_probe),
            )
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="delta_.*_stage_content_changed",
        ):
            await importer._assert_profile_delta_stage(delta)

    for probe_name in probes:
        monkeypatch.setattr(
            importer,
            probe_name,
            AsyncMock(return_value=False),
        )
    await importer._assert_profile_delta_stage(delta)

@pytest.mark.parametrize(
    ("raw_value", "error"),
    (
        ({}, "source_context_vector_invalid"),
        ([{}], "source_context_vector_invalid"),
        (
            [{"source_id": "", "context_digest": "a" * 64}],
            "source_context_vector_invalid",
        ),
        (
            [{"source_id": "source-a", "context_digest": "bad"}],
            "source_context_vector_invalid",
        ),
        (
            [
                {"source_id": "source-a", "context_digest": "a" * 64},
                {"source_id": "source-a", "context_digest": "b" * 64},
            ],
            "source_context_vector_invalid",
        ),
    ),
)
def test_source_context_vector_rejects_noncanonical_input(raw_value, error):
    with pytest.raises(RuntimeError, match=error):
        importer._profile_context_vector_from_json(raw_value)

def test_source_context_vector_accepts_json_and_sorts():
    raw_value = json.dumps(
        [
            {"source_id": "source-b", "context_digest": "b" * 64},
            {"source_id": "source-a", "context_digest": "a" * 64},
        ]
    )
    assert importer._profile_context_vector_from_json(raw_value) == (
        ("source-a", "a" * 64),
        ("source-b", "b" * 64),
    )

@pytest.mark.parametrize(
    "raw_value",
    (
        {},
        [{}],
        [{"source_id": "", "dataset_id": "dataset-a"}],
        [{"source_id": "source-a", "dataset_id": ""}],
        [
            {"source_id": "source-a", "dataset_id": "dataset-a"},
            {"source_id": "source-a", "dataset_id": "dataset-b"},
        ],
    ),
)
def test_source_vector_rejects_noncanonical_input(raw_value):
    with pytest.raises(RuntimeError, match="source_vector_invalid"):
        importer._profile_source_vector_from_json(raw_value)

def test_source_vector_accepts_json_and_sorts():
    assert importer._profile_source_vector_from_json(
        json.dumps(
            [
                {"source_id": "source-b", "dataset_id": "dataset-b"},
                {"source_id": "source-a", "dataset_id": "dataset-a"},
            ]
        )
    ) == (
        ("source-a", "dataset-a"),
        ("source-b", "dataset-b"),
    )

@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("status", "building"),
        ("operation", "invalid"),
        ("generation_id", "invalid"),
        ("selection_proof_id", "invalid"),
        ("source_vector_hash", "0" * 64),
        ("source_context_vector_hash", "0" * 64),
        ("executable_plan_hash", "invalid"),
        ("profile_strategy_version", None),
        ("profile_as_of", "2026-7-3"),
        ("published_at", None),
        ("control_generation", 0),
        ("authority_revision", 0),
        ("profile_schema_version", 0),
        ("evidence_target_oid", 0),
        ("profile_target_oid", 0),
        ("evidence_rows", -1),
        ("profile_rows", -1),
        ("cutover_forecast_hash", "invalid"),
    ),
)
def test_serving_state_rejects_each_lineage_drift(
    field_name,
    replacement,
):
    serving_row = _valid_serving_row()
    serving_row[field_name] = replacement
    with pytest.raises(
        RuntimeError,
        match="serving_generation_invalid",
    ):
        importer._profile_serving_state_from_row(serving_row)

def test_serving_state_decodes_canonical_row():
    serving_state = importer._profile_serving_state_from_row(
        _valid_serving_row()
    )
    assert serving_state.status == "published"
    assert serving_state.capacity_geometry_status == "legacy_unavailable"

@pytest.mark.parametrize(
    ("selected", "retained", "datasets", "error"),
    (
        (("a",), ("a",), (), "cardinality"),
        (("a", "a"), ("a",), ("d1", "d2"), "not_unique"),
        (("a",), ("a", "a"), ("d1",), "not_unique"),
        (("a", "b"), ("a", "b"), ("d1", "d1"), "not_unique"),
        (("b",), ("a",), ("d1",), "not_retained"),
    ),
)
def test_profile_batch_scope_rejects_ambiguous_identity(
    selected,
    retained,
    datasets,
    error,
):
    with pytest.raises(RuntimeError, match=error):
        importer._validate_provider_directory_profile_batch_scope(
            selected,
            retained,
            datasets,
        )

def test_profile_batch_option_edges():
    """Batch options and NPI ranges reject unbounded geometry."""

    with pytest.raises(ValueError, match="materialization mode"):
        importer._validate_profile_batch_plan_options(
            importer._ProviderDirectoryProfileBatchPlanOptions(
                materialization_mode="unsupported"
            )
        )
    with pytest.raises(ValueError, match="requires source"):
        importer._validate_profile_batch_plan_options(
            importer._ProviderDirectoryProfileBatchPlanOptions(
                materialization_mode="source_delta"
            )
        )
    for field_name in ("evidence_window_size", "compact_window_size"):
        options = importer._ProviderDirectoryProfileBatchPlanOptions(
            **{field_name: 0}
        )
        with pytest.raises(ValueError, match="outside"):
            importer._profile_batch_window_sizes(options)

    assert importer._provider_directory_profile_max_wave_width(()) == 0
    assert importer._provider_directory_profile_max_wave_width(
        ((0, 1), (1, 3))
    ) == 2
    assert importer._profile_batch_detail_by_name(None) == {}
    assert importer._provider_directory_profile_npi_ranges(
        profile.PROFILE_NPI_BATCH_SIZE
    )[0] == (
        profile.NPI_MIN,
        profile.NPI_MIN + profile.PROFILE_NPI_BATCH_SIZE,
    )
    with pytest.raises(ValueError, match="bounded minimum"):
        importer._provider_directory_profile_npi_ranges(
            profile.PROFILE_NPI_BATCH_SIZE - 1
        )
    with pytest.raises(ValueError, match="must be positive"):
        importer._provider_directory_profile_npi_ranges(0)


def test_profile_batch_delta_plan_edges():
    """Source-delta waves remain bounded and omit compact copy work."""

    copy = importer._ProviderDirectoryProfileEvidenceBatch(kind="copy")
    fact_a = importer._ProviderDirectoryProfileEvidenceBatch(
        kind="fact",
        source_id="a",
        dataset_id="d1",
        fact_type="specialty",
    )
    fact_b = dataclasses.replace(
        fact_a,
        source_id="b",
        dataset_id="d2",
    )
    assert importer._provider_directory_profile_plan_waves(
        (copy, fact_a, fact_b),
        window_size=2,
        source_bounded=True,
    ) == ((0, 1), (1, 2), (2, 3))

    plan = importer._provider_directory_profile_batch_plan(
        ("a",),
        ("a",),
        ("d1",),
        has_existing_artifacts=True,
        materialization_mode="source_delta",
        current_source_vector_hash="1" * 64,
        desired_source_vector_hash="2" * 64,
        current_source_context_vector_hash="3" * 64,
        desired_source_context_vector_hash="4" * 64,
        evidence_window_size=2,
        compact_window_size=2,
    )
    assert plan.materialization_mode == "source_delta"
    assert not plan.include_copy_batch
    assert max(end - start for start, end in plan.evidence_waves) <= 2
    assert max(end - start for start, end in plan.compact_waves) <= 2

@pytest.mark.asyncio
async def test_projection_helpers_reject_missing_and_invalid_rows(monkeypatch):
    fact_batch = importer._ProviderDirectoryProfileEvidenceBatch(
        kind="fact",
        source_id="source-a",
        dataset_id="dataset-a",
        fact_type="specialty",
    )
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="evidence_projection_missing"):
        await importer._project_profile_evidence_batch(
            fact_batch,
            {
                "target_ref": "target",
                "source_ref": "source",
                "practitioner_ref": "practitioner",
                "role_ref": "role",
                "organization_ref": "organization",
                "affiliation_ref": "affiliation",
                "affiliation_organization_ref": "affiliation_edge",
                "service_ref": "service",
                "endpoint_ref": "endpoint",
            },
            {},
        )

    for projection_row in (
        {},
        {"projected_rows": "bad", "projected_logical_bytes": 0},
        {"projected_rows": -1, "projected_logical_bytes": 0},
    ):
        monkeypatch.setattr(
            importer.db,
            "first",
            AsyncMock(return_value=projection_row),
        )
        with pytest.raises(RuntimeError, match="projection_invalid"):
            await importer._project_profile_evidence_batch(
                fact_batch,
                {
                    "target_ref": "target",
                    "source_ref": "source",
                    "practitioner_ref": "practitioner",
                    "role_ref": "role",
                    "organization_ref": "organization",
                    "affiliation_ref": "affiliation",
                    "affiliation_organization_ref": "affiliation_edge",
                    "service_ref": "service",
                    "endpoint_ref": "endpoint",
                },
                {},
            )
