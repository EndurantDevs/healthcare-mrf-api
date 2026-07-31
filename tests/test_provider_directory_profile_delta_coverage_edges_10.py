# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Replay and build-admission edge proofs for bounded Profile deltas."""

from __future__ import annotations

from tests.provider_directory_profile_delta_coverage_support import (
    AsyncMock,
    Mock,
    SimpleNamespace,
    _execution,
    _wal_tracker_admission,
    contextlib,
    datetime,
    importer,
    json,
    pytest,
)

_REPLAY_GEOMETRY_FIELDS = (
    "database_system_identifier",
    "database_oid",
    "database_name",
    "tablespace_oid",
    "tablespace_name",
    "postgres_server_version_num",
    "postgres_block_size_bytes",
    "postgres_wal_block_size_bytes",
    "postgres_wal_segment_size_bytes",
    "postgres_full_page_writes",
    "postgres_wal_compression",
    "postgres_wal_level",
    "postgres_wal_log_hints",
    "postgres_data_checksums",
    "postgres_default_toast_compression",
    "postgres_checkpoint_timeout_seconds",
    "postgres_max_wal_size_bytes",
    "evidence_target_storage_fingerprint",
    "profile_target_storage_fingerprint",
    "build_checkpoint_oid",
    "serving_generation_oid",
    "delta_receipt_oid",
    "build_checkpoint_storage_fingerprint",
    "serving_generation_storage_fingerprint",
    "delta_receipt_storage_fingerprint",
)


def _capacity_build():
    return SimpleNamespace(
        capacity_geometry_status="verified",
        capacity_geometry_hash="hash",
        capacity_geometry_json="json",
    )


def _patch_capacity_build_identity(monkeypatch, *, matching):
    admission = _wal_tracker_admission()
    geometry = admission.geometry
    monkeypatch.setattr(
        importer.profile_capacity,
        "revalidate_capacity_geometry",
        lambda _geometry: geometry,
    )
    monkeypatch.setattr(
        importer,
        "_expected_profile_capacity_identity",
        lambda *_args: {"identity": 1},
    )
    monkeypatch.setattr(
        importer,
        "_observed_profile_capacity_identity",
        lambda *_args: {"identity": 1 if matching else 2},
    )
    return admission, geometry


def test_capacity_build_observed_identity_edge(monkeypatch):
    admission, _geometry = _patch_capacity_build_identity(
        monkeypatch,
        matching=False,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="build_identity_changed",
    ):
        importer._assert_provider_directory_profile_capacity_build(
            admission,
            _capacity_build(),
            object(),
            object(),
        )


def test_capacity_build_missing_execution_edge(monkeypatch):
    admission, _geometry = _patch_capacity_build_identity(
        monkeypatch,
        matching=True,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="build_identity_changed",
    ):
        importer._assert_provider_directory_profile_capacity_build(
            admission,
            _capacity_build(),
            object(),
            object(),
        )


def test_capacity_build_execution_digest_edge(monkeypatch):
    admission, _geometry = _patch_capacity_build_identity(
        monkeypatch,
        matching=True,
    )
    token = importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(
        SimpleNamespace(
            attestation=SimpleNamespace(profile_input_digest="wrong")
        )
    )
    try:
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="build_identity_changed",
        ):
            importer._assert_provider_directory_profile_capacity_build(
                admission,
                _capacity_build(),
                object(),
                object(),
            )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(token)


def test_capacity_build_geometry_edge(monkeypatch):
    admission, geometry = _patch_capacity_build_identity(
        monkeypatch,
        matching=True,
    )
    token = importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(
        SimpleNamespace(
            attestation=SimpleNamespace(
                profile_input_digest=geometry.profile_input_digest
            )
        )
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_geometry_identity",
        lambda **_kwargs: ("verified", "wrong", "wrong"),
    )
    try:
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="build_geometry_changed",
        ):
            importer._assert_provider_directory_profile_capacity_build(
                admission,
                _capacity_build(),
                object(),
                object(),
            )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(token)


def test_replay_owner_cutover_and_json_edges(monkeypatch):
    cutover_identity = Mock(return_value=None)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_cutover_receipt_identity",
        cutover_identity,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="cutover_evidence_missing",
    ):
        importer._profile_replay_owner_run_id({})
    cutover_identity.return_value = ("a" * 64, "{}")
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="owner_invalid",
    ):
        importer._profile_replay_owner_run_id(
            {"cutover_forecast_json": "{"}
        )


def test_replay_owner_mapping_and_success_edges(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_cutover_receipt_identity",
        lambda _receipt: ("a" * 64, "{}"),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="owner_invalid",
    ):
        importer._profile_replay_owner_run_id(
            {"cutover_forecast_json": {}}
        )
    owner_run_id = "run_" + "1" * 32
    assert importer._profile_replay_owner_run_id(
        {
            "cutover_forecast_json": json.dumps(
                {"run_id": owner_run_id}
            )
        }
    ) == owner_run_id


@pytest.mark.asyncio
async def test_replay_source_context_scope_edges(monkeypatch):
    execution = _execution()
    context = importer._ProviderDirectoryProfileSourceContext(
        source_id="source-a",
        endpoint_id="endpoint-a",
        canonical_api_base=None,
        org_name="Payer",
        plan_name=None,
    )
    scope = AsyncMock(return_value=(["wrong"], ["wrong"], (context,)))
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_scope_source_ids",
        scope,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="source_scope_changed",
    ):
        await importer._provider_directory_profile_replay_source_context(
            execution
        )
    source_id = execution.attestation.pairs[0]["source_id"]
    scope.return_value = ([source_id], [source_id], (context,))
    source_vector, context_vector = (
        await importer._provider_directory_profile_replay_source_context(
            execution
        )
    )
    assert source_vector[0][0] == source_id
    assert context_vector[0][0] == "source-a"


def test_replay_serving_values_missing_cutover_edge(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_cutover_receipt_identity",
        lambda _receipt: None,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="cutover_evidence_missing",
    ):
        importer._profile_replay_serving_values(
            {},
            _execution(),
            (),
            (),
        )


@pytest.mark.asyncio
async def test_replay_database_matching_and_changed_edges(monkeypatch):
    geometry = _wal_tracker_admission().geometry
    identity_by_field = {
        field_name: getattr(geometry, field_name)
        for field_name in _REPLAY_GEOMETRY_FIELDS
    }
    resolver = AsyncMock(return_value=SimpleNamespace(**identity_by_field))
    monkeypatch.setattr(
        importer, "_provider_directory_profile_capacity_database_identity", resolver
    )
    tablespaces = Mock()
    monkeypatch.setattr(
        importer, "_assert_provider_directory_profile_capacity_tablespaces", tablespaces
    )
    await importer._assert_replay_database(
        "mrf",
        object(),
        geometry,
        object(),
    )
    resolver.return_value = SimpleNamespace(
        **(identity_by_field | {"database_oid": -1})
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="database_changed",
    ):
        await importer._assert_replay_database(
            "mrf",
            object(),
            geometry,
            object(),
        )


def test_replay_timeline_invalid_edge():
    moment = datetime.datetime(
        2026,
        7,
        30,
        tzinfo=datetime.timezone.utc,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="timeline_invalid",
    ):
        importer._assert_replay_timeline(
            {"accepted_at": moment},
            {"committed_at": moment},
            SimpleNamespace(
                max_build_deadline=moment,
                expires_at=moment,
            ),
        )


def _patch_committed_replay_dependencies(monkeypatch, receipt):
    monkeypatch.setattr(importer, "_replay_control_run", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_replay_current_consumption",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        importer,
        "_replay_exact_receipt",
        AsyncMock(return_value=receipt),
    )


@pytest.mark.asyncio
async def test_committed_replay_none_and_current_conflict_edges(monkeypatch):
    execution = _execution()
    _patch_committed_replay_dependencies(monkeypatch, None)
    assert await importer._committed_replay_result(
        "mrf",
        "consumption",
        "run_" + "1" * 32,
        execution,
        object(),
    ) is None
    importer._replay_exact_receipt.return_value = {"build_id": "build"}
    importer._replay_current_consumption.return_value = {}
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_replay_receipt_owner_run_id",
        lambda _receipt: "run_" + "2" * 32,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="current_consumption_conflict",
    ):
        await importer._committed_replay_result(
            "mrf",
            "consumption",
            "run_" + "1" * 32,
            execution,
            object(),
        )


@pytest.mark.asyncio
async def test_committed_replay_pointer_and_success_edges(monkeypatch):
    run_id = "run_" + "1" * 32
    receipt_by_field = {"build_id": "build"}
    _patch_committed_replay_dependencies(monkeypatch, receipt_by_field)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_replay_receipt_owner_run_id",
        lambda _receipt: run_id,
    )
    monkeypatch.setattr(
        importer,
        "_replay_bound_consumption",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(importer, "_assert_replay_owner", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_replay_capacity_artifacts",
        lambda *_args: (object(), object()),
    )
    monkeypatch.setattr(
        importer,
        "_replay_serving_state",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(importer, "_assert_replay_database", AsyncMock())
    monkeypatch.setattr(importer, "_assert_replay_timeline", lambda *_args: None)
    committed = AsyncMock(return_value=False)
    monkeypatch.setattr(
        importer,
        "_is_provider_directory_dataset_cutover_committed",
        committed,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="dataset_pointer_changed",
    ):
        await importer._committed_replay_result(
            "mrf",
            "consumption",
            run_id,
            _execution(),
            object(),
        )
    committed.return_value = True
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_replay_metrics",
        lambda *_args: {"incremental": True},
    )
    result_by_field = await importer._committed_replay_result(
        "mrf",
        "consumption",
        run_id,
        _execution(),
        object(),
    )
    assert result_by_field["incremental"] is True


@pytest.mark.asyncio
async def test_committed_run_replay_valid_path_edge(monkeypatch):
    @contextlib.asynccontextmanager
    async def transaction():
        yield

    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    replay = AsyncMock(return_value={"incremental": True})
    monkeypatch.setattr(importer, "_committed_replay_result", replay)
    run_id = "run_" + "1" * 32
    result_by_field = (
        await importer._provider_directory_profile_committed_run_replay(
            run_id=run_id,
            control_run_id=run_id,
            execution=_execution(),
            fence=object(),
        )
    )
    assert result_by_field["incremental"] is True


def test_profile_build_admission_identity_and_target_edges(monkeypatch):
    identity = SimpleNamespace(
        materialization_mode="source_delta",
        serving_state=SimpleNamespace(
            evidence_target_oid=11,
            profile_target_oid=12,
        ),
    )
    admission = _wal_tracker_admission()
    token = importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
        admission
    )
    try:
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="admitted_identity_changed",
        ):
            importer._validate_profile_build_admission(
                identity,
                SimpleNamespace(target_oid=11),
                SimpleNamespace(target_oid=12),
            )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="serving_target_changed",
    ):
        importer._validate_profile_build_admission(
            identity,
            SimpleNamespace(target_oid=99),
            SimpleNamespace(target_oid=12),
        )
