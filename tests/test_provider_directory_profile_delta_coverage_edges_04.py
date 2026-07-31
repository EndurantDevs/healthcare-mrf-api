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


def _profile_capacity_identity_by_field(serving_state):
    """Return a canonical database and tablespace identity mapping."""

    return {
        "evidence_target_oid": serving_state.evidence_target_oid,
        "evidence_schema": "mrf",
        "evidence_relation": profile.PROFILE_EVIDENCE_TABLE,
        "evidence_relkind": "r",
        "evidence_relpersistence": "p",
        "profile_target_oid": serving_state.profile_target_oid,
        "profile_schema": "mrf",
        "profile_relation": profile.PROFILE_TABLE,
        "profile_relkind": "r",
        "profile_relpersistence": "p",
        "database_tablespace_oid": 1,
        "evidence_tablespace_oid": 1,
        "profile_tablespace_oid": 1,
        "database_tablespace_name": "pg_default",
        "temp_tablespaces": "",
    }


def test_artifact_config_and_scope_naming_edges(monkeypatch):
    env_name = "HLTHPRT_PROVIDER_DIRECTORY_TEST_WORKERS"
    monkeypatch.delenv(env_name, raising=False)
    assert importer._provider_directory_positive_config(env_name, 2) == (
        2,
        False,
    )
    monkeypatch.setenv(env_name, "0")
    with pytest.raises(RuntimeError, match="positive_config_invalid"):
        importer._provider_directory_positive_config(env_name, 2)

    admission = _wal_tracker_admission()
    token = importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
        admission
    )
    try:
        assert importer._provider_directory_artifact_scope_workers() == (
            admission.geometry.artifact_scope_worker_count
        )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)

    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_SCOPE_WORKERS",
        "2",
    )
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "2")
    with pytest.raises(RuntimeError, match="worker_capacity_exceeded"):
        importer._provider_directory_artifact_scope_workers()

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: admission,
    )
    with pytest.raises(RuntimeError, match="owner_run_changed"):
        importer._provider_directory_artifact_scope_table_name(
            "provider_directory_source",
            "run_" + "f" * 32,
        )
    assert importer._provider_directory_artifact_scope_table_name(
        "provider_directory_source",
        admission.run_id,
    ).endswith(admission.run_id.removeprefix("run_"))

    assert importer._provider_directory_artifact_scope_table_prefix(
        "short"
    ) == "short_artifact_scope"
    long_prefix = importer._provider_directory_artifact_scope_table_prefix(
        "x" * 63
    )
    assert len(long_prefix) <= 30
    with pytest.raises(RuntimeError, match="owner_identity_invalid"):
        importer._owned_artifact_scope_name("source", run_id="invalid")

def test_artifact_scope_projection_capacity_edges(monkeypatch):
    projection = SimpleNamespace(
        projected_rows=2,
        projected_logical_bytes=20,
        projection_hash="a" * 64,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: None,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_scope_row_limit",
        lambda: 2,
    )
    importer._assert_artifact_scope_projection(projection)
    with pytest.raises(RuntimeError, match="projected_rows_exceeded"):
        importer._assert_artifact_scope_projection(
            dataclasses.replace(projection, projected_rows=3)
            if dataclasses.is_dataclass(projection)
            else SimpleNamespace(
                projected_rows=3,
                projected_logical_bytes=20,
                projection_hash="a" * 64,
            )
        )

    admission = SimpleNamespace(
        geometry=SimpleNamespace(
            max_artifact_scope_rows=2,
            artifact_scope_projected_logical_bytes=20,
            artifact_scope_projection_hash="a" * 64,
        )
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: admission,
    )
    importer._assert_artifact_scope_projection(projection)
    for field_name, replacement in (
        ("projected_rows", 3),
        ("projected_logical_bytes", 21),
        ("projection_hash", "b" * 64),
    ):
        changed = SimpleNamespace(**vars(projection))
        setattr(changed, field_name, replacement)
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="artifact_projection_changed",
        ):
            importer._assert_artifact_scope_projection(changed)

def test_artifact_scope_owner_and_batch_coordinate_edges():
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="owner_invalid",
    ):
        importer._artifact_scope_owner_from_row({})
    owner = importer._artifact_scope_owner_from_row(
        {
            "run_id": "run_" + "1" * 32,
            "build_id": "pdpb_" + "2" * 32,
            "capacity_geometry_hash": "3" * 64,
            "status": "succeeded",
            "importer": "provider-directory-fhir",
            "finished_at": "2026-07-30T00:00:00Z",
        }
    )
    assert owner.status == "succeeded"

    with pytest.raises(RuntimeError, match="batch_row_count_invalid"):
        importer._validate_artifact_resource_inserted_rows(
            inserted_rows=-1,
            batch_size=10,
            expected_batch=None,
        )
    expected_batch = SimpleNamespace(projected_rows=1)
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="resource_projection_changed",
    ):
        importer._validate_artifact_resource_inserted_rows(
            inserted_rows=0,
            batch_size=10,
            expected_batch=expected_batch,
        )

def test_artifact_projection_values_and_source_batch_edges():
    assert importer._provider_directory_artifact_projection_values(
        {"projected_rows": 1, "projected_logical_bytes": 8}
    ) == (1, 8, None)
    with pytest.raises(RuntimeError, match="projection_invalid"):
        importer._provider_directory_artifact_projection_values(
            {"projected_rows": -1, "projected_logical_bytes": 0}
        )

@pytest.mark.asyncio
async def test_artifact_source_cursor_and_projection_edges(monkeypatch):
    dataset = SimpleNamespace(source_id="source-a")
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="batch_cursor_invalid"):
        await importer._next_artifact_resource_id(
            "mrf",
            "resource",
            dataset,
            None,
        )
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value="r2"))
    assert await importer._next_artifact_resource_id(
        "mrf",
        "resource",
        dataset,
        "r1",
    ) == "r2"

    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value={
                "projected_rows": 2,
                "projected_logical_bytes": 8,
            }
        ),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="source_projection_changed",
    ):
        await importer._provider_directory_artifact_source_exact_projection(
            "mrf",
            ["source-a"],
        )

@pytest.mark.parametrize(
    ("database_pool_size", "reserve"),
    ((3, 3), (4, 4)),
)
def test_capacity_artifact_worker_pool_edges(
    monkeypatch,
    database_pool_size,
    reserve,
):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_SCOPE_WORKERS",
        "1",
    )
    with pytest.raises(RuntimeError, match="artifact_workers_exceeded"):
        importer._provider_directory_profile_capacity_artifact_workers(
            database_pool_size=database_pool_size,
            pool_reserve_connections=reserve,
        )

def test_profile_geometry_identity_edges():
    assert importer._provider_directory_profile_capacity_geometry_identity(
        status="legacy_unavailable",
        geometry_hash=None,
        geometry_json=None,
    ) == ("legacy_unavailable", None, None)
    for geometry_by_field, error in (
        (
            {
                "status": "legacy_unavailable",
                "geometry_hash": "a" * 64,
                "geometry_json": None,
            },
            "legacy_geometry_invalid",
        ),
        (
            {"status": "unknown", "geometry_hash": None, "geometry_json": None},
            "geometry_status_invalid",
        ),
        (
            {
                "status": "verified",
                "geometry_hash": "a" * 64,
                "geometry_json": [],
            },
            "geometry_json_invalid",
        ),
    ):
        with pytest.raises(RuntimeError, match=error):
            importer._provider_directory_profile_capacity_geometry_identity(
                **geometry_by_field
            )

@pytest.mark.asyncio
async def test_profile_capacity_database_identity_edges(monkeypatch):
    """Database identity remains bound to the serving target relations."""

    serving_state = _matching_delta_serving_state(_prepared_delta())
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="database_identity_missing"):
        await importer._profile_capacity_database_row(serving_state)

    identity_by_field = _profile_capacity_identity_by_field(serving_state)
    assert importer._profile_capacity_database_tablespace_oid(
        identity_by_field,
        "mrf",
        serving_state,
    ) == 1

    changed_identity_by_field = dict(
        identity_by_field,
        evidence_target_oid=999,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="target_identity_changed",
    ):
        importer._profile_capacity_database_tablespace_oid(
            changed_identity_by_field,
            "mrf",
            serving_state,
        )
    changed_identity_by_field = dict(
        identity_by_field,
        evidence_tablespace_oid=2,
    )
    with pytest.raises(RuntimeError, match="tablespace_unsupported"):
        importer._profile_capacity_database_tablespace_oid(
            changed_identity_by_field,
            "mrf",
            serving_state,
        )


@pytest.mark.asyncio
async def test_profile_capacity_temp_tablespace_edges(monkeypatch):
    """Temp tablespaces must resolve to one admitted database location."""

    serving_state = _matching_delta_serving_state(_prepared_delta())
    identity_by_field = _profile_capacity_identity_by_field(serving_state)
    assert await importer._profile_capacity_temp_tablespace(
        identity_by_field,
        1,
    ) == (1, "pg_default")

    for temp_setting in ("a,b", "bad-name"):
        with pytest.raises(RuntimeError, match="temp_tablespace_ambiguous"):
            await importer._profile_capacity_temp_tablespace(
                dict(identity_by_field, temp_tablespaces=temp_setting),
                1,
            )
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="temp_tablespace_missing"):
        await importer._profile_capacity_temp_tablespace(
            dict(identity_by_field, temp_tablespaces="fast_temp"),
            1,
        )

def test_capacity_serving_and_writable_layout_edges():
    state = _matching_delta_serving_state(_prepared_delta())
    importer._assert_profile_capacity_serving(state, state)
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="serving_generation_changed",
    ):
        importer._assert_profile_capacity_serving(state, None)

    layout = SimpleNamespace(effective_tablespace_oids=(1,))
    target_layouts = SimpleNamespace(
        evidence=layout,
        profile=layout,
        database_tablespace_oid=1,
    )
    metadata_layouts = SimpleNamespace(
        layouts_by_name={"checkpoint": layout}
    )
    importer._validate_profile_capacity_writable_layouts(
        target_layouts,
        metadata_layouts,
    )
    with pytest.raises(RuntimeError, match="writable_tablespace"):
        importer._validate_profile_capacity_writable_layouts(
            target_layouts,
            SimpleNamespace(
                layouts_by_name={
                    "checkpoint": SimpleNamespace(
                        effective_tablespace_oids=(2,)
                    )
                }
            ),
        )

def test_capacity_timeout_admission_and_wal_candidate_edges():
    admission = _wal_tracker_admission()
    with pytest.raises(
        importer.ProviderDirectoryCapacityLeaseError,
        match="deadline_reached",
    ):
        importer._profile_capacity_timeouts(admission.geometry, 1001)

    with pytest.raises(RuntimeError, match="control_operation_unknown"):
        importer._profile_control_wal_candidate(
            admission.wal_tracker,
            admission.control_wal_projection,
            {"unknown": 1},
        )
    with pytest.raises(RuntimeError, match="relation_cap_missing"):
        importer._profile_relation_wal_candidate(
            admission,
            {"unknown": 1},
        )
    profile_cap = importer._provider_directory_profile_capacity_relation_cap(
        admission,
        "profile_stage",
    )
    with pytest.raises(RuntimeError, match="relation_wal_projected"):
        importer._profile_relation_wal_candidate(
            admission,
            {"profile_stage": profile_cap.max_wal_bytes + 1},
        )
    admission.wal_tracker.accounted_metadata_wal_bytes = (
        admission.geometry.metadata_wal_upper_bound_bytes
    )
    with pytest.raises(RuntimeError, match="metadata_wal_projected"):
        importer._profile_metadata_wal_candidate(admission, 1)
