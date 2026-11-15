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


def _numeric_database_identity(wal_lsn="0/1"):
    """Use actual numeric PostgreSQL assumptions and the closed logical role map."""
    geometry = _wal_tracker_admission().geometry
    return importer._ProviderDirectoryProfileDatabaseCapacityIdentity(
        wal_lsn=wal_lsn,
        relations={role: dict(entry) for role, entry in geometry.relations.items()},
        **{
            name: getattr(geometry, name)
            for name in importer._ProviderDirectoryProfileDatabaseCapacityIdentity.__dataclass_fields__
            if name.startswith("postgres_")
        },
    )


def _logical_capacity_layout(geometry, role):
    """Create one measured layout within its authentic configured numeric ceilings."""
    expected = geometry.relations[role]
    return importer._ProviderDirectoryProfileCapacityLayout(
        schema_name=expected["schema"],
        relation_name=expected["name"],
        relation_oid=12,
        toast_oid=None,
        main_index_oids=tuple(range(len(expected["main_index_pages"]))),
        main_index_pages=tuple(expected["main_index_pages"]),
        toast_index_oids=tuple(range(len(expected["toast_index_pages"]))),
        toast_index_pages=tuple(expected["toast_index_pages"]),
        toastable_columns=tuple("column_" + str(index) for index in range(expected["toastable_column_count"])),
    )


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
    token = importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
    try:
        assert importer._provider_directory_artifact_scope_workers() == (admission.geometry.artifact_scope_worker_count)
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

    assert importer._provider_directory_artifact_scope_table_prefix("short") == "short_artifact_scope"
    long_prefix = importer._provider_directory_artifact_scope_table_prefix("x" * 63)
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
        importer._provider_directory_artifact_projection_values({"projected_rows": -1, "projected_logical_bytes": 0})


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
    assert (
        await importer._next_artifact_resource_id(
            "mrf",
            "resource",
            dataset,
            "r1",
        )
        == "r2"
    )

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
            importer._provider_directory_profile_capacity_geometry_identity(**geometry_by_field)


@pytest.mark.asyncio
async def test_profile_capacity_named_database_settings_edges(monkeypatch):
    """Measure serving names and numeric settings without retaining database allocation tokens."""
    serving = _matching_delta_serving_state(_prepared_delta())
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="database_settings_missing"):
        await importer._profile_capacity_database_row(serving)
    expected = _numeric_database_identity()
    numeric_by_field = {name: getattr(expected, name) for name in expected.__dataclass_fields__ if name != "relations"}
    importer.db.first.return_value = numeric_by_field
    resolver = AsyncMock(return_value=12)
    layout = _logical_capacity_layout(_wal_tracker_admission().geometry, "profile_target")
    monkeypatch.setattr(importer, "_provider_directory_relation_oid", resolver)
    monkeypatch.setattr(
        importer, "_provider_directory_profile_capacity_relation_layout", AsyncMock(return_value=layout)
    )
    measured = await importer._provider_directory_profile_capacity_database_identity("controls", serving)
    assert measured.wal_lsn == expected.wal_lsn
    assert measured.relations["evidence_target"]["schema"] == serving.targets["evidence"]["schema"]
    assert measured.relations["profile_target"]["name"] == serving.targets["profile"]["name"]
    assert measured.relations["build_checkpoint"]["schema"] == "controls"
    assert len(measured.relations) == resolver.await_count == 7
    resolver.return_value = None
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="relation_missing"):
        await importer._provider_directory_profile_capacity_database_identity("controls", serving)


@pytest.mark.asyncio
async def test_profile_capacity_named_layout_scope_and_numeric_bounds(monkeypatch):
    """Missing names, changed schema and unfunded index growth refuse admission."""
    geometry = _wal_tracker_admission().geometry
    role = "capacity_consumption"
    layout = _logical_capacity_layout(geometry, role)
    resolver = AsyncMock(return_value=12)
    capture = AsyncMock(return_value=layout)
    monkeypatch.setattr(importer, "_provider_directory_relation_oid", resolver)
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_relation_layout", capture)
    assert await importer._profile_capacity_named_layout(geometry, role) == layout
    importer._assert_profile_capacity_layout_bounds(dataclasses.replace(layout, relation_oid=999), geometry, role)
    capture.return_value = dataclasses.replace(layout, schema_name="other_schema")
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="layout_scope_changed"):
        await importer._profile_capacity_named_layout(geometry, role)
    capture.return_value = dataclasses.replace(layout, main_index_pages=(max(layout.main_index_pages) + 1,))
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="index_bound_changed"):
        await importer._profile_capacity_named_layout(geometry, role)
    resolver.return_value = None
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="relation_missing"):
        await importer._profile_capacity_named_layout(geometry, role)


def test_capacity_serving_and_named_control_layout_edges():
    """Serving CAS and complete control coordinates remain required independently."""
    state = _matching_delta_serving_state(_prepared_delta())
    importer._assert_profile_capacity_serving(state, state)
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="serving_generation_changed"):
        importer._assert_profile_capacity_serving(state, None)
    geometry = _wal_tracker_admission().geometry
    role = "build_checkpoint"
    layout = _logical_capacity_layout(geometry, role)
    importer._assert_profile_capacity_layout_bounds(layout, geometry, role)
    for changed in (
        dataclasses.replace(layout, relation_name="other_checkpoint"),
        dataclasses.replace(layout, toastable_columns=()),
    ):
        with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="layout_scope_changed"):
            importer._assert_profile_capacity_layout_bounds(changed, geometry, role)


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
    admission.wal_tracker.accounted_metadata_wal_bytes = admission.geometry.metadata_wal_upper_bound_bytes
    with pytest.raises(RuntimeError, match="metadata_wal_projected"):
        importer._profile_metadata_wal_candidate(admission, 1)
