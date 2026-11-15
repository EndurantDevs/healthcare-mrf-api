# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capacity and admission edge proofs for bounded Profile deltas."""

from __future__ import annotations

from tests.provider_directory_profile_delta_coverage_support import (
    AsyncMock,
    SimpleNamespace,
    _execution,
    _wal_tracker_admission,
    dataclasses,
    datetime,
    importer,
    pytest,
)
from tests.test_provider_directory_profile_capacity_runtime import (
    _limits_payload,
)
from tests.test_provider_directory_profile_delta_coverage_edges_04 import (
    _logical_capacity_layout,
    _numeric_database_identity,
)


def _safe_relation_by_field():
    return {
        "relkind": "r",
        "relpersistence": "p",
        "table_am": "heap",
        "relreplident": "d",
        "relrowsecurity": False,
        "relforcerowsecurity": False,
        "relispartition": False,
        "user_trigger_count": 0,
        "rule_count": 0,
        "inheritance_count": 0,
        "toast_oid": 0,
    }


def test_capacity_artifact_worker_success_and_failure_edges(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_provider_directory_positive_config",
        lambda *_args: (1, False),
    )
    assert (
        importer._provider_directory_profile_capacity_artifact_workers(
            database_pool_size=3,
            pool_reserve_connections=1,
        )
        == 1
    )
    with pytest.raises(RuntimeError, match="workers_exceeded"):
        importer._provider_directory_profile_capacity_artifact_workers(
            database_pool_size=1,
            pool_reserve_connections=1,
        )


@pytest.mark.asyncio
async def test_capacity_relation_missing_and_safe_edges(monkeypatch):
    monkeypatch.setattr(importer.db, "first", AsyncMock(return_value=None))
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="shape_missing",
    ):
        await importer._profile_capacity_relation_row(12, "p", 0)
    importer.db.first.return_value = _safe_relation_by_field()
    relation_by_field, toast_oid = await importer._profile_capacity_relation_row(12, "p", 0)
    assert relation_by_field["relkind"] == "r"
    assert toast_oid is None


def test_capacity_trigger_and_index_success_edges():
    importer._assert_profile_capacity_trigger_shape([], 0, None)
    index_rows = [
        {
            "relation_oid": 12,
            "index_oid": 13,
            "index_name": "profile_idx",
            "relfilenode": 14,
            "tablespace_oid": 0,
            "main_bytes": 8,
            "index_am": "btree",
            "indisvalid": True,
            "indisready": True,
            "indislive": True,
        }
    ]
    structural_rows = importer._profile_capacity_structural_indexes(
        index_rows,
        12,
    )
    assert structural_rows[0]["relation_kind"] == "main"


@pytest.mark.asyncio
async def test_explicit_named_control_layouts_success_edge(monkeypatch):
    identity = _numeric_database_identity()
    geometry = _wal_tracker_admission().geometry
    roles = ("build_checkpoint", "import_run", "capacity_consumption", "serving_generation")
    layouts = tuple(_logical_capacity_layout(geometry, role) for role in roles)
    resolver = AsyncMock(return_value=12)
    capture = AsyncMock(side_effect=layouts)
    monkeypatch.setattr(importer, "_provider_directory_relation_oid", resolver)
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_relation_layout", capture)
    assert await importer._profile_primary_control_layouts(identity) == layouts
    assert [call.args for call in resolver.await_args_list] == [
        (identity.relations[role]["schema"], identity.relations[role]["name"]) for role in roles
    ]
    assert all(call.kwargs == {"expected_persistence": "p"} for call in capture.await_args_list)


@pytest.mark.asyncio
async def test_capacity_named_control_relation_missing_edge(monkeypatch):
    capture = AsyncMock()
    monkeypatch.setattr(importer, "_provider_directory_relation_oid", AsyncMock(return_value=None))
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_relation_layout", capture)
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="control_relation_missing"):
        await importer._profile_primary_control_layouts(_numeric_database_identity())
    capture.assert_not_awaited()


def _control_layouts():
    geometry = _wal_tracker_admission().geometry
    return tuple(
        _logical_capacity_layout(geometry, role)
        for role in ("build_checkpoint", "import_run", "capacity_consumption", "serving_generation")
    )


def test_capacity_complete_named_control_scope_edges():
    identity = _numeric_database_identity()
    layouts = _control_layouts()
    importer._assert_profile_control_layout_identities(identity, layouts)
    for changed in (
        layouts[:-1],
        (dataclasses.replace(layouts[0], schema_name="other_schema"), *layouts[1:]),
        tuple(reversed(layouts)),
    ):
        with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="control_relation_changed"):
            importer._assert_profile_control_layout_identities(identity, changed)


@pytest.mark.asyncio
async def test_profile_admission_identity_invalid_and_valid_edges(monkeypatch):
    invalid_identity = SimpleNamespace(materialization_mode="full_swap")
    resolver = AsyncMock(return_value=invalid_identity)
    monkeypatch.setattr(importer, "_profile_build_identity_inputs", resolver)
    with pytest.raises(RuntimeError, match="delta_required"):
        await importer._profile_admission_identity(SimpleNamespace())
    valid_identity = SimpleNamespace(
        materialization_mode="source_delta",
        serving_state=object(),
        current_source_vector_hash="a",
        desired_source_vector_hash="b",
        current_source_context_vector_hash="c",
        desired_source_context_vector_hash="d",
    )
    resolver.return_value = valid_identity
    assert await importer._profile_admission_identity(SimpleNamespace()) is valid_identity


@pytest.mark.asyncio
async def test_unconsumed_capacity_run_success_edge(monkeypatch):
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=False))
    await importer._assert_profile_capacity_run_unconsumed("run_" + "1" * 32)


@pytest.mark.asyncio
async def test_admission_workload_scope_edge():
    identity = SimpleNamespace(
        source_ids=("source-a",),
        serving_state=object(),
        batch_plan=object(),
    )
    source_fence = SimpleNamespace(datasets=[])
    wrong_resource_fence = SimpleNamespace(datasets=[SimpleNamespace(source_id="source-b")])
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="resource_scope_changed",
    ):
        await importer._profile_admission_workload(
            identity,
            source_fence,
            wrong_resource_fence,
            frozenset(),
        )


@pytest.mark.asyncio
async def test_admission_workload_success_edge(monkeypatch):
    identity = SimpleNamespace(
        source_ids=("source-a",),
        serving_state=object(),
        initial_targets=None,
        batch_plan=object(),
    )
    source_fence = SimpleNamespace(datasets=[])
    limits = importer.profile_capacity_runtime.validated_capacity_limits(
        _limits_payload(
            artifact_scope_batch_size=100,
            pool_reserve_connections=1,
        )
    )
    monkeypatch.setattr(
        importer.profile_capacity_runtime,
        "configured_capacity_limits",
        lambda: limits,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_scope_exact_projection",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_database_pool_capacity",
        lambda: 3,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_artifact_workers",
        lambda **_kwargs: 1,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_database_identity",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_control_wal_plan_input",
        AsyncMock(return_value=object()),
    )
    resource_fence = SimpleNamespace(datasets=[SimpleNamespace(source_id="source-a")])
    workload = await importer._profile_admission_workload(
        identity,
        source_fence,
        resource_fence,
        frozenset(),
    )
    assert workload.artifact_worker_count == 1


def test_admission_run_id_and_lock_projection_edges():
    execution = _execution()
    with pytest.raises(RuntimeError, match="run_id_invalid"):
        importer._validated_admission_run_id(None, None, execution)
    valid_run_id = "run_" + "1" * 32
    assert (
        importer._validated_admission_run_id(
            valid_run_id,
            valid_run_id,
            execution,
        )
        == valid_run_id
    )
    valid_projection = SimpleNamespace(
        operations=[
            SimpleNamespace(
                operation_name="admission_row_lock",
                operation_count=(importer.PROVIDER_DIRECTORY_PROFILE_ADMISSION_ROW_LOCK_COUNT),
            )
        ]
    )
    importer._assert_admission_lock_projection(valid_projection)
    with pytest.raises(RuntimeError, match="lock_projection_invalid"):
        importer._assert_admission_lock_projection(SimpleNamespace(operations=[]))


@pytest.mark.asyncio
async def test_admission_named_scope_and_numeric_settings_guard_edges(monkeypatch):
    expected = _numeric_database_identity("0/1")
    observed = dataclasses.replace(expected, wal_lsn="0/2")
    resolver = AsyncMock(return_value=observed)
    monkeypatch.setattr(importer, "_provider_directory_profile_capacity_database_identity", resolver)
    build_identity = SimpleNamespace(serving_state=object(), initial_targets=None)
    assert await importer._admission_database_guard(build_identity, expected) is observed
    changed_by_role = {
        **expected.relations,
        "evidence_target": {**expected.relations["evidence_target"], "name": "other_evidence"},
    }
    for changed in (
        dataclasses.replace(observed, relations=changed_by_role),
        dataclasses.replace(observed, postgres_block_size_bytes=observed.postgres_block_size_bytes * 2),
    ):
        resolver.return_value = changed
        with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="scope_or_settings_changed"):
            await importer._admission_database_guard(build_identity, expected)


@pytest.mark.asyncio
async def test_admission_run_toast_success_and_failure_edges(monkeypatch):
    geometry_payload = _wal_tracker_admission().geometry
    layout = _logical_capacity_layout(geometry_payload, "import_run")
    monkeypatch.setattr(importer, "_provider_directory_relation_oid", AsyncMock(return_value=12))
    monkeypatch.setattr(
        importer, "_provider_directory_profile_capacity_relation_layout", AsyncMock(return_value=layout)
    )
    chunk_count = AsyncMock(return_value=1)
    monkeypatch.setattr(importer, "_provider_directory_profile_toast_chunk_count", chunk_count)
    database_identity = _numeric_database_identity()
    geometry = SimpleNamespace(postgres_default_toast_compression="pglz")
    wal_input = SimpleNamespace(import_run_update=SimpleNamespace(deleted_toast_chunks=1))
    await importer._assert_admission_run_toast(
        "run_" + "1" * 32,
        database_identity,
        geometry,
        wal_input,
    )
    chunk_count.return_value = 2
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="import_run_toast_changed",
    ):
        await importer._assert_admission_run_toast(
            "run_" + "1" * 32,
            database_identity,
            geometry,
            wal_input,
        )


def test_capacity_admission_context_invalid_edge():
    token = importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(object())
    try:
        with pytest.raises(RuntimeError, match="admission_invalid"):
            importer._provider_directory_profile_capacity_admission()
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)


@pytest.mark.asyncio
async def test_capacity_scratch_row_byte_and_deadline_edges(monkeypatch):
    admission = _wal_tracker_admission()
    future_lease = SimpleNamespace(
        max_build_deadline=(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1))
    )
    admission = dataclasses.replace(admission, lease=future_lease)
    token = importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(admission)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_relation_bytes",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_profile_wal_budget",
        AsyncMock(),
    )
    try:
        with pytest.raises(RuntimeError, match="row_limit_exceeded"):
            await importer._assert_provider_directory_profile_capacity_scratch(
                "profile_stage",
                (),
                observed_rows=2,
                maximum_rows=1,
            )
        assert (
            await importer._assert_provider_directory_profile_capacity_scratch(
                "profile_stage",
                (),
                observed_rows=1,
                maximum_rows=1,
            )
            == 0
        )
        expired = dataclasses.replace(
            admission,
            lease=SimpleNamespace(
                max_build_deadline=(datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=1))
            ),
        )
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(expired)
        with pytest.raises(
            importer.ProviderDirectoryCapacityLeaseError,
            match="max_build_deadline",
        ):
            await importer._assert_provider_directory_profile_capacity_scratch(
                "profile_stage",
                (),
            )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)
