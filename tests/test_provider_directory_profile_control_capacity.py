# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Control-plane WAL projection contracts for Profile builds."""

from __future__ import annotations

import dataclasses

import pytest

from process import provider_directory_profile_capacity as capacity
from tests.test_provider_directory_profile_capacity_projection import _projection_geometry


def _control_metadata_input(relation_name, operation, **overrides):
    metadata_by_field = {
        "relation_name": relation_name,
        "operation": operation,
        "payload_upper_bytes": 1_024,
        "deleted_toast_chunks": 0,
        "main_index_pages": (1,),
        "toast_index_pages": (1,),
    }
    metadata_by_field.update(overrides)
    return capacity.ProviderDirectoryProfileMetadataMutationInput(**metadata_by_field)


def _control_wal_plan_input(
    *,
    artifact_batch_counts=(2, 1, 0, 0, 0, 0, 0, 0, 0),
    evidence_batch_count=4,
    compact_batch_count=3,
    affected_source_count=2,
    admission_row_lock_count=2,
    cutover_row_lock_count=12,
    **overrides,
):
    plan_by_field = {
        "artifact_batch_counts": tuple(
            capacity.ProfileControlArtifactBatchCount(
                artifact_name=artifact_name,
                batch_count=batch_count,
            )
            for artifact_name, batch_count in zip(
                capacity.CONTROL_WAL_ARTIFACT_SCOPE_NAMES,
                artifact_batch_counts,
                strict=True,
            )
        ),
        "artifact_scope_recovery_contract_id": (capacity.ARTIFACT_SCOPE_RECOVERY_CONTRACT_ID),
        "evidence_batch_count": evidence_batch_count,
        "compact_batch_count": compact_batch_count,
        "affected_source_count": affected_source_count,
        "admission_row_lock_count": admission_row_lock_count,
        "cutover_row_lock_count": cutover_row_lock_count,
        "build_checkpoint_insert": _control_metadata_input(
            "build_checkpoint",
            "insert",
        ),
        "build_checkpoint_update": _control_metadata_input(
            "build_checkpoint",
            "update",
        ),
        "import_run_update": _control_metadata_input(
            "import_run",
            "update",
        ),
        "capacity_consumption_insert": _control_metadata_input(
            "capacity_consumption",
            "insert",
        ),
    }
    plan_by_field.update(overrides)
    return capacity.ProfileControlWalPlanInput(**plan_by_field)


def _control_operations_by_name(projection):
    return {operation.operation_name: operation for operation in projection.operations}


def _bound_control_wal_projection(plan_input=None):
    """Build the exact two-pass geometry without hashing the projection."""

    resolved_plan_input = plan_input or _control_wal_plan_input()
    seed_geometry = capacity.revalidate_capacity_geometry(
        dataclasses.replace(
            _projection_geometry(),
            control_wal_plan_input_hash=(capacity.profile_control_wal_plan_input_hash(resolved_plan_input)),
            control_wal_upper_bound_bytes=1,
            control_metadata_data_upper_bound_bytes=1,
        )
    )
    seed_projection = capacity.project_profile_control_wal_capacity(
        seed_geometry,
        resolved_plan_input,
    )
    geometry = capacity.revalidate_capacity_geometry(
        dataclasses.replace(
            seed_geometry,
            control_wal_upper_bound_bytes=(seed_projection.total_control_wal_bytes),
            control_metadata_data_upper_bound_bytes=(seed_projection.total_control_metadata_data_bytes),
        )
    )
    projection = capacity.project_profile_control_wal_capacity(
        geometry,
        resolved_plan_input,
    )
    return geometry, projection


def test_control_wal_projection_derives_exact_batch_operation_counts():
    _, projection = _bound_control_wal_projection()
    operations = _control_operations_by_name(projection)

    assert operations["artifact_scope_payload"].operation_count == 3
    assert operations["evidence_payload"].operation_count == 4
    assert operations["evidence_checkpoint_advance"].operation_count == 4
    assert operations["evidence_import_run_progress"].operation_count == 4
    assert operations["affected_npi_payload"].operation_count == 3
    assert operations["profile_payload"].operation_count == 3
    assert operations["profile_checkpoint_advance"].operation_count == 3
    assert operations["profile_import_run_progress"].operation_count == 3
    assert operations["evidence_progress_start"].operation_count == 1
    assert operations["profile_progress_start"].operation_count == 1
    assert operations["profile_checkpoint_failure_reserve"].operation_count == 1


def test_control_wal_projection_binds_fixed_catalog_statement_envelopes():
    _, projection = _bound_control_wal_projection()
    operations = _control_operations_by_name(projection)
    layout = operations["artifact_scope_layout"]
    stage_reinitialize = operations["profile_stage_reinitialize"]
    stage_initialize = operations["profile_stage_initialize"]

    assert layout.operation_count == (capacity.CONTROL_WAL_ARTIFACT_LAYOUT_STATEMENT_COUNT)
    assert layout.fixed_statement_count == (capacity.CONTROL_WAL_ARTIFACT_LAYOUT_STATEMENT_COUNT)
    assert layout.fixed_statement_wal_bytes == (
        capacity.CONTROL_WAL_ARTIFACT_LAYOUT_STATEMENT_COUNT * capacity.CONTROL_WAL_DDL_UPPER_BOUND_BYTES_PER_STATEMENT
    )
    assert stage_reinitialize.operation_count == 1
    assert stage_reinitialize.metadata_mutation_count == 1
    assert stage_reinitialize.fixed_statement_count == (
        capacity.CONTROL_WAL_PROFILE_STAGE_REINITIALIZE_DROP_STATEMENT_COUNT
    )
    assert stage_reinitialize.commit_count == 0
    assert stage_initialize.operation_count == 1
    assert stage_initialize.metadata_mutation_count == 1
    assert stage_initialize.fixed_statement_count == (capacity.CONTROL_WAL_PROFILE_STAGE_LAYOUT_STATEMENT_COUNT)
    assert operations["artifact_scope_analyze"].operation_count == 9
    assert operations["admission_row_lock"].operation_count == 2
    assert operations["capacity_consumption_insert"].operation_count == 3
    assert operations["cutover_row_lock"].operation_count == 12
    assert operations["admission_row_lock"].wal_bytes_per_operation == (
        capacity.CONTROL_WAL_ROW_LOCK_UPPER_BOUND_BYTES_PER_TUPLE
    )
    assert operations["profile_stage_drop"].operation_count == 3
    assert operations["artifact_scope_drop"].operation_count == 9


def test_control_wal_projection_reserves_each_payload_commit_envelope():
    geometry, projection = _bound_control_wal_projection()
    operations = _control_operations_by_name(projection)

    for operation_name, expected_count in (
        ("artifact_scope_payload", 3),
        ("evidence_payload", 4),
        ("affected_npi_payload", 3),
        ("profile_payload", 3),
    ):
        operation = operations[operation_name]
        assert operation.metadata_wal_bytes == 0
        assert operation.metadata_data_bytes == 0
        expected_lock_bytes = (
            0
            if operation_name == "artifact_scope_payload"
            else capacity.CONTROL_WAL_ROW_LOCK_UPPER_BOUND_BYTES_PER_TUPLE
        )
        assert operation.fixed_statement_count == (expected_count if expected_lock_bytes else 0)
        assert operation.fixed_statement_wal_bytes == expected_count * expected_lock_bytes
        assert operation.wal_bytes_per_operation == (geometry.postgres_block_size_bytes + expected_lock_bytes)
        assert operation.commit_count == expected_count
        assert operation.commit_envelope_bytes == (expected_count * geometry.postgres_block_size_bytes)


def test_control_projection_reserves_repeated_metadata_data_growth():
    geometry, projection = _bound_control_wal_projection()
    operations = _control_operations_by_name(projection)

    for operation_name in (
        "capacity_consumption_insert",
        "profile_stage_reinitialize",
        "profile_stage_initialize",
        "evidence_progress_start",
        "evidence_checkpoint_advance",
        "evidence_import_run_progress",
        "evidence_checkpoint_complete",
        "profile_progress_start",
        "profile_checkpoint_advance",
        "profile_import_run_progress",
        "profile_checkpoint_ready",
        "profile_checkpoint_retire",
        "profile_checkpoint_failure_reserve",
    ):
        operation = operations[operation_name]
        assert operation.metadata_data_bytes_per_operation > 0
        assert operation.metadata_data_bytes == (
            operation.operation_count * operation.metadata_data_bytes_per_operation
        )
    assert projection.total_control_metadata_data_bytes == sum(
        operation.metadata_data_bytes for operation in projection.operations
    )
    assert geometry.control_metadata_data_upper_bound_bytes == (projection.total_control_metadata_data_bytes)
    assert geometry.reservation_bytes_by_storage_class["data"] >= projection.total_control_metadata_data_bytes


def test_control_wal_payload_and_hash_bind_order_geometry_and_counts():
    plan_input = _control_wal_plan_input()
    geometry, projection = _bound_control_wal_projection(plan_input)
    projection_by_field = capacity.profile_control_wal_projection_payload(projection)

    assert projection_by_field["final_cutover_contract_id"] == (capacity.CUTOVER_FORECAST_CONTRACT_ID)
    assert projection_by_field["plan_input"]["artifact_batch_counts"][0] == {
        "artifact_name": "source",
        "batch_count": 2,
    }
    assert projection_by_field["plan_input"]["build_checkpoint_insert"]["main_index_pages"] == [1]
    assert projection_by_field["total_control_metadata_data_bytes"] == (projection.total_control_metadata_data_bytes)
    assert " " not in (capacity.canonical_profile_control_wal_projection_json(projection))
    assert len(capacity.profile_control_wal_projection_hash(projection)) == 64
    assert (
        capacity.revalidate_profile_control_wal_projection(
            geometry,
            projection,
        )
        == projection
    )
    changed_plan_input = _control_wal_plan_input(evidence_batch_count=5)
    _, changed = _bound_control_wal_projection(
        changed_plan_input,
    )
    assert capacity.profile_control_wal_projection_hash(changed) != (
        capacity.profile_control_wal_projection_hash(projection)
    )


def test_control_wal_plan_input_hash_is_canonical_and_geometry_independent():
    plan_input = _control_wal_plan_input()
    payload = capacity.profile_control_wal_plan_input_payload(plan_input)
    canonical_json = capacity.canonical_profile_control_wal_plan_input_json(plan_input)

    assert payload["artifact_batch_counts"][0] == {
        "artifact_name": "source",
        "batch_count": 2,
    }
    assert "capacity_geometry_hash" not in canonical_json
    assert "control_wal_upper_bound_bytes" not in canonical_json
    assert "control_metadata_data_upper_bound_bytes" not in canonical_json
    assert " " not in canonical_json
    assert len(capacity.profile_control_wal_plan_input_hash(plan_input)) == 64
    assert capacity.profile_control_wal_plan_input_hash(
        dataclasses.replace(plan_input, evidence_batch_count=5)
    ) != capacity.profile_control_wal_plan_input_hash(plan_input)


def test_control_wal_projection_requires_signed_plan_input_hash():
    plan_input = _control_wal_plan_input()
    geometry, _ = _bound_control_wal_projection(plan_input)
    changed_plan_input = dataclasses.replace(
        plan_input,
        compact_batch_count=plan_input.compact_batch_count + 1,
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="control_wal_plan_input_hash_mismatch",
    ):
        capacity.project_profile_control_wal_capacity(
            geometry,
            changed_plan_input,
        )


def test_control_wal_revalidation_requires_exact_geometry_upper_bound():
    geometry, projection = _bound_control_wal_projection()
    changed_geometry = capacity.revalidate_capacity_geometry(
        dataclasses.replace(
            geometry,
            control_wal_upper_bound_bytes=(geometry.control_wal_upper_bound_bytes + 1),
        )
    )

    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError,
        match="control_wal_projection_geometry_bound_mismatch",
    ):
        capacity.revalidate_profile_control_wal_projection(
            changed_geometry,
            projection,
        )


def _old_payload_projection(geometry, projection):
    locked_names = {"evidence_payload", "profile_payload", "affected_npi_payload"}
    old_operations = tuple(
        dataclasses.replace(
            operation,
            fixed_statement_count=0,
            fixed_statement_wal_bytes=0,
            wal_bytes_per_operation=geometry.postgres_block_size_bytes if operation.operation_count else 0,
            wal_bytes=operation.commit_envelope_bytes,
        )
        if operation.operation_name in locked_names
        else operation
        for operation in projection.operations
    )
    delta = sum(
        operation.fixed_statement_wal_bytes
        for operation in projection.operations
        if operation.operation_name in locked_names
    )
    old_geometry = capacity.revalidate_capacity_geometry(
        dataclasses.replace(geometry, control_wal_upper_bound_bytes=projection.total_control_wal_bytes - delta)
    )
    old_projection = dataclasses.replace(
        projection,
        capacity_geometry_hash=capacity.capacity_geometry_hash(old_geometry),
        operations=old_operations,
        pre_cutover_wal_bytes=projection.pre_cutover_wal_bytes - delta,
        total_control_wal_bytes=projection.total_control_wal_bytes - delta,
    )
    return old_geometry, old_projection, delta


@pytest.mark.parametrize("counts", [(0, 0, 0), (2, 0, 3), (0, 5, 0), (7, 3, 2)])
def test_checkpoint_payloads_bind_exact_lock_commit_and_phase_totals(counts):
    evidence, compact, affected_sources = counts
    geometry, projection = _bound_control_wal_projection(
        _control_wal_plan_input(
            evidence_batch_count=evidence, compact_batch_count=compact, affected_source_count=affected_sources
        )
    )
    operations = _control_operations_by_name(projection)
    for name in ("evidence_payload", "profile_payload", "affected_npi_payload"):
        operation = operations[name]
        count = operation.operation_count
        assert operation.fixed_statement_count == operation.commit_count == count
        assert operation.fixed_statement_wal_bytes == count * 65536
        assert operation.commit_envelope_bytes == count * 8192
        assert operation.wal_bytes_per_operation == (73728 if count else 0)
        assert operation.wal_bytes == count * 73728
    assert operations["artifact_scope_payload"].fixed_statement_wal_bytes == 0
    assert projection.total_control_wal_bytes == sum(operation.wal_bytes for operation in projection.operations)
    assert projection.pre_cutover_wal_bytes == sum(
        operation.wal_bytes for operation in projection.operations if operation.phase == "pre_cutover"
    )
    assert projection.total_control_wal_bytes == geometry.control_wal_upper_bound_bytes


@pytest.mark.parametrize("counts", [(1, 0, 0), (0, 1, 0), (4, 3, 2)])
def test_old_self_consistent_payload_geometry_requires_regeneration(counts):
    evidence, compact, affected_sources = counts
    geometry, projection = _bound_control_wal_projection(
        _control_wal_plan_input(
            evidence_batch_count=evidence, compact_batch_count=compact, affected_source_count=affected_sources
        )
    )
    old_geometry, old_projection, delta = _old_payload_projection(geometry, projection)
    assert delta == 65536 * sum(
        operation.operation_count
        for operation in projection.operations
        if operation.operation_name in {"evidence_payload", "profile_payload", "affected_npi_payload"}
    )
    assert geometry.control_wal_plan_input_hash == old_geometry.control_wal_plan_input_hash
    assert projection.plan_input == old_projection.plan_input
    assert projection.total_control_metadata_data_bytes == old_projection.total_control_metadata_data_bytes
    assert projection.post_cutover_wal_bytes == old_projection.post_cutover_wal_bytes
    assert projection.failure_reserve_wal_bytes == old_projection.failure_reserve_wal_bytes
    assert capacity.capacity_geometry_hash(geometry) != capacity.capacity_geometry_hash(old_geometry)
    assert capacity.profile_control_wal_projection_hash(projection) != capacity.profile_control_wal_projection_hash(
        old_projection
    )
    assert old_geometry.control_wal_upper_bound_bytes == old_projection.total_control_wal_bytes
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError, match="control_wal_projection_formula_changed"):
        capacity.revalidate_profile_control_wal_projection(old_geometry, old_projection)


def test_zero_payload_full_swap_preserves_zero_lock_exposure():
    from process.provider_directory_profile_capacity_control_budget import _checkpoint_payload_control_operation
    from process.provider_directory_profile_capacity_control_operations import _affected_control_operations

    geometry, _ = _bound_control_wal_projection()
    for name in ("evidence_payload", "profile_payload", "affected_npi_payload"):
        operation = _checkpoint_payload_control_operation(geometry, name, 0)
        assert all(
            getattr(operation, field.name) == 0
            for field in dataclasses.fields(operation)
            if field.name not in {"phase", "operation_name"}
        )
    affected = _affected_control_operations(dataclasses.replace(geometry, materialization_mode="full_swap"), 7)
    assert all(operation.operation_count == operation.wal_bytes == 0 for operation in affected)


def test_checkpoint_payload_checked_product_rejects_signed_overflow():
    from process.provider_directory_profile_capacity_control_budget import _checkpoint_payload_control_operation

    geometry, _ = _bound_control_wal_projection()
    largest_count = (2**63 - 1) // 73728
    operation = _checkpoint_payload_control_operation(geometry, "evidence_payload", largest_count)
    assert operation.wal_bytes == largest_count * 73728
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError, match="control_wal_projection_overflow"):
        _checkpoint_payload_control_operation(geometry, "evidence_payload", largest_count + 1)


def _control_layout_growth_case(*, initial=False, evidence_batches=4, compact_batches=3):
    """Match real measured role counts while the admission mutation covers a larger union."""
    from copy import deepcopy

    from tests.test_provider_directory_profile_initial import _geometry as initial_geometry

    plan = _control_wal_plan_input(
        evidence_batch_count=evidence_batches,
        compact_batch_count=compact_batches,
        build_checkpoint_insert=_control_metadata_input(
            "build_checkpoint", "insert", main_index_pages=(2, 2), toast_index_pages=(2,)
        ),
        build_checkpoint_update=_control_metadata_input(
            "build_checkpoint", "update", main_index_pages=(2, 2), toast_index_pages=(2,)
        ),
        import_run_update=_control_metadata_input(
            "import_run", "update", payload_upper_bytes=65_536, main_index_pages=(2,) * 8, toast_index_pages=(2,)
        ),
        capacity_consumption_insert=_control_metadata_input(
            "capacity_consumption", "insert", main_index_pages=(5, 3, 2, 2), toast_index_pages=(3, 2)
        ),
    )
    base = (
        initial_geometry()
        if initial
        else dataclasses.replace(
            _projection_geometry(), physical_projection_contract_id=capacity.BOUNDED_ADMISSION_CONTRACT_ID
        )
    )
    relations_by_role = deepcopy(base.relations)
    for role, index_count in (("build_checkpoint", 2), ("import_run", 8), ("capacity_consumption", 2)):
        relations_by_role[role].update(main_index_pages=[2] * index_count, toast_index_pages=[2])
    relations_by_role["import_run"]["toastable_column_count"] = 19
    seed = dataclasses.replace(
        base,
        relations=relations_by_role,
        control_wal_plan_input_hash=capacity.profile_control_wal_plan_input_hash(plan),
        control_wal_upper_bound_bytes=1,
        control_metadata_data_upper_bound_bytes=1,
    )
    projected = capacity.project_profile_control_wal_capacity(seed, plan)
    geometry = dataclasses.replace(
        seed,
        control_wal_upper_bound_bytes=projected.total_control_wal_bytes,
        control_metadata_data_upper_bound_bytes=projected.total_control_metadata_data_bytes,
    )
    return geometry, plan, capacity.project_profile_control_wal_capacity(geometry, plan)


@pytest.mark.parametrize("evidence_batches,compact_batches", [(0, 0), (1, 2), (4, 3)])
def test_control_relation_limits_reuse_the_funded_sequence_without_extra_reservation(evidence_batches, compact_batches):
    from copy import deepcopy

    from process.provider_directory_profile_capacity_control_budget import (
        _final_control_index_page_bounds,
        control_relation_index_page_bounds,
    )

    geometry, plan, original_projection = _control_layout_growth_case(
        evidence_batches=evidence_batches,
        compact_batches=compact_batches,
    )
    original_payload = capacity.profile_control_wal_plan_input_payload(plan)
    limits_by_role = control_relation_index_page_bounds(geometry, plan)
    sequence_count = evidence_batches + compact_batches + 2
    assert limits_by_role["import_run"]["main_index_pages"] == list(
        _final_control_index_page_bounds((2,) * 8, sequence_count)
    )
    assert len(limits_by_role["capacity_consumption"]["main_index_pages"]) == 2
    assert len(plan.capacity_consumption_insert.main_index_pages) == 4
    assert len(limits_by_role["capacity_consumption"]["toast_index_pages"]) == 1
    assert len(plan.capacity_consumption_insert.toast_index_pages) == 2
    relations_by_role = deepcopy(geometry.relations)
    for role, limits in limits_by_role.items():
        relations_by_role[role].update(limits)
    funded_geometry = dataclasses.replace(geometry, relations=relations_by_role)
    funded_projection = capacity.project_profile_control_wal_capacity(funded_geometry, plan)
    assert funded_projection.operations == original_projection.operations
    assert funded_geometry.reservation_bytes_by_storage_class == geometry.reservation_bytes_by_storage_class
    assert capacity.profile_control_wal_plan_input_payload(plan) == original_payload
    assert capacity.capacity_geometry_hash(funded_geometry) != capacity.capacity_geometry_hash(geometry)


@pytest.mark.parametrize("initial", [False, True])
def test_admission_producer_signs_funded_control_limits_from_the_original_plan(initial):
    import importlib
    from types import SimpleNamespace

    from process import provider_directory_profile_initial_contract as initial_contract
    from process.provider_directory_profile_capacity_control_budget import control_relation_index_page_bounds

    fhir = importlib.import_module("process.provider_directory_fhir")
    geometry, plan, original_projection = _control_layout_growth_case(initial=initial)
    runtime = fhir.profile_capacity_runtime
    limits = runtime.ProviderDirectoryProfileCapacityLimits(
        **{
            field.name: getattr(geometry, field.name)
            for field in dataclasses.fields(runtime.ProviderDirectoryProfileCapacityLimits)
        }
    )
    input_type = (
        initial_contract.InitialCapacityGeometryInputs
        if initial
        else runtime.ProviderDirectoryProfileCapacityGeometryInputs
    )
    input_by_field = {
        field.name: getattr(geometry, field.name)
        for field in dataclasses.fields(input_type)
        if field.name != "artifact_scope_projected_rows"
    }
    inputs = input_type(**input_by_field, artifact_scope_projected_rows=geometry.max_artifact_scope_rows)
    producer_state = fhir._profile_admission_geometry(
        SimpleNamespace(limits=limits, control_wal_plan_input=plan), inputs
    )
    for role, expected in control_relation_index_page_bounds(geometry, plan).items():
        for field, bound in expected.items():
            assert producer_state.geometry.relations[role][field] == bound
    assert inputs.relations == geometry.relations
    assert producer_state.control_wal_projection.plan_input == plan
    assert producer_state.control_wal_projection.operations == original_projection.operations
    assert producer_state.geometry.reservation_bytes_by_storage_class == geometry.reservation_bytes_by_storage_class
    assert producer_state.geometry.control_wal_plan_input_hash == geometry.control_wal_plan_input_hash
    assert capacity.capacity_geometry_hash(producer_state.geometry) != capacity.capacity_geometry_hash(geometry)


@pytest.mark.parametrize(
    "mutation", ["old_two", "funded", "over_bound", "extra_index", "missing_index", "schema", "name", "toast_columns"]
)
def test_signed_control_layout_limits_stay_closed_after_funded_growth(mutation):
    import importlib
    from copy import deepcopy
    from types import SimpleNamespace

    from process.provider_directory_profile_capacity_control_budget import control_relation_index_page_bounds

    fhir = importlib.import_module("process.provider_directory_fhir")
    old_geometry, plan, _projection = _control_layout_growth_case()
    relations_by_role = deepcopy(old_geometry.relations)
    for role, limits in control_relation_index_page_bounds(old_geometry, plan).items():
        relations_by_role[role].update(limits)
    geometry = old_geometry if mutation == "old_two" else dataclasses.replace(old_geometry, relations=relations_by_role)
    expected = geometry.relations["import_run"]
    actual_by_field = {
        "schema_name": expected["schema"],
        "relation_name": expected["name"],
        "main_index_pages": (4,) + (2,) * 7,
        "toast_index_pages": (2,),
        "toastable_columns": tuple(str(index) for index in range(19)),
    }
    overrides_by_mutation = {
        "over_bound": {"main_index_pages": (max(expected["main_index_pages"]) + 1,) + (2,) * 7},
        "extra_index": {"main_index_pages": actual_by_field["main_index_pages"] + (2,)},
        "missing_index": {"main_index_pages": actual_by_field["main_index_pages"][:-1]},
        "schema": {"schema_name": "other"},
        "name": {"relation_name": "other"},
        "toast_columns": {"toastable_columns": actual_by_field["toastable_columns"] + ("unexpected",)},
    }
    actual_by_field.update(overrides_by_mutation.get(mutation, {}))
    if mutation == "funded":
        fhir._assert_profile_capacity_layout_bounds(SimpleNamespace(**actual_by_field), geometry, "import_run")
    else:
        with pytest.raises(
            fhir.ProviderDirectoryArtifactBuildStale, match="capacity_(index_bound|layout_scope)_changed"
        ):
            fhir._assert_profile_capacity_layout_bounds(SimpleNamespace(**actual_by_field), geometry, "import_run")
    assert old_geometry.relations["import_run"]["main_index_pages"] == [2] * 8


@pytest.mark.parametrize(
    "field,unfunded", [("main_index_pages", [6] * 5), ("main_index_pages", [1000, 2]), ("toast_index_pages", [2, 2, 2])]
)
def test_role_page_bounds_cannot_borrow_unfunded_union_indexes(field, unfunded):
    from copy import deepcopy

    from process.provider_directory_profile_capacity_control_budget import control_relation_index_page_bounds

    geometry, plan, _projection = _control_layout_growth_case()
    relations_by_role = deepcopy(geometry.relations)
    relations_by_role["capacity_consumption"][field] = unfunded
    with pytest.raises(
        capacity.ProviderDirectoryProfileCapacityError, match="control_relation_index_projection_unfunded"
    ):
        control_relation_index_page_bounds(dataclasses.replace(geometry, relations=relations_by_role), plan)
