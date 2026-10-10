# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Assemble the ordered Provider Directory Profile control-WAL ledger."""

from __future__ import annotations

from process.provider_directory_profile_capacity_control_budget import (
    _checkpoint_payload_control_operation,
    _commit_control_operation,
    _control_wal_operation,
    _fixed_control_operation,
    _metadata_control_operation,
    _row_lock_control_operation,
)
from process.provider_directory_profile_capacity_target import _checked_add
from process.provider_directory_profile_capacity_types import (
    _CONTROL_WAL_OPERATION_ORDER,
    CONTROL_WAL_AFFECTED_NPI_DELTA_STATEMENT_COUNT,
    CONTROL_WAL_ANALYZE_UPPER_BOUND_BYTES_PER_STATEMENT,
    CONTROL_WAL_ARTIFACT_LAYOUT_STATEMENT_COUNT,
    CONTROL_WAL_ARTIFACT_SCOPE_NAMES,
    CONTROL_WAL_ARTIFACT_SCOPE_TABLE_COUNT,
    CONTROL_WAL_CHECKPOINT_RETIRE_UPDATE_COUNT,
    CONTROL_WAL_DDL_UPPER_BOUND_BYTES_PER_STATEMENT,
    CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT,
    CONTROL_WAL_FAILURE_CHECKPOINT_UPDATE_COUNT,
    CONTROL_WAL_PROFILE_STAGE_ANALYZE_STATEMENT_COUNT,
    CONTROL_WAL_PROFILE_STAGE_DROP_STATEMENT_COUNT,
    CONTROL_WAL_PROFILE_STAGE_LAYOUT_STATEMENT_COUNT,
    CONTROL_WAL_PROFILE_STAGE_REINITIALIZE_DROP_STATEMENT_COUNT,
    ProfileControlWalPlanInput,
    ProviderDirectoryProfileCapacityGeometry,
    ProviderDirectoryProfileControlWalOperation,
)


def _artifact_control_operations(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    artifact_batch_count: int,
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    return (
        _fixed_control_operation(
            geometry,
            "pre_cutover",
            "artifact_scope_recovery_drop",
            CONTROL_WAL_ARTIFACT_SCOPE_TABLE_COUNT,
            CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
        _fixed_control_operation(
            geometry,
            "pre_cutover",
            "artifact_scope_layout",
            CONTROL_WAL_ARTIFACT_LAYOUT_STATEMENT_COUNT,
            CONTROL_WAL_DDL_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
        _commit_control_operation(
            geometry,
            "artifact_scope_payload",
            artifact_batch_count,
        ),
        _fixed_control_operation(
            geometry,
            "pre_cutover",
            "artifact_scope_analyze",
            CONTROL_WAL_ARTIFACT_SCOPE_TABLE_COUNT,
            CONTROL_WAL_ANALYZE_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
    )


def _stage_control_operations(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    checkpoint_insert: tuple[int, int],
    checkpoint_update: tuple[int, int],
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    reinitialize = _control_wal_operation(
        geometry,
        ("pre_cutover", "profile_stage_reinitialize"),
        operation_count=1,
        metadata_data_bytes_per_operation=checkpoint_update[0],
        metadata_wal_bytes_per_operation=checkpoint_update[1],
        fixed_statements_per_operation=(CONTROL_WAL_PROFILE_STAGE_REINITIALIZE_DROP_STATEMENT_COUNT),
        fixed_wal_bytes_per_operation=(
            CONTROL_WAL_PROFILE_STAGE_REINITIALIZE_DROP_STATEMENT_COUNT
            * CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT
        ),
        commits_per_operation=0,
    )
    initialize = _control_wal_operation(
        geometry,
        ("pre_cutover", "profile_stage_initialize"),
        operation_count=1,
        metadata_data_bytes_per_operation=checkpoint_insert[0],
        metadata_wal_bytes_per_operation=checkpoint_insert[1],
        fixed_statements_per_operation=(CONTROL_WAL_PROFILE_STAGE_LAYOUT_STATEMENT_COUNT),
        fixed_wal_bytes_per_operation=(
            CONTROL_WAL_PROFILE_STAGE_LAYOUT_STATEMENT_COUNT * CONTROL_WAL_DDL_UPPER_BOUND_BYTES_PER_STATEMENT
        ),
    )
    return reinitialize, initialize


def _evidence_control_operations(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    evidence_batch_count: int,
    checkpoint_update: tuple[int, int],
    import_run_update: tuple[int, int],
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    return (
        _metadata_control_operation(
            geometry,
            "pre_cutover",
            "evidence_progress_start",
            1,
            *import_run_update,
        ),
        _checkpoint_payload_control_operation(
            geometry,
            "evidence_payload",
            evidence_batch_count,
        ),
        _metadata_control_operation(
            geometry,
            "pre_cutover",
            "evidence_checkpoint_advance",
            evidence_batch_count,
            *checkpoint_update,
        ),
        _metadata_control_operation(
            geometry,
            "pre_cutover",
            "evidence_import_run_progress",
            evidence_batch_count,
            *import_run_update,
        ),
        _fixed_control_operation(
            geometry,
            "pre_cutover",
            "evidence_stage_analyze",
            1,
            CONTROL_WAL_ANALYZE_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
        _metadata_control_operation(
            geometry,
            "pre_cutover",
            "evidence_checkpoint_complete",
            1,
            *checkpoint_update,
        ),
    )


def _affected_control_operations(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    affected_source_count: int,
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    if geometry.materialization_mode == "full_swap":
        return (
            _checkpoint_payload_control_operation(geometry, "affected_npi_payload", 0),
            _fixed_control_operation(
                geometry, "pre_cutover", "affected_npi_analyze", 0, CONTROL_WAL_ANALYZE_UPPER_BOUND_BYTES_PER_STATEMENT
            ),
        )
    affected_payload_count = _checked_add(
        affected_source_count,
        CONTROL_WAL_AFFECTED_NPI_DELTA_STATEMENT_COUNT,
    )
    if geometry.bounded_admission:
        affected_payload_count *= (
            geometry.max_affected_npis + geometry.artifact_scope_batch_size - 1
        ) // geometry.artifact_scope_batch_size
    return (
        _checkpoint_payload_control_operation(
            geometry,
            "affected_npi_payload",
            affected_payload_count,
        ),
        _fixed_control_operation(
            geometry,
            "pre_cutover",
            "affected_npi_analyze",
            1,
            CONTROL_WAL_ANALYZE_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
    )


def _profile_control_operations(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    compact_batch_count: int,
    checkpoint_update: tuple[int, int],
    import_run_update: tuple[int, int],
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    return (
        _metadata_control_operation(
            geometry,
            "pre_cutover",
            "profile_progress_start",
            1,
            *import_run_update,
        ),
        _checkpoint_payload_control_operation(
            geometry,
            "profile_payload",
            compact_batch_count,
        ),
        _metadata_control_operation(
            geometry,
            "pre_cutover",
            "profile_checkpoint_advance",
            compact_batch_count,
            *checkpoint_update,
        ),
        _metadata_control_operation(
            geometry,
            "pre_cutover",
            "profile_import_run_progress",
            compact_batch_count,
            *import_run_update,
        ),
        _fixed_control_operation(
            geometry,
            "pre_cutover",
            "profile_stage_analyze",
            1,
            CONTROL_WAL_ANALYZE_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
        _metadata_control_operation(
            geometry,
            "pre_cutover",
            "profile_checkpoint_ready",
            1,
            *checkpoint_update,
        ),
    )


def _terminal_control_operations(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    checkpoint_update: tuple[int, int],
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    return (
        _metadata_control_operation(
            geometry,
            "post_cutover",
            "profile_checkpoint_retire",
            CONTROL_WAL_CHECKPOINT_RETIRE_UPDATE_COUNT,
            *checkpoint_update,
        ),
        _fixed_control_operation(
            geometry,
            "post_cutover",
            "profile_stage_drop",
            CONTROL_WAL_PROFILE_STAGE_DROP_STATEMENT_COUNT,
            CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
        _fixed_control_operation(
            geometry,
            "post_cutover",
            "artifact_scope_drop",
            CONTROL_WAL_ARTIFACT_SCOPE_TABLE_COUNT,
            CONTROL_WAL_DROP_UPPER_BOUND_BYTES_PER_STATEMENT,
        ),
        _metadata_control_operation(
            geometry,
            "failure_reserve",
            "profile_checkpoint_failure_reserve",
            CONTROL_WAL_FAILURE_CHECKPOINT_UPDATE_COUNT,
            *checkpoint_update,
        ),
    )


def _control_wal_phase_total(
    operations: tuple[ProviderDirectoryProfileControlWalOperation, ...],
    phase: str,
) -> int:
    return _checked_add(*(operation.wal_bytes for operation in operations if operation.phase == phase))


def _initial_control_operations(geometry):
    from process.provider_directory_profile_initial_contract import (
        INITIAL_CUTOVER_ATTEMPTS,
        INITIAL_CUTOVER_STATEMENTS,
        InitialCapacityGeometry,
    )

    if isinstance(geometry, InitialCapacityGeometry):
        return (
            _control_wal_operation(
                geometry,
                ("pre_cutover", "initial_cutover"),
                operation_count=INITIAL_CUTOVER_ATTEMPTS,
                fixed_statements_per_operation=INITIAL_CUTOVER_STATEMENTS,
                fixed_wal_bytes_per_operation=INITIAL_CUTOVER_STATEMENTS
                * CONTROL_WAL_DDL_UPPER_BOUND_BYTES_PER_STATEMENT,
                commits_per_operation=1,
            ),
        )
    return ()


def _control_wal_operation_ledger(
    geometry: ProviderDirectoryProfileCapacityGeometry,
    plan_input: ProfileControlWalPlanInput,
    metadata_mutation_bounds: tuple[
        tuple[int, int],
        tuple[int, int],
        tuple[int, int],
        tuple[int, int],
    ],
) -> tuple[ProviderDirectoryProfileControlWalOperation, ...]:
    """Return the closed, ordered WAL operation ledger for one build."""

    artifact_batch_count = _checked_add(*(entry.batch_count for entry in plan_input.artifact_batch_counts))
    operations = (
        *_admission_control_operations(geometry, plan_input, metadata_mutation_bounds[3]),
        _row_lock_control_operation(geometry, "control_maintenance_acquire", 1),
        *_artifact_control_operations(geometry, artifact_batch_count),
        *_stage_control_operations(
            geometry,
            metadata_mutation_bounds[0],
            metadata_mutation_bounds[1],
        ),
        *_evidence_control_operations(
            geometry,
            plan_input.evidence_batch_count,
            metadata_mutation_bounds[1],
            metadata_mutation_bounds[2],
        ),
        *_affected_control_operations(
            geometry,
            plan_input.affected_source_count,
        ),
        *_profile_control_operations(
            geometry,
            plan_input.compact_batch_count,
            metadata_mutation_bounds[1],
            metadata_mutation_bounds[2],
        ),
        _commit_control_operation(geometry, "control_maintenance_release", 1),
        _row_lock_control_operation(
            geometry,
            "cutover_row_lock",
            plan_input.cutover_row_lock_count,
        ),
        *_terminal_control_operations(
            geometry,
            metadata_mutation_bounds[1],
        ),
    )
    return operations + _initial_control_operations(geometry)


def _checkpoint_metadata_values(fhir, build, batch_plan, has_existing_artifacts):
    """Preserve the exact checkpoint preimage used by preventive accounting."""
    return {
        "build_id": fhir._provider_directory_profile_build_id(build),
        "strategy_version": fhir.profile_artifact.PROFILE_BUILD_STRATEGY_VERSION,
        "schema_version": fhir.profile_artifact.PROFILE_SCHEMA_VERSION,
        "resume_lineage_hash": build.resume_lineage_hash,
        "owner_run_id": build.owner_run_id,
        "profile_as_of": build.profile_as_of,
        "executable_plan_hash": batch_plan.fingerprint,
        "materialization_mode": build.materialization_mode,
        "capacity_geometry_status": build.capacity_geometry_status,
        "capacity_geometry_hash": build.capacity_geometry_hash,
        "capacity_geometry_json": build.capacity_geometry_json,
        "source_ids": build.source_ids,
        "retained_source_ids": build.retained_source_ids,
        "dataset_ids": build.dataset_ids,
        "evidence_stage": build.evidence_stage,
        "profile_stage": build.profile_stage,
        "current_source_vector_hash": build.current_source_vector_hash,
        "desired_source_vector_hash": build.desired_source_vector_hash,
        "current_source_context_vector_hash": build.current_source_context_vector_hash,
        "desired_source_context_vector_hash": build.desired_source_context_vector_hash,
        "refresh_source_ids": build.source_ids,
        "removed_source_ids": build.removed_source_ids,
        "affected_npi_stage": build.affected_npi_stage,
        "has_existing_artifacts": has_existing_artifacts,
        "evidence_total_batches": len(batch_plan.evidence_batches),
        "profile_total_batches": len(batch_plan.compact_batches),
    }


async def _reserve_checkpoint_claim(fhir, admission, metadata_by_field):
    if admission is None:
        return
    fhir._checked_serialized_metadata_payload_bytes(
        metadata_by_field,
        fixed_row_overhead=4_096,
    )
    await fhir._reserve_provider_directory_profile_wal_budget(
        admission,
        control_operation_counts={
            "profile_stage_reinitialize": 1,
            "profile_stage_initialize": 1,
        },
    )


async def _checkpoint_claim_guards(fhir, admission, build, evidence_fence, profile_fence):
    if admission is None:
        return
    fhir._assert_provider_directory_profile_capacity_build(
        admission,
        build,
        evidence_fence,
        profile_fence,
    )
    await fhir._assert_provider_directory_profile_capacity_consumption(admission, build)
    await fhir._apply_provider_directory_profile_capacity_settings(admission)


async def _create_checkpoint_stages(fhir, build):
    """Install complete empty logged layouts before any scratch projection."""
    await fhir.db.status(
        fhir.profile_artifact.profile_evidence_table_sql(
            build.schema,
            build.evidence_stage,
            logged=True,
        )
    )
    await fhir.db.status(
        fhir.profile_artifact.profile_table_sql(
            build.schema,
            build.profile_stage,
            logged=True,
        )
    )
    # Build every secondary index while the logged stages are empty.
    # Scratch projections then see the complete write layout and no bulk index
    # build can emit unforecast data, temp files, or WAL.
    for statement in fhir.profile_artifact.profile_index_statements(
        build.schema,
        build.evidence_stage,
        evidence=True,
    ):
        await fhir.db.status(statement)
    for statement in fhir.profile_artifact.profile_index_statements(
        build.schema,
        build.profile_stage,
        evidence=False,
    ):
        await fhir.db.status(statement)
    if build.materialization_mode == "source_delta":
        if build.affected_npi_stage is None:
            raise RuntimeError("provider_directory_profile_affected_stage_missing")
        await fhir.db.status(
            f"CREATE TABLE "
            f"{fhir._provider_directory_profile_build_ref(build, build.affected_npi_stage)} "
            "(npi bigint PRIMARY KEY);"
        )


async def _checkpoint_stage_oids(fhir, build):
    return {
        "evidence_stage_oid": await fhir._require_provider_directory_profile_stage_oid(
            build.schema,
            build.evidence_stage,
        ),
        "profile_stage_oid": await fhir._require_provider_directory_profile_stage_oid(
            build.schema,
            build.profile_stage,
        ),
        "affected_npi_stage_oid": (
            await fhir._require_provider_directory_profile_stage_oid(
                build.schema,
                build.affected_npi_stage,
            )
            if build.affected_npi_stage is not None
            else None
        ),
    }


async def _checkpoint_stage_fingerprints(fhir, build, stage_oids_by_name):
    return {
        "evidence_stage_storage_fingerprint": (
            await fhir._provider_directory_profile_stage_storage_fingerprint(
                build.schema,
                build.evidence_stage,
                expected_oid=stage_oids_by_name["evidence_stage_oid"],
                lock_relation=True,
            )
        ),
        "profile_stage_storage_fingerprint": (
            await fhir._provider_directory_profile_stage_storage_fingerprint(
                build.schema,
                build.profile_stage,
                expected_oid=stage_oids_by_name["profile_stage_oid"],
                lock_relation=True,
            )
        ),
        "affected_npi_stage_storage_fingerprint": (
            await fhir._provider_directory_profile_stage_storage_fingerprint(
                build.schema,
                build.affected_npi_stage,
                expected_oid=stage_oids_by_name["affected_npi_stage_oid"],
                lock_relation=True,
            )
            if build.affected_npi_stage is not None and stage_oids_by_name["affected_npi_stage_oid"] is not None
            else None
        ),
    }


_CHECKPOINT_INSERT_SQL = """
                INSERT INTO {checkpoint_ref} (
                    build_id, strategy_version, schema_version,
                    resume_lineage_hash, owner_run_id, state, profile_as_of,
                    executable_plan_hash, materialization_mode,
                    capacity_geometry_status, capacity_geometry_hash,
                    capacity_geometry_json,
                    source_ids, retained_source_ids, dataset_ids,
                    evidence_stage, profile_stage, evidence_stage_oid,
                    profile_stage_oid,
                    evidence_stage_storage_fingerprint,
                    profile_stage_storage_fingerprint,
                    affected_npi_stage_storage_fingerprint,
                    evidence_target_oid, profile_target_oid,
                    current_source_vector_hash, desired_source_vector_hash,
                    current_source_context_vector_hash,
                    desired_source_context_vector_hash,
                    refresh_source_ids, removed_source_ids,
                    affected_npi_stage, affected_npi_stage_oid,
                    has_existing_artifacts, evidence_next_batch,
                    evidence_total_batches, profile_next_batch,
                    profile_total_batches, created_at, updated_at
                ) VALUES (
                    :build_id, :strategy_version, :schema_version,
                    :resume_lineage_hash, :owner_run_id,
                    'building_evidence', :profile_as_of,
                    :executable_plan_hash, :materialization_mode,
                    :capacity_geometry_status, :capacity_geometry_hash,
                    CAST(:capacity_geometry_json AS jsonb),
                    CAST(:source_ids AS jsonb),
                    CAST(:retained_source_ids AS jsonb),
                    CAST(:dataset_ids AS jsonb), :evidence_stage,
                    :profile_stage, :evidence_stage_oid, :profile_stage_oid,
                    :evidence_stage_storage_fingerprint,
                    :profile_stage_storage_fingerprint,
                    :affected_npi_stage_storage_fingerprint,
                    :evidence_target_oid, :profile_target_oid,
                    :current_source_vector_hash,
                    :desired_source_vector_hash,
                    :current_source_context_vector_hash,
                    :desired_source_context_vector_hash,
                    CAST(:refresh_source_ids AS jsonb),
                    CAST(:removed_source_ids AS jsonb),
                    :affected_npi_stage, :affected_npi_stage_oid,
                    :has_existing_artifacts, 0,
                    :evidence_total_batches, 0, :profile_total_batches,
                    now(), now()
                );
                """


_CHECKPOINT_CLAIM_SQL = """
            UPDATE {checkpoint_ref}
               SET owner_run_id = :owner_run_id,
                   state = :state,
                   last_error = NULL,
                   updated_at = now()
             WHERE build_id = :build_id
               AND capacity_geometry_status = :capacity_geometry_status
               AND capacity_geometry_hash IS NOT DISTINCT FROM
                   :capacity_geometry_hash
               AND capacity_geometry_json::jsonb IS NOT DISTINCT FROM
                   CAST(:capacity_geometry_json AS jsonb)
               AND to_regclass(:evidence_stage_relation)::oid::bigint
                   = evidence_stage_oid
               AND to_regclass(:profile_stage_relation)::oid::bigint
                   = profile_stage_oid
               AND (
                    materialization_mode <> 'source_delta'
                    OR to_regclass(
                        :affected_npi_stage_relation
                    )::oid::bigint = affected_npi_stage_oid
               );
            """


def _checkpoint_insert_values(fhir, build, metadata_by_field, evidence_fence, profile_fence):
    insert_by_field = dict(metadata_by_field)
    for field_name in ("source_ids", "retained_source_ids", "dataset_ids", "removed_source_ids"):
        insert_by_field[field_name] = fhir.json.dumps(list(insert_by_field[field_name]))
    insert_by_field["refresh_source_ids"] = fhir.json.dumps(
        list(build.source_ids) if build.materialization_mode == "source_delta" else []
    )
    insert_by_field["evidence_target_oid"] = evidence_fence.target_oid
    insert_by_field["profile_target_oid"] = profile_fence.target_oid
    return insert_by_field


async def _initialize_checkpoint(
    fhir,
    build,
    checkpoint_by_field,
    metadata_by_field,
    evidence_fence,
    profile_fence,
    batch_plan,
    has_existing_artifacts,
):
    """Keep failed-stage refusal ahead of checkpoint deletion and stage creation."""
    checkpoint_ref = fhir._provider_directory_profile_checkpoint_ref(build.schema)
    await fhir._drop_profile_stages_for_reinitialize(build, checkpoint_by_field)
    await fhir.db.status(
        f"DELETE FROM {checkpoint_ref} WHERE build_id = :build_id;",
        build_id=fhir._provider_directory_profile_build_id(build),
    )
    await _create_checkpoint_stages(fhir, build)
    stage_oids_by_name = await _checkpoint_stage_oids(fhir, build)
    fingerprints_by_name = await _checkpoint_stage_fingerprints(fhir, build, stage_oids_by_name)
    if metadata_by_field is None:
        metadata_by_field = _checkpoint_metadata_values(fhir, build, batch_plan, has_existing_artifacts)
    await fhir.db.status(
        _CHECKPOINT_INSERT_SQL.format(checkpoint_ref=checkpoint_ref),
        **_checkpoint_insert_values(fhir, build, metadata_by_field, evidence_fence, profile_fence),
        **stage_oids_by_name,
        **fingerprints_by_name,
    )
    return fhir._ProviderDirectoryProfileBuildCheckpointState(
        evidence_next_batch=0,
        evidence_total_batches=metadata_by_field["evidence_total_batches"],
        profile_next_batch=0,
        profile_total_batches=metadata_by_field["profile_total_batches"],
        state="building_evidence",
    )


def _checkpoint_claim_state(fhir, checkpoint_state, checkpoint_by_field):
    failed_from_state = (
        (fhir._clean_text(checkpoint_by_field.get("last_error")) or "")
        .rpartition("[checkpoint_state=")[2]
        .removesuffix("]")
    )
    is_evidence_finalized = checkpoint_state.state in {
        "evidence_complete",
        "building_profile",
        "ready",
    } or (
        checkpoint_state.state == "failed" and failed_from_state in {"evidence_complete", "building_profile", "ready"}
    )
    if checkpoint_state.profile_next_batch == checkpoint_state.profile_total_batches:
        return "ready"
    if checkpoint_state.evidence_next_batch == checkpoint_state.evidence_total_batches and (
        checkpoint_state.profile_next_batch > 0 or is_evidence_finalized
    ):
        return "building_profile"
    return "building_evidence"


async def _resume_checkpoint_claim(fhir, build, checkpoint_by_field):
    checkpoint_state = fhir._provider_directory_profile_checkpoint_state(checkpoint_by_field)
    claimed_state = _checkpoint_claim_state(fhir, checkpoint_state, checkpoint_by_field)
    claimed_count = await fhir.db.status(
        _CHECKPOINT_CLAIM_SQL.format(
            checkpoint_ref=fhir._provider_directory_profile_checkpoint_ref(build.schema),
        ),
        state=claimed_state,
        **fhir._profile_checkpoint_relation_params(build),
    )
    if fhir._coerce_rowcount(claimed_count) != 1:
        raise RuntimeError("provider_directory_profile_build_checkpoint_claim_lost")
    return fhir.replace(checkpoint_state, state=claimed_state)


async def claim_checkpoint_rows(
    fhir, build, *, has_existing_artifacts, evidence_build_fence, profile_build_fence, batch_plan=None
):
    """Claim a resumable checkpoint or initialize its exact logged stages."""
    resolved_batch_plan = batch_plan or fhir._provider_directory_profile_build_plan(
        build,
        has_existing_artifacts=has_existing_artifacts,
    )
    checkpoint_ref = fhir._provider_directory_profile_checkpoint_ref(build.schema)
    build_id = fhir._provider_directory_profile_build_id(build)
    admission = fhir._provider_directory_profile_capacity_admission()
    metadata_by_field = (
        _checkpoint_metadata_values(fhir, build, resolved_batch_plan, has_existing_artifacts)
        if admission is not None
        else None
    )
    await _reserve_checkpoint_claim(fhir, admission, metadata_by_field)
    async with fhir.profile_control_custody.transaction(
        fhir,
        fhir.db.transaction(),
        identity=("checkpoint_claim", build, resolved_batch_plan),
        enabled=(admission is not None and admission.geometry.bounded_admission),
    ):
        await _checkpoint_claim_guards(fhir, admission, build, evidence_build_fence, profile_build_fence)
        checkpoint_row = await fhir.db.first(
            f"SELECT * FROM {checkpoint_ref} WHERE build_id = :build_id FOR UPDATE;",
            build_id=build_id,
        )
        checkpoint_by_field = (
            fhir._pagination_checkpoint_row_mapping(checkpoint_row) if checkpoint_row is not None else {}
        )
        if admission is not None:
            await fhir._validate_profile_checkpoint_layout(admission, checkpoint_ref, build_id)
        is_reusable = bool(checkpoint_by_field) and await fhir._is_profile_build_checkpoint_reusable(
            build,
            checkpoint_by_field,
            has_existing_artifacts=has_existing_artifacts,
            evidence_build_fence=evidence_build_fence,
            profile_build_fence=profile_build_fence,
            evidence_total_batches=len(resolved_batch_plan.evidence_batches),
            profile_total_batches=len(resolved_batch_plan.compact_batches),
        )
        if not is_reusable:
            return await _initialize_checkpoint(
                fhir,
                build,
                checkpoint_by_field,
                metadata_by_field,
                evidence_build_fence,
                profile_build_fence,
                resolved_batch_plan,
                has_existing_artifacts,
            )
        return await _resume_checkpoint_claim(fhir, build, checkpoint_by_field)


def _admission_control_operations(geometry, plan_input, consumption_bounds):
    return (
        _row_lock_control_operation(geometry, "admission_row_lock", plan_input.admission_row_lock_count),
        _metadata_control_operation(geometry, "pre_cutover", "capacity_consumption_insert", 3, *consumption_bounds),
    )
