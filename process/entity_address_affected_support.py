# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare and reconcile the six affected Address support families."""

import datetime
from contextlib import asynccontextmanager
from types import SimpleNamespace


def _require(condition):
    if not condition:
        raise RuntimeError("entity_address_affected_support_invalid")


def _support_models(native):
    return tuple(native.SUPPORT_TABLE_MODELS)


async def populate(native, schema, stage, classes, available, context, source_run_id):
    """Use the maintained generators only against the bounded Unified candidate."""
    from process import entity_address_preparation_admission as admitted

    floor = int(
        await admitted.read_scalar(
            native.db, f"SELECT COALESCE(MAX(evidence_id),0) FROM {schema}.entity_address_evidence"
        )
        or 0
    )
    _require(floor >= 0)
    context["affected_evidence_id_floor"] = floor
    statements = [
        native._evidence_from_stage_sql(
            schema,
            classes[native.EntityAddressEvidence].__tablename__,
            stage,
            source_run_id=source_run_id,
            node_id=None,
            evidence_id_floor=floor,
        )
    ]
    for model, generator in (
        (native.EntityAddressPlanBridge, native._plan_bridge_sql),
        (native.EntityAddressNetworkBridge, native._network_bridge_sql),
        (native.EntityAddressProcedureBridge, native._procedure_bridge_sql),
        (native.EntityAddressMedicationBridge, native._medication_bridge_sql),
    ):
        statements.append(generator(schema, classes[model].__tablename__, stage))
    for statement in statements:
        await admitted.stage_status(statement)
    await _populate_facility(native, schema, stage, classes, available, context, source_run_id)
    counts_by_target = {}
    for model, staged in classes.items():
        counts_by_target[model.__main_table__] = int(
            await admitted.read_scalar(native.db, f"SELECT count(*) FROM {schema}.{staged.__tablename__}") or 0
        )
    context.update(support_stage_populated=True, support_stage_skipped=False, support_counts=counts_by_target)
    return counts_by_target


async def _populate_facility(native, schema, stage, classes, available, context, source_run_id):
    from process import entity_address_affected_patch as affected
    from process import entity_address_preparation_admission as admitted

    has_facilities = await admitted.read_scalar(
        native.db, f"SELECT EXISTS(SELECT 1 FROM {schema}.{stage} WHERE entity_type='facility_anchor')"
    )
    context["affected_facility_generated"] = False
    if not has_facilities:
        return
    capture = affected.current_capture()
    recipe = capture.affected_scope.get("facility_recipe")
    _require(isinstance(recipe, dict))
    expected_by_field = {
        "enabled": native._is_env_enabled(
            "HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_FACILITY_CANDIDATES", native.DEFAULT_BUILD_FACILITY_CANDIDATES
        ),
        "include_nppes": native._is_env_enabled("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_NPPES", False),
        "include_other_identifier": native._is_env_enabled(
            "HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_OTHER_IDENTIFIER", False
        ),
        "candidate_limit": native._env_int("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_LIMIT", 25, minimum=1),
    }
    _require(all(recipe.get(key) == expected_value for key, expected_value in expected_by_field.items()))
    if not recipe["enabled"]:
        return
    options = native._facility_candidate_options(SimpleNamespace(available=available))
    _require(options == recipe["options"])
    from process.provider_directory_ordinary_facility_inputs import require_facility_inputs

    async with admitted.native_transaction() as session:
        await require_facility_inputs(session, schema, capture)
        await admitted.stage_status(
            native._facility_anchor_npi_candidate_sql(
                schema,
                classes[native.FacilityAnchorNPICandidate].__tablename__,
                stage,
                source_run_id=source_run_id,
                candidate_options_by_name=options,
            )
        )
    context["affected_facility_generated"] = True


async def finalize_identities(native, descriptor, context):
    """Expose stable new evidence keys and retain every surviving manual review."""
    from process import entity_address_preparation_admission as admitted

    stage_by_target = {target: name for target, name, _oid in descriptor.support_stages}
    _require(set(stage_by_target) == {model.__main_table__ for model in _support_models(native)})
    schema = descriptor.schema
    async with admitted.native_transaction():
        evidence = stage_by_target[native.EntityAddressEvidence.__main_table__]
        floor = context["affected_evidence_id_floor"]
        _require(type(floor) is int and floor >= 0)
        _require(
            await native.db.scalar(
                f"SELECT EXISTS(SELECT 1 FROM {schema}.{evidence} WHERE evidence_id<=:floor)", floor=floor
            )
            is False
        )
        if context.get("affected_facility_generated"):
            await _reconcile_prepared_facility(native, descriptor, stage_by_target["facility_anchor_npi_candidate"])


def _natural_match(left, right):
    return " AND ".join(
        f"{left}.{column} IS NOT DISTINCT FROM {right}.{column}"
        for column in ("location_key", "facility_anchor_id", "candidate_npi", "candidate_method")
    )


async def _reconcile_prepared_facility(native, descriptor, stage):
    schema, scope = descriptor.schema, descriptor.affected_location_table
    conflicts = await native.db.scalar(f"""
        SELECT EXISTS(SELECT 1 FROM {schema}.facility_anchor_npi_candidate previous
          JOIN {schema}.{scope} scope ON scope.location_key=previous.location_key
         WHERE previous.reviewed_at IS NOT NULL OR previous.reviewed_by IS NOT NULL
            OR previous.review_status IN ('approved','rejected')
      GROUP BY previous.location_key,previous.facility_anchor_id,previous.candidate_npi,previous.candidate_method
        HAVING COUNT(DISTINCT jsonb_build_array(previous.review_status,previous.reviewed_by,previous.reviewed_at)::text)>1)
    """)
    _require(conflicts is False)
    await native.db.status(f"""
        WITH matches AS MATERIALIZED (
            SELECT candidate.candidate_id AS new_id, previous.candidate_id AS retained_id,
                   previous.review_status,previous.reviewed_by,previous.reviewed_at,
                   ROW_NUMBER() OVER(PARTITION BY candidate.candidate_id ORDER BY
                     (previous.reviewed_at IS NOT NULL OR previous.reviewed_by IS NOT NULL
                      OR previous.review_status IN ('approved','rejected')) DESC,
                     previous.reviewed_at DESC NULLS LAST,previous.candidate_id) AS preference
              FROM {schema}.{stage} candidate JOIN {schema}.facility_anchor_npi_candidate previous
                ON {_natural_match("candidate", "previous")}
        )
        UPDATE {schema}.{stage} AS candidate SET candidate_id=matches.retained_id,
               review_status=matches.review_status,reviewed_by=matches.reviewed_by,reviewed_at=matches.reviewed_at
          FROM matches WHERE matches.preference=1 AND candidate.candidate_id=matches.new_id
    """)


def apply_statements(native, descriptor, context):
    """Replace current bridges and retain displaced evidence and Facility review history."""
    schema, scope = descriptor.schema, descriptor.affected_location_table
    retirement_time, _facility_time = transition_time_sql(native, descriptor)
    stage_by_target = {target_relation: stage for target_relation, stage, _oid in descriptor.support_stages}
    statements = []
    for model in (
        native.EntityAddressPlanBridge,
        native.EntityAddressNetworkBridge,
        native.EntityAddressProcedureBridge,
        native.EntityAddressMedicationBridge,
    ):
        target_relation = model.__main_table__
        columns = ",".join(native._model_column_names(model))
        statements.extend(
            (
                f"DELETE FROM {schema}.{target_relation} AS live USING {schema}.{scope} scope WHERE live.location_key=scope.location_key",
                f"INSERT INTO {schema}.{target_relation} ({columns}) SELECT {columns} FROM {schema}.{stage_by_target[target_relation]}",
            )
        )
    evidence = stage_by_target["entity_address_evidence"]
    columns = ",".join(native._model_column_names(native.EntityAddressEvidence))
    statements.extend(
        (
            f"UPDATE {schema}.entity_address_evidence AS live SET retired_at={retirement_time} FROM {schema}.{scope} scope WHERE live.location_key=scope.location_key AND live.retired_at IS NULL",
            f"INSERT INTO {schema}.entity_address_evidence ({columns}) SELECT {columns} FROM {schema}.{evidence}",
        )
    )
    statements.extend(_facility_apply_statements(native, descriptor, context))
    return tuple(statements)


def _facility_apply_statements(native, descriptor, context):
    schema, scope = descriptor.schema, descriptor.affected_location_table
    stage = next(name for target, name, _oid in descriptor.support_stages if target == "facility_anchor_npi_candidate")
    columns = native._model_column_names(native.FacilityAnchorNPICandidate)
    assignments = ",".join(f"{column}=candidate.{column}" for column in columns if column != "candidate_id")
    complete = descriptor.facility_generated
    _evidence_time, update_time = transition_time_sql(native, descriptor)
    absence = (
        f"NOT EXISTS(SELECT 1 FROM {schema}.{stage} candidate WHERE candidate.candidate_id=live.candidate_id)"
        if complete
        else f"NOT EXISTS(SELECT 1 FROM {schema}.{descriptor.stage_table} candidate WHERE candidate.location_key=live.location_key)"
    )
    statements = [
        f"UPDATE {schema}.facility_anchor_npi_candidate AS live SET candidate_status='inactive',updated_at={update_time} FROM {schema}.{scope} scope WHERE live.location_key=scope.location_key AND ({absence})"
    ]
    if complete:
        names = ",".join(columns)
        statements.extend(
            (
                f"UPDATE {schema}.facility_anchor_npi_candidate AS live SET {assignments} FROM {schema}.{stage} candidate WHERE live.candidate_id=candidate.candidate_id AND {_natural_match('live', 'candidate')}",
                f"INSERT INTO {schema}.facility_anchor_npi_candidate ({names}) SELECT {names} FROM {schema}.{stage} candidate WHERE NOT EXISTS(SELECT 1 FROM {schema}.facility_anchor_npi_candidate live WHERE live.candidate_id=candidate.candidate_id)",
            )
        )
    return tuple(statements)


async def validate(native, descriptor):
    """Require every current support candidate to reference the verified new Unified rows."""
    schema = descriptor.schema
    _require(
        {target for target, _stage, _oid in descriptor.support_stages}
        == {model.__main_table__ for model in _support_models(native)}
    )
    evidence = next(stage for target, stage, _oid in descriptor.support_stages if target == "entity_address_evidence")
    _require(
        await native.db.scalar(
            f"SELECT EXISTS(SELECT 1 FROM {schema}.{evidence} candidate JOIN {schema}.entity_address_evidence previous USING(evidence_id))"
        )
        is False
    )
    facility = next(
        stage for target, stage, _oid in descriptor.support_stages if target == "facility_anchor_npi_candidate"
    )
    conflicting_id = await native.db.scalar(
        f"SELECT EXISTS(SELECT 1 FROM {schema}.{facility} candidate JOIN {schema}.facility_anchor_npi_candidate previous ON previous.candidate_id=candidate.candidate_id WHERE NOT ({_natural_match('previous', 'candidate')}))"
    )
    _require(conflicting_id is False)
    for target, stage, _oid in descriptor.support_stages:
        missing = await native.db.scalar(
            f"SELECT EXISTS(SELECT 1 FROM {schema}.{stage} support WHERE NOT EXISTS(SELECT 1 FROM {schema}.{descriptor.stage_table} candidate WHERE candidate.location_key=support.location_key))"
        )
        _require(missing is False)


async def is_bridge_reuse_available(native, db_schema: str, build_network_bridge: bool) -> bool:
    """Preserve the existing bridge-reuse preflight for every ordinary/default path."""
    bridge_models: list[type] = [
        native.EntityAddressPlanBridge,
        native.EntityAddressProcedureBridge,
        native.EntityAddressMedicationBridge,
    ]
    if build_network_bridge:
        bridge_models.append(native.EntityAddressNetworkBridge)
    for model in bridge_models:
        if not await native._has_table(db_schema, model.__main_table__):
            return False
    return True


def legacy_patch_models(native, build_network_bridge: bool) -> list[type]:
    """Keep the existing default support-patch family unchanged."""
    models: list[type] = [
        native.EntityAddressEvidence,
        native.EntityAddressPlanBridge,
        native.EntityAddressProcedureBridge,
        native.EntityAddressMedicationBridge,
    ]
    if build_network_bridge:
        models.insert(2, native.EntityAddressNetworkBridge)
    return models


def legacy_evidence_sql(native, context) -> str:
    """Keep the default raw/stage evidence choice on the maintained native helpers."""
    plan = context.plan
    if plan.raw_table:
        return native._evidence_from_raw_sql(
            context.db_schema,
            context.stage_table_map[native.EntityAddressEvidence],
            plan.raw_table,
            source_run_id=plan.source_run_id,
            node_id=plan.node_id,
        )
    return native._evidence_from_stage_sql(
        context.db_schema,
        context.stage_table_map[native.EntityAddressEvidence],
        context.stage_table,
        source_run_id=plan.source_run_id,
        node_id=plan.node_id,
        affected_group_table=(plan.affected_group_table if context.is_partial_support_patch else None),
    )


@asynccontextmanager
async def inference_inputs(schema):
    """Keep optional ordinary inference on its captured inputs in one Builder transaction."""
    from process import entity_address_affected_patch as affected
    from process import entity_address_preparation_admission as admitted

    capture = affected.current_capture()
    if capture is None:
        yield
        return
    from process.provider_directory_ordinary_facility_inputs import require_facility_inputs

    async with admitted.native_transaction() as session:
        await require_facility_inputs(session, schema, capture)
        yield


def transition_time_sql(native, descriptor):
    """Render the same genuine preparation instant in each existing column's native type."""
    try:
        observed = datetime.datetime.fromisoformat(descriptor.transition_at)
    except TypeError, ValueError:
        raise RuntimeError("entity_address_affected_support_invalid") from None
    _require(observed.utcoffset() == datetime.timedelta(0))
    _require(observed.isoformat(timespec="microseconds") == descriptor.transition_at)
    evidence = native._sql_literal(descriptor.transition_at) + "::timestamptz"
    facility = native._sql_literal(observed.replace(tzinfo=None).isoformat(timespec="microseconds")) + "::timestamp"
    return evidence, facility
