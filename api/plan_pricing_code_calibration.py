# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Operator-only complete calibration through the ordinary code-projection job."""

import asyncio
from uuid import uuid4

from sqlalchemy import text

from api.plan_pricing_code_contract import SPEC, identity_digest, scope_selectors, validate_params
from api.plan_pricing_code_oracle import CodeInputOracle, require_budget
from api.plan_pricing_code_storage import attachment_name, lock_identity
from api.plan_pricing_projection_contract import lock_provider_generation
from api.plan_release_serving_resolution import PLAN_RELEASE_RESOLUTION_READY, resolve_plan_release_serving_resolution
from db.connection import db
from process import reference_family_archive as native


class _RollbackCalibration(Exception):
    def __init__(self, receipt):
        self.receipt = receipt


async def _publication_state(session, identity):
    from api.ptg2_serving import PTG2_SCHEMA

    attachment = await session.scalar(
        text("SELECT to_regclass(:name)::oid"), {"name": "hp_snapshot_retention." + attachment_name(identity)}
    )
    pins = (
        (
            await session.execute(
                text(f"""SELECT snapshot_id::text FROM {native._quoted(PTG2_SCHEMA)}.ptg2_snapshot_pin
        WHERE owner_type=:contract AND owner_id=:identity ORDER BY snapshot_id::text"""),
                {"contract": identity["contract"], "identity": identity_digest(identity)},
            )
        )
        .scalars()
        .all()
    )
    return attachment, tuple(pins)


async def _cleanup_proof(calibration_by_field):
    """Observe exact transaction-owned stage removal and unchanged publication state."""
    if calibration_by_field.get("schema") is None:
        return
    async with db.transaction() as session:
        await session.execute(text("SELECT set_config('statement_timeout','5000',true)"))
        if (
            await session.scalar(
                text("SELECT to_regnamespace(:schema)::oid"), {"schema": calibration_by_field["schema"]}
            )
            is not None
        ):
            raise ValueError("code calibration stage survived rollback")
        remaining = await session.scalar(
            text("""SELECT count(*) FROM pg_class WHERE relnamespace=:namespace
            AND (relname LIKE 'code_calibration_%' OR relname LIKE 'plan_pricing_%')"""),
            {"namespace": calibration_by_field.get("temporary_namespace_oid", 0)},
        )
        if remaining != 0:
            raise ValueError("code calibration temporary evidence survived rollback")
        if (
            await _publication_state(session, calibration_by_field["identity"])
            != calibration_by_field["publication_before"]
        ):
            raise ValueError("code calibration changed publication or retention pins")
    calibration_by_field["cleanup_verified"] = True


async def _resolved_inputs(session, params, calibration_by_field):
    """Authenticate current immutable scope and ordinary Publisher before creating data."""
    from api import plan_pricing_code_build as build

    for timeout_sql in (
        "SET LOCAL statement_timeout = '5000'",
        "SET LOCAL lock_timeout = '500'",
        "SET LOCAL idle_in_transaction_session_timeout = '15000'",
    ):
        await session.execute(text(timeout_sql))
    await lock_provider_generation(session)
    resolution = await resolve_plan_release_serving_resolution(session, params["plan_release_id"])
    if (
        resolution.state != PLAN_RELEASE_RESOLUTION_READY
        or resolution.selection is None
        or resolution.selection.serving_revision_id != params["serving_revision_id"]
    ):
        raise ValueError("code calibration requires the exact current published revision")
    scope = scope_selectors(params)
    identity, inputs = await build.resolve_inputs(
        session, resolution.selection, (params["code_system"], params["code"]), scope=scope
    )
    if identity_digest(identity) != params["expected_identity_digest"]:
        raise ValueError("code calibration input identity changed before preparation")
    await lock_identity(session, identity)
    await native.protected_publisher_owner(session)
    calibration_by_field.update(identity=identity, publication_before=await _publication_state(session, identity))
    return resolution.selection, identity, inputs, scope


async def _register_stage(session, calibration_by_field):
    """Register an empty native and temporary preimage before creating candidate data."""
    dataset_id = uuid4()
    calibration_by_field["schema"] = native.reference_family_stage_schema(dataset_id)
    if (
        await session.scalar(text("SELECT to_regnamespace(:schema)::oid"), {"schema": calibration_by_field["schema"]})
        is not None
    ):
        raise ValueError("code calibration stage preimage is not empty")
    existing_temp = await session.scalar(
        text("""SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema()
        AND (relname LIKE 'code_calibration_%' OR relname LIKE 'plan_pricing_%')""")
    )
    if existing_temp != 0:
        raise ValueError("code calibration temporary preimage is not empty")
    await session.execute(text("CREATE TEMP TABLE code_calibration_registration(owner uuid) ON COMMIT DROP"))
    calibration_by_field["temporary_namespace_oid"] = await session.scalar(text("SELECT pg_my_temp_schema()"))
    return dataset_id


async def _prepare(session, params, context, calibration_by_field):
    """Compare the complete unpublished grouped candidate with independent input evidence."""
    from api import plan_pricing_code_build as build

    selection, identity, inputs, scope = await _resolved_inputs(session, params, calibration_by_field)
    dataset_id = await _register_stage(session, calibration_by_field)
    oracle = CodeInputOracle(session, identity, inputs)
    limits = await oracle.prepare()
    ownership = await native.precreate_model_family_stage(session, SPEC, dataset_id)
    oracle.schema = ownership.schema_name
    work = build._CodeBuildState(
        build.hashlib.sha256(), audit=context["audit"], phase_timings=context["phase_timings"], calibration=oracle
    )
    await build._create_stage_tables(session)
    await build._prepare_code_candidate(
        session, ownership, identity, inputs, (params["code_system"], params["code"]), work, limits
    )
    proof = await oracle.verify(context["audit"], ownership)
    after, _ = await build.resolve_inputs(session, selection, (params["code_system"], params["code"]), scope=scope)
    if after != identity:
        raise ValueError("code calibration input identity changed during preparation")
    return {
        "complete": True,
        "before_identity_digest": identity_digest(identity),
        "after_identity_digest": identity_digest(after),
        "scope": identity["scope"],
        "oracle": proof,
        "candidate_bytes": await require_budget(session, ownership.schema_name),
    }


async def calibrate(params, context):
    """Always roll back native data; only audited status and evidence leave the job."""
    validate_params(params)
    if params.get("operation") != "calibrate":
        raise ValueError("code calibration requires an exact operator operation")
    calibration_by_field = {"complete": False, "cleanup_verified": False}
    context["audit"]["calibration"] = calibration_by_field
    receipt = None
    try:
        async with asyncio.timeout(360):
            try:
                async with db.transaction() as session:
                    receipt = await _prepare(session, params, context, calibration_by_field)
                    raise _RollbackCalibration(receipt)
            except _RollbackCalibration as finished:
                receipt = finished.receipt
    finally:
        try:
            async with asyncio.timeout(20):
                await _cleanup_proof(calibration_by_field)
        finally:
            # Internal custody coordinates never become a reusable authority envelope.
            for name in ("publication_before", "identity", "schema", "temporary_namespace_oid"):
                calibration_by_field.pop(name, None)
    calibration_by_field.update(complete=True, cleanup_verified=True)
    return {"calibration": {**receipt, "rolled_back": True, "cleanup_verified": True}}
