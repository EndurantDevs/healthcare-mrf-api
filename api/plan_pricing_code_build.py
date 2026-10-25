# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare one complete procedure with the existing occurrence and aggregate codecs."""

import hashlib
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from uuid import uuid4

from sqlalchemy import text

from api.plan_pricing_code_contract import (
    SPEC,
    capability_identity,
    identity_digest,
    scope_selectors,
    sealed_read_digest,
    source_coverage,
    validate_params,
)
from api.plan_pricing_code_storage import locate, lock_identity, receipt, seal
from api.plan_pricing_projection_contract import (
    canonical_json,
    lock_provider_generation,
    projection_code_identity,
    provider_signature,
)
from api.plan_pricing_projection_source import (
    MAX_PROVIDER_ROWS_PER_BATCH,
    BindingProjection,
    binding_source,
    projection_provider_rows_for_npis,
)
from api.plan_pricing_projection_v3_aggregate import _aggregate_pack_row, _aggregate_records
from api.plan_pricing_projection_v3_code import (
    MAX_CODE_OCCURRENCES,
    _binding_code_rows,
    _code_occurrences,
    _insert_code_occurrences,
    _preflight_binding_price_memberships,
)
from api.plan_pricing_projection_v3_price import MAX_CODE_STAGED_PRICE_ATOMS, _stage_binding_price_rates
from api.plan_pricing_projection_v3_provider import MAX_PROJECTION_PROVIDER_MEMBERSHIPS, _create_stage_tables
from api.plan_pricing_projection_v3_provider_cells import _materialize_provider_cells
from api.plan_pricing_projection_v3_types import _BuildState
from api.plan_pricing_projection_v3_work import _record_code_work, _stage_code_work, _WorkLimits
from api.plan_pricing_projection_v4_occurrence import rate_occurrence_rows
from api.ptg2_code_scope import load_sealed_code_rows
from api.ptg2_db_sidecars import _preflight_price_membership_aliases_from_db
from api.ptg_membership_source import source_identity
from api.ptg_membership_storage import locate as locate_membership
from api.ptg_membership_storage import receipt as membership_receipt
from api.ptg_provider_membership_build import _reader_principals
from db.connection import db
from db.models.plan_pricing_code_projection import (
    PlanPricingCodeAggregatePack,
    PlanPricingCodeManifest,
    PlanPricingCodeOccurrence,
)
from process import reference_family_archive as native
from process.live_progress import enqueue_live_progress
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

MAX_CODE_MEMBER_CELL_WORK_ROWS: int | None = 32_768
MAX_CODE_FACTOR_JOIN_WORK_ROWS: int | None = 16_384
CODE_PROVIDER_READ_BATCH_SIZE = 100


@dataclass
class _CodeBuildState(_BuildState):
    audit: dict | None = None
    phase_timings: dict | None = None
    calibration: object | None = None


@contextmanager
def _audit_phase(audit, phase_timings, name):
    """Retain the last entered phase and elapsed work even when it is interrupted."""
    if audit is not None:
        audit["phase"] = name
    started = time.monotonic()
    try:
        yield
    finally:
        if phase_timings is not None:
            phase_timings[name] = phase_timings.get(name, 0.0) + time.monotonic() - started


async def _code_provider_rows_for_npis(session, npis):
    """Bound each witness query without weakening the complete pending-batch limit."""
    providers_by_npi = {}
    row_count = 0
    for start in range(0, len(npis), CODE_PROVIDER_READ_BATCH_SIZE):
        batch = npis[start : start + CODE_PROVIDER_READ_BATCH_SIZE]
        rows = await projection_provider_rows_for_npis(session, batch)
        row_count += sum(map(len, rows.values()))
        if set(rows) - set(batch) or row_count > MAX_PROVIDER_ROWS_PER_BATCH:
            raise ValueError("code pricing provider-row bound exceeded")
        providers_by_npi.update(rows)
    return providers_by_npi


def _member_cell_limit():
    """Require native resource qualification for this capability's actual join units."""
    if type(MAX_CODE_MEMBER_CELL_WORK_ROWS) is not int or MAX_CODE_MEMBER_CELL_WORK_ROWS <= 0:
        raise ValueError("code pricing member-cell work bound is not calibrated")
    return MAX_CODE_MEMBER_CELL_WORK_ROWS


def _factor_join_limit():
    if type(MAX_CODE_FACTOR_JOIN_WORK_ROWS) is not int or MAX_CODE_FACTOR_JOIN_WORK_ROWS <= 0:
        raise ValueError("code pricing factor-join work bound is not calibrated")
    return MAX_CODE_FACTOR_JOIN_WORK_ROWS


class CodeProjectionMembershipNotReady(ValueError):
    """A missing preparation dependency is different from corrupt ready storage."""


async def resolve_inputs(session, selection, code_identity, *, scope=None):
    """Authenticate every logical binding and its complete protected code membership."""
    inputs, source_coverages = [], []
    bindings = tuple(binding for binding in selection.bindings if binding.role == "in_network")
    if not 1 <= len(bindings) <= 16:
        raise ValueError("code projection binding count exceeds its bound")
    ordinal_by_read_identity = {}
    for binding in bindings:
        params = asdict(binding)
        params["ordinal"] = binding.binding_ordinal
        params["market_type"] = binding.plan_market_type
        tables, read_identity = await binding_source(session, params)
        _, _, identity = await source_identity(
            session, binding.source_key, binding.snapshot_id, *code_identity, tables=tables
        )
        found = await locate_membership(session, identity)
        if found is None:
            raise CodeProjectionMembershipNotReady(
                "code projection requires ready complete source/procedure membership"
            )
        code_rows = await load_sealed_code_rows(
            session,
            tables,
            {
                "plan_id": binding.plan_id,
                "plan_market_type": binding.plan_market_type,
                "code_system": code_identity[0],
                "code": code_identity[1],
            },
        )
        if (
            len(code_rows) > MAX_CODE_OCCURRENCES
            or sum(int(code_row["rate_count"]) for code_row in code_rows) > MAX_CODE_OCCURRENCES
        ):
            raise ValueError("code projection occurrence count exceeds its bound")
        representative = ordinal_by_read_identity.setdefault(read_identity, binding.binding_ordinal)
        source_coverages.append(
            source_coverage(
                binding.binding_ordinal,
                identity,
                code_rows,
                membership_receipt(found),
                materialized_binding_ordinal=representative,
                read_identity_digest=sealed_read_digest(read_identity),
            )
        )
        if representative == binding.binding_ordinal:
            inputs.append((BindingProjection(params, tables, {code_identity: code_rows}, len(code_rows)), found))
    if (
        sum(
            code_row["rate_count"]
            for binding, _found in inputs
            for code_row in binding.code_rows_by_identity[code_identity]
        )
        > MAX_CODE_OCCURRENCES
    ):
        raise ValueError("code projection complete occurrence count exceeds its bound")
    identity = await _resolve_capability_identity(session, selection, code_identity, source_coverages, scope)
    return identity, inputs


async def _resolve_capability_identity(session, selection, code_identity, source_coverages, scope):
    """Bind fresh provider identity to the complete server-resolved centroid scope."""
    scope_by_field = scope
    if scope is not None:
        from api.plan_pricing_projection_read import geo_cells

        cells = await geo_cells(session, scope, result_type="rate_aggregates")
        scope_by_field = {**scope, "geo_cells": sorted(set(cells))}
    signature = await provider_signature(session)
    return capability_identity(selection, code_identity, signature, source_coverages, scope=scope_by_field)


async def _copy(session, ownership, model, rows):
    for start in range(0, len(rows), 1000):
        await native.native_copy_record_batch(
            session,
            model,
            schema_name=ownership.schema_name,
            table_name=model.__tablename__,
            columns=tuple(column.name for column in model.__table__.columns),
            records=rows[start : start + 1000],
        )


async def _bounded_factor_rows(session, schema, parameters_by_name, factor_remaining):
    """Validate complete pre-deduplication work before any membership insertion."""
    factor_rows = int(
        await session.scalar(
            text(f"""WITH second_counts AS MATERIALIZED (
        SELECT second.owner_key AS factor,count(*)::numeric AS sets FROM {schema}.ptg_membership_second second
         WHERE EXISTS(SELECT 1 FROM {schema}.ptg_membership_code code WHERE code.provider_set_key=second.member_key AND code.code_key=ANY(CAST(:code_keys AS bigint[])))
         GROUP BY second.owner_key)
        SELECT coalesce(sum(selected.sets),0)
          FROM {schema}.ptg_membership_first first JOIN second_counts selected ON selected.factor=first.member_key"""),
            parameters_by_name,
        )
    )
    if factor_rows > factor_remaining:
        raise ValueError("code projection pre-deduplication factor join exceeds its work bound")
    return factor_rows


async def _require_complete_memberships(session, serving, binding, schema, parameters_by_name):
    """Reject any incomplete selected set, including declared empty memberships."""
    # A complete factor transpose must reproduce every declared selected set,
    # including zero-member sets; a row limit can never become partial success.
    invalid = await session.scalar(
        text(f"""WITH expected AS MATERIALIZED (
        SELECT provider_set.provider_set_key,provider_set.provider_count
          FROM {serving._shared_v3_provider_set_table(binding.serving_tables)} provider_set
         WHERE snapshot_key=:snapshot AND EXISTS (
            SELECT 1 FROM {schema}.ptg_membership_code code
             WHERE code.provider_set_key=provider_set.provider_set_key
               AND code.code_key=ANY(CAST(:code_keys AS bigint[])))
        ), actual AS MATERIALIZED (
        SELECT provider_set_key,count(*)::bigint AS provider_count
          FROM plan_pricing_provider_member_stage WHERE binding_ordinal=:ordinal GROUP BY provider_set_key)
        SELECT EXISTS(SELECT 1 FROM expected FULL JOIN actual USING(provider_set_key)
            WHERE expected.provider_set_key IS NULL OR expected.provider_count<0
               OR expected.provider_count IS DISTINCT FROM coalesce(actual.provider_count,0))"""),
        parameters_by_name,
    )
    if invalid:
        raise PTG2ManifestArtifactError("code projection membership is incomplete")


async def _stage_memberships(session, binding, found, code_rows, remaining, factor_remaining):
    """Use the complete factor join in SQL; never materialize a whole set in Python."""
    from api import ptg2_serving as serving

    schema = native._quoted(found[0].schema_name)
    parameters_by_name = {
        "ordinal": binding.binding["ordinal"],
        "snapshot": serving._required_shared_snapshot_key(binding.serving_tables),
        "code_keys": [int(code_row["code_key"]) for code_row in code_rows],
        "limit": remaining + 1,
    }
    factor_rows = await _bounded_factor_rows(session, schema, parameters_by_name, factor_remaining)
    await session.execute(
        text(f"""INSERT INTO plan_pricing_provider_member_stage (binding_ordinal,provider_set_key,npi)
        SELECT :ordinal, second.member_key, dictionary.npi
          FROM {schema}.ptg_membership_second second
          JOIN {schema}.ptg_membership_first first ON first.member_key=second.owner_key
          JOIN {serving._ptg2_npi_scope_table(binding.serving_tables)} dictionary
            ON dictionary.snapshot_key=:snapshot AND dictionary.npi_key=first.owner_key
         WHERE EXISTS (SELECT 1 FROM {schema}.ptg_membership_code code
                        WHERE code.provider_set_key=second.member_key
                          AND code.code_key=ANY(CAST(:code_keys AS bigint[])))
         GROUP BY second.member_key,dictionary.npi LIMIT :limit"""),
        parameters_by_name,
    )
    count = int(
        await session.scalar(
            text("SELECT count(*) FROM plan_pricing_provider_member_stage WHERE binding_ordinal=:ordinal"),
            parameters_by_name,
        )
    )
    if count > remaining:
        raise ValueError("code projection provider membership exceeds its bounded capability")
    await _require_complete_memberships(session, serving, binding, schema, parameters_by_name)
    await session.execute(
        text(f"""INSERT INTO plan_pricing_provider_set_stage (binding_ordinal,provider_set_key,provider_set_id,membership_count)
        SELECT :ordinal,p.provider_set_key,encode(p.provider_set_global_id_128,'hex'),p.provider_count
          FROM {serving._shared_v3_provider_set_table(binding.serving_tables)} p
         WHERE p.snapshot_key=:snapshot AND EXISTS(SELECT 1 FROM {schema}.ptg_membership_code code
              WHERE code.provider_set_key=p.provider_set_key AND code.code_key=ANY(CAST(:code_keys AS bigint[])))"""),
        parameters_by_name,
    )
    return count, factor_rows


async def _store_occurrences(session, ownership, binding, serving_rows, retained_prices):
    """Retain the same complete occurrence order and native row accounting."""
    from api import ptg2_serving as serving

    await _insert_code_occurrences(
        session,
        binding.binding["ordinal"],
        _code_occurrences(
            serving,
            [
                occurrence_row
                for occurrence_row in serving_rows
                if serving._ptg2_manifest_id(occurrence_row["price_set_global_id_128"]) in retained_prices
            ],
        ),
    )
    occurrences = list(rate_occurrence_rows(serving, binding.binding["ordinal"], serving_rows, retained_prices))
    await _copy(
        session,
        ownership,
        PlanPricingCodeOccurrence,
        [
            (
                occurrence_row["binding_ordinal"],
                ordinal,
                occurrence_row["provider_set_key"],
                occurrence_row["provider_set_ref"],
                occurrence_row["price_key"],
                occurrence_row["price_set_ref"],
                occurrence_row["rate_pack_ref"],
                occurrence_row["source_artifact_key"],
                occurrence_row["provider_count"],
                occurrence_row["group_fragment"],
                occurrence_row["occurrence_multiplicity"],
            )
            for ordinal, occurrence_row in enumerate(occurrences)
        ],
    )
    return len(occurrences)


async def _stage_inputs(
    session,
    ownership,
    candidate_id,
    inputs,
    code_identity,
    state,
    *,
    scope=None,
    zip_limits=None,
):
    """Complete scoped membership admission before copying rate occurrences."""
    remaining_atoms = MAX_CODE_STAGED_PRICE_ATOMS
    work_by_name = {"factor": 0}
    if scope is not None:
        from api import plan_pricing_code_zip_build as scoped

        scoped_work = await scoped.stage_inputs(session, candidate_id, scope, inputs, code_identity, state, zip_limits)
    for binding, found in inputs:
        code_rows = binding.code_rows_by_identity[code_identity]
        if scope is None:
            membership_count, factor_count = await _stage_memberships(
                session,
                binding,
                found,
                code_rows,
                MAX_PROJECTION_PROVIDER_MEMBERSHIPS - state.provider_membership_count,
                _factor_join_limit() - work_by_name["factor"],
            )
            state.provider_membership_count += membership_count
            work_by_name["factor"] += factor_count
        serving_rows, price_keys = await _binding_code_rows(session, binding, code_rows)
        span = await _preflight_binding_price_memberships(
            session, state, binding, price_keys, _preflight_price_membership_aliases_from_db
        )
        retained, atoms = await _stage_binding_price_rates(
            session, binding, price_keys, maximum_atom_count=remaining_atoms, block_span=span
        )
        remaining_atoms -= atoms
        occurrence_count = await _store_occurrences(session, ownership, binding, serving_rows, retained)
        state.rate_occurrence_count += occurrence_count
    if scope is not None:
        return scoped_work
    await session.execute(
        text(
            "INSERT INTO plan_pricing_provider_npi_pending_stage SELECT DISTINCT npi FROM plan_pricing_provider_member_stage"
        )
    )
    await _materialize_provider_cells(session, candidate_id, state, provider_rows_for_npis=_code_provider_rows_for_npis)
    return {"factor_join_work_rows": work_by_name["factor"]}


async def _store_provider_inputs(session, ownership, candidate_id):
    schema = native._quoted(ownership.schema_name)
    await session.execute(
        text(
            f"INSERT INTO {schema}.plan_pricing_code_membership SELECT binding_ordinal,provider_set_key,npi FROM plan_pricing_provider_member_stage"
        )
    )
    await session.execute(
        text(
            f"INSERT INTO {schema}.plan_pricing_code_provider_cell SELECT npi,geo_cell,entity_type_code,taxonomy_codes,fragment FROM plan_pricing_provider_cell_stage WHERE projection_id=:id"
        ),
        {"id": candidate_id},
    )
    await session.execute(
        text(
            f"INSERT INTO {schema}.plan_pricing_code_provider_state SELECT upper(convert_from(state_fragment,'UTF8')::jsonb->'provider'->>'state'),npi,state_fragment FROM plan_pricing_provider_cell_stage WHERE projection_id=:id AND state_fragment IS NOT NULL"
        ),
        {"id": candidate_id},
    )


async def _store_aggregates(session, ownership, candidate_id, code_identity, state, *, zip_limits=None):
    from api.plan_pricing_aggregate_pack import AggregateCodeIdentity

    limit = MAX_PROJECTION_PROVIDER_MEMBERSHIPS if zip_limits is None else zip_limits["member_cell"]
    cell_limit = _member_cell_limit() if zip_limits is None else zip_limits["member_cell"]
    work = await _stage_code_work(session, candidate_id, code_identity, limit, cell_limit)
    _record_code_work(state, work, _WorkLimits(limit, limit, cell_limit, cell_limit))
    aggregate_records = await _aggregate_records(session, candidate_id, code_identity, state)
    prefixes = sorted({aggregate_record.zip5[:2] for aggregate_record in aggregate_records})
    for prefix in prefixes:
        aggregate_pack = _aggregate_pack_row(
            candidate_id,
            AggregateCodeIdentity(*code_identity),
            prefix,
            [aggregate_record for aggregate_record in aggregate_records if aggregate_record.zip5.startswith(prefix)],
        )
        await _copy(
            session,
            ownership,
            PlanPricingCodeAggregatePack,
            [
                (
                    prefix,
                    aggregate_pack["entry_count"],
                    aggregate_pack["raw_byte_count"],
                    aggregate_pack["stored_byte_count"],
                    aggregate_pack["logical_digest"],
                    hashlib.sha256(aggregate_pack["payload"]).digest(),
                    aggregate_pack["payload"],
                )
            ],
        )
    schema = native._quoted(ownership.schema_name)
    await session.execute(
        text(f"""INSERT INTO {schema}.plan_pricing_code_rate_profile
        SELECT rate.binding_ordinal,rate.provider_set_key,membership.membership_count,
               min(rate.negotiated_rate),max(rate.negotiated_rate),sum(rate.multiplicity)::bigint,
               array_agg(rate.negotiated_rate ORDER BY rate.negotiated_rate),
               array_agg(rate.multiplicity ORDER BY rate.negotiated_rate)
          FROM plan_pricing_rate_frequency_stage rate
          JOIN plan_pricing_provider_set_stage membership USING(binding_ordinal,provider_set_key)
         GROUP BY rate.binding_ordinal,rate.provider_set_key,membership.membership_count""")
    )
    return {
        "aggregate_entry_count": len(aggregate_records),
        "aggregate_pack_count": len(prefixes),
        "work": asdict(work),
    }


async def _content_digest(session, ownership):
    """Hash every completed stored row in native primary-key order with bounded retention."""
    hasher = hashlib.sha256()
    schema = native._quoted(ownership.schema_name)
    for model in SPEC.model_types:
        if model is PlanPricingCodeManifest:
            continue
        name = native._quoted(model.__tablename__)
        keys = tuple(column.name for column in model.__table__.primary_key.columns)
        order = ",".join(native._quoted(key) for key in keys)
        batch_size = 8 if model is PlanPricingCodeAggregatePack else 1000
        last_keys = None
        while True:
            predicate = (
                ""
                if last_keys is None
                else "WHERE (" + order + ") > (" + ",".join(":last_" + str(index) for index in range(len(keys))) + ")"
            )
            parameters_by_name = (
                {}
                if last_keys is None
                else {"last_" + str(index): key_value for index, key_value in enumerate(last_keys)}
            )
            stored_rows = (
                (
                    await session.execute(
                        text(
                            f"SELECT row_to_json(stored)::text AS row,{order} FROM {schema}.{name} stored {predicate} ORDER BY {order} LIMIT {batch_size}"
                        ),
                        parameters_by_name,
                    )
                )
                .mappings()
                .all()
            )
            if not stored_rows:
                break
            for stored_row in stored_rows:
                encoded_row = stored_row["row"].encode("utf8")
                hasher.update(model.__tablename__.encode() + b"\0" + len(encoded_row).to_bytes(8, "big") + encoded_row)
            last_keys = tuple(stored_rows[-1][key] for key in keys)
    return hasher.hexdigest()


async def _complete_candidate(session, ownership, identity, state, work_counts, aggregate_counts):
    await native.complete_model_family_stage(session, SPEC, ownership)
    for name in SPEC.table_names:
        await session.execute(text(f"ANALYZE {native._quoted(ownership.schema_name)}.{native._quoted(name)}"))
    counts_by_name = {
        **work_counts,
        "provider_membership_count": state.provider_membership_count,
        "provider_cell_count": state.provider_cell_count,
        "provider_state_count": state.provider_state_count,
        "provider_fragment_byte_count": state.provider_fragment_byte_count,
        "rate_occurrence_count": state.rate_occurrence_count,
        **aggregate_counts,
    }
    content_digest = await _content_digest(session, ownership)
    await _copy(
        session,
        ownership,
        PlanPricingCodeManifest,
        [(1, canonical_json(identity), content_digest, canonical_json(counts_by_name))],
    )


async def _prepare_code_candidate(session, ownership, identity, inputs, code_identity, state, zip_limits):
    candidate_id = identity_digest(identity)
    with _audit_phase(state.audit, state.phase_timings, "code_pricing.inputs"):
        work_counts = await _stage_inputs(
            session,
            ownership,
            candidate_id,
            inputs,
            code_identity,
            state,
            scope=identity.get("scope"),
            zip_limits=zip_limits,
        )
        await _store_provider_inputs(session, ownership, candidate_id)
    with _audit_phase(state.audit, state.phase_timings, "code_pricing.aggregates"):
        aggregate_counts = await _store_aggregates(
            session,
            ownership,
            candidate_id,
            code_identity,
            state,
            zip_limits=zip_limits,
        )
    with _audit_phase(state.audit, state.phase_timings, "code_pricing.candidate"):
        await _complete_candidate(session, ownership, identity, state, work_counts, aggregate_counts)


async def build_in_session(
    session,
    *,
    selection,
    code_system,
    code,
    expected_identity_digest,
    scope=None,
    audit=None,
    phase_timings=None,
):
    """Prepare one exact candidate with transient worker diagnostics."""
    code_identity = projection_code_identity(code_system, code)
    if code_identity != (code_system, code):
        raise ValueError("code projection requires a canonical procedure identity")
    zip_limits = None
    if scope is None:
        _member_cell_limit()
        _factor_join_limit()
    else:
        from api.plan_pricing_code_zip_build import work_limits

        zip_limits = work_limits()
    options = {} if scope is None else {"scope": scope}
    with _audit_phase(audit, phase_timings, "code_pricing.resolve"):
        identity, inputs = await resolve_inputs(session, selection, code_identity, **options)
    if identity_digest(identity) != expected_identity_digest:
        raise ValueError("code pricing expected source or provider identity changed before preparation")
    await lock_identity(session, identity)
    existing = await locate(session, identity, allow_stale_provider=True)
    if existing is not None:
        return receipt(existing)
    await native.protected_publisher_owner(session)
    ownership = await native.precreate_model_family_stage(session, SPEC, uuid4())
    state = _CodeBuildState(hashlib.sha256(), audit=audit, phase_timings=phase_timings)
    await _create_stage_tables(session)
    enqueue_live_progress(
        phase="code pricing inputs",
        message="Preparing complete procedure membership, prices and frozen provider witnesses",
    )
    await _prepare_code_candidate(session, ownership, identity, inputs, code_identity, state, zip_limits)
    after, _ = await resolve_inputs(session, selection, code_identity, **options)
    if after != identity:
        raise PTG2ManifestArtifactError("code pricing source or provider generation changed during preparation")
    principals = set()
    for binding, _found in inputs:
        principals.update(await _reader_principals(session, binding.serving_tables))
    with _audit_phase(audit, phase_timings, "code_pricing.seal"):
        await seal(session, ownership, identity, reader_principals=tuple(sorted(principals)))
    return receipt(await locate(session, identity))


async def build_plan_pricing_code_projection(
    *,
    plan_release_id,
    serving_revision_id,
    code_system,
    code,
    expected_identity_digest,
    scope_kind=None,
    zip5=None,
    zip_radius_miles=None,
):
    """Retain the eight-selector entry point for direct callers and queued jobs."""
    params_by_name = dict(
        plan_release_id=plan_release_id,
        serving_revision_id=serving_revision_id,
        code_system=code_system,
        code=code,
        expected_identity_digest=expected_identity_digest,
    )
    scope_by_field = {"scope_kind": scope_kind, "zip5": zip5, "zip_radius_miles": zip_radius_miles}
    params_by_name.update(
        {key: parameter_value for key, parameter_value in scope_by_field.items() if parameter_value is not None}
    )
    return await _build_code_projection(params_by_name, {})


async def control_start(ctx, task):
    """Use trusted per-run context without admitting unrecognized worker selectors."""
    params_by_name = dict(task)
    params_by_name.pop("run_id", None)
    params_by_name.pop("test_mode", None)
    prepared_by_field = await _build_code_projection(params_by_name, ctx)
    return {
        **prepared_by_field,
        "audit": ctx["context"]["audit"],
        "phase_timings": ctx["context"]["phase_timings"],
    }


async def _build_code_projection(params_by_name, control_context):
    from api.plan_release_serving_resolution import (
        PLAN_RELEASE_RESOLUTION_READY,
        resolve_plan_release_serving_resolution,
    )

    context_by_field = control_context.setdefault("context", {})
    audit_by_name = context_by_field["audit"] = {}
    timings_by_phase = context_by_field["phase_timings"] = {}
    validate_params(params_by_name)
    if params_by_name.get("operation") == "calibrate":
        from api.plan_pricing_code_calibration import calibrate

        return await calibrate(params_by_name, context_by_field)
    async with db.transaction() as session:
        await lock_provider_generation(session)
        resolution = await resolve_plan_release_serving_resolution(session, params_by_name["plan_release_id"])
        if (
            resolution.state != PLAN_RELEASE_RESOLUTION_READY
            or resolution.selection is None
            or resolution.selection.serving_revision_id != params_by_name["serving_revision_id"]
        ):
            raise ValueError("code pricing release is not the exact current published revision")
        return await build_in_session(
            session,
            selection=resolution.selection,
            code_system=params_by_name["code_system"],
            code=params_by_name["code"],
            expected_identity_digest=params_by_name["expected_identity_digest"],
            scope=scope_selectors(params_by_name),
            audit=audit_by_name,
            phase_timings=timings_by_phase,
        )
