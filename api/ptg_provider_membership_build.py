# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain the source before building one complete isolated source/code family."""

import asyncio
import hashlib
import struct
from contextlib import asynccontextmanager
from uuid import uuid4

from sqlalchemy import text

from api.ptg2_db_sidecars import ForwardReadBudget
from api.ptg2_v4_graph import load_v4_relation_manifest, lookup_v4_relation_members
from api.ptg_membership_contract import (
    MAX_BUILD_BYTES,
    MAX_BUILD_EDGES,
    MAX_BUILD_OWNERS,
    MAX_CODE_OCCURRENCES,
    MAX_FACTOR_BATCH_MEMBERS,
    MAX_MAP_BYTES,
    SPEC,
    digest,
    encoded,
    validate_params,
)
from api.ptg_membership_source import source_identity
from api.ptg_membership_storage import (
    locate,
    lock_identity,
    lock_retained_source,
    prepare_source_retention,
    receipt,
    release_unattached_source_retention,
    seal,
)
from db.connection import db
from db.models.ptg_membership_projection import (
    PTGMembershipCode,
    PTGMembershipCodeCoverage,
    PTGMembershipFirst,
    PTGMembershipManifest,
    PTGMembershipSecond,
)
from process import reference_family_archive as native
from process.live_progress import enqueue_live_progress
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_v4_snapshot_maps import summarize_persisted_v4_snapshot_maps


async def _copy(session, ownership, model, rows):
    for start in range(0, len(rows), 5000):
        await native.native_copy_record_batch(
            session,
            model,
            schema_name=ownership.schema_name,
            table_name=model.__tablename__,
            columns=tuple(column.name for column in model.__table__.columns),
            records=rows[start : start + 5000],
        )


async def _authenticate_root(session, tables, root):
    from api import ptg2_serving as serving

    schema = serving._payload_schema(tables)
    expected = (
        (
            await session.execute(
                text(
                    f"SELECT object_kind_count,map_pack_count,coordinate_count,entry_count,logical_byte_count,stored_map_byte_count "
                    f'FROM "{schema}".ptg2_v4_snapshot_map_root WHERE snapshot_key=:key'
                ),
                {"key": root.snapshot_key},
            )
        )
        .mappings()
        .one()
    )
    if not 0 < expected["stored_map_byte_count"] <= MAX_MAP_BYTES or expected["coordinate_count"] > MAX_BUILD_EDGES:
        raise ValueError("membership source map exceeds the bounded build contract")
    actual = await summarize_persisted_v4_snapshot_maps(
        session,
        schema_name=schema,
        snapshot_key=root.snapshot_key,
        batch_rows=32,
        cancel_gc_candidates=False,
    )
    if actual.map_digest != root.map_digest or any(getattr(actual, name) != value for name, value in expected.items()):
        raise PTG2ManifestArtifactError("membership source CAS mapping root differs")


async def _copy_relation(session, tables, root, relation, ownership, model, hasher, *, edge_allowance):
    from api import ptg2_serving as serving

    schema = serving._payload_schema(tables)
    manifest = await load_v4_relation_manifest(
        session, snapshot_key=root.snapshot_key, relation=relation, schema_name=schema
    )
    if manifest.owner_count > MAX_BUILD_OWNERS or manifest.logical_member_count > edge_allowance:
        raise ValueError("membership factor relation exceeds the bounded build contract")
    enqueue_live_progress(phase=f"membership {relation}", done=0, total=manifest.logical_member_count, unit="edges")
    hasher.update(relation.encode("ascii") + b"\0")
    count = 0
    reported = 0
    for start in range(manifest.owner_base, manifest.owner_base + manifest.owner_count, 256):
        owners = tuple(range(start, min(start + 256, manifest.owner_base + manifest.owner_count)))
        members = await lookup_v4_relation_members(
            session,
            snapshot_key=root.snapshot_key,
            relation=relation,
            owner_keys=owners,
            schema_name=schema,
            max_members=min(MAX_FACTOR_BATCH_MEMBERS, edge_allowance - count),
        )
        if set(members) != set(owners):
            raise PTG2ManifestArtifactError("membership factor owner coverage is incomplete")
        edge_rows = []
        for owner in owners:
            members_for_owner = members[owner]
            if tuple(members_for_owner) != tuple(sorted(set(members_for_owner))) or any(
                not 0 <= member_key <= 0xFFFFFFFF for member_key in members_for_owner
            ):
                raise PTG2ManifestArtifactError("membership factor vector is noncanonical")
            for member in members_for_owner:
                edge_rows.append((owner, member))
                hasher.update(struct.pack(">II", owner, member))
                if len(edge_rows) == 5000:
                    await _copy(session, ownership, model, edge_rows)
                    edge_rows = []
            count += len(members_for_owner)
        await _copy(session, ownership, model, edge_rows)
        if count - reported >= 250_000 or count == manifest.logical_member_count:
            enqueue_live_progress(
                phase=f"membership {relation}", done=count, total=manifest.logical_member_count, unit="edges"
            )
            reported = count
    if count != manifest.logical_member_count:
        raise PTG2ManifestArtifactError("membership factor member count differs from sealed manifest")
    return {
        "relation": relation,
        "owner_base": manifest.owner_base,
        "owner_count": manifest.owner_count,
        "edge_count": count,
    }


async def _physical_code_rows(session, tables, code_system, code):
    """Read every compatible physical code key, independent of one logical plan label."""
    from api import ptg2_serving as serving

    predicates = ["code_metadata.snapshot_key=:snapshot_key"]
    params_by_name = {"snapshot_key": serving._required_shared_snapshot_key(tables)}
    serving._append_reported_code_system_filter(
        predicates,
        params_by_name,
        column="code_metadata.reported_code_system",
        code_system=code_system,
    )
    serving._append_reported_code_value_filter(
        predicates,
        params_by_name,
        column="code_metadata.reported_code",
        param_name="reported_code",
        values=serving._ptg2_reported_code_lookup_values(code_system, code),
    )
    code_rows = (
        (
            await session.execute(
                text(
                    f"SELECT code_metadata.code_key,code_metadata.rate_count FROM {serving._shared_v3_code_table(tables)} code_metadata "
                    f"WHERE {' AND '.join(predicates)} ORDER BY code_metadata.code_key LIMIT :row_limit"
                ),
                params_by_name | {"row_limit": MAX_CODE_OCCURRENCES + 1},
            )
        )
        .mappings()
        .all()
    )
    if (
        len(code_rows) > MAX_CODE_OCCURRENCES
        or sum(int(code_row["rate_count"]) for code_row in code_rows) > MAX_CODE_OCCURRENCES
    ):
        raise ValueError("membership requested code exceeds the bounded build contract")
    return code_rows


async def _copy_code(session, tables, identity, ownership, hasher, *, byte_allowance):
    from api import ptg2_serving as serving

    rows = await _physical_code_rows(session, tables, identity["code_system"], identity["code"])
    budget = ForwardReadBudget(
        maximum_fragments=MAX_CODE_OCCURRENCES,
        maximum_raw_payload_bytes=byte_allowance,
        maximum_row_capacity=MAX_CODE_OCCURRENCES + 1,
    )
    coverage_rows = []
    for row in rows:
        entries = await serving._shared_forward_entries_for_code_rows(session, tables, [row], scan_budget=budget)
        if len(entries) != int(row["rate_count"]):
            raise PTG2ManifestArtifactError("membership requested code occurrence coverage is incomplete")
        sets = sorted({int(entry.provider_set_key) for entry in entries})
        set_digest = hashlib.sha256(b"".join(struct.pack(">I", key) for key in sets)).hexdigest()
        coverage_by_field = {
            "code_key": int(row["code_key"]),
            "occurrence_count": len(entries),
            "set_count": len(sets),
            "set_digest": set_digest,
        }
        coverage_rows.append(coverage_by_field)
        hasher.update(encoded(coverage_by_field).encode("ascii"))
        await _copy(session, ownership, PTGMembershipCode, [(coverage_by_field["code_key"], key) for key in sets])
        await _copy(session, ownership, PTGMembershipCodeCoverage, [tuple(coverage_by_field.values())])
    return coverage_rows


async def _validate_candidate(session, ownership, factors, coverage):
    schema = native._quoted(ownership.schema_name)
    for model, expected in (
        (PTGMembershipFirst, factors[0]["edge_count"]),
        (PTGMembershipSecond, factors[1]["edge_count"]),
        (PTGMembershipCode, sum(row["set_count"] for row in coverage)),
        (PTGMembershipCodeCoverage, len(coverage)),
    ):
        count = await session.scalar(text(f"SELECT count(*) FROM {schema}.{native._quoted(model.__tablename__)}"))
        if count != expected:
            raise PTG2ManifestArtifactError("membership native COPY accounting differs")
    lower = factors[1]["owner_base"]
    upper = lower + factors[1]["owner_count"]
    invalid = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {schema}.ptg_membership_first WHERE member_key<:lower OR member_key>=:upper)"
        ),
        {"lower": lower, "upper": upper},
    )
    if invalid:
        raise PTG2ManifestArtifactError("membership first factor escapes complete second owner domain")
    invalid = await session.scalar(
        text(
            f"SELECT EXISTS(SELECT 1 FROM {schema}.ptg_membership_code code "
            f"LEFT JOIN {schema}.ptg_membership_code_coverage coverage USING(code_key) WHERE coverage.code_key IS NULL)"
        )
    )
    if invalid:
        raise PTG2ManifestArtifactError("membership code rows escape explicit coverage")


async def _reader_principals(session, tables):
    from api import ptg2_serving as serving

    relation = (
        tables.physical_binding.relation("ptg2_v4_npi_scope")
        if tables.physical_binding
        else (native._quoted(serving._payload_schema(tables)) + ".ptg2_v4_npi_scope")
    )
    rows = (
        (
            await session.execute(
                text(
                    "SELECT DISTINCT CASE WHEN acl.grantee=0 THEN 'PUBLIC' ELSE quote_ident(role.rolname) END "
                    "FROM pg_class c,LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) acl "
                    "LEFT JOIN pg_roles role ON role.oid=acl.grantee WHERE c.oid=to_regclass(:relation) "
                    "AND acl.privilege_type='SELECT' LIMIT 65"
                ),
                {"relation": relation},
            )
        )
        .scalars()
        .all()
    )
    if len(rows) > 64:
        raise ValueError("membership source read-principal inventory exceeds its bound")
    return rows


async def _copy_factors(session, tables, root, ownership, hasher):
    relations = (
        ("npi_patterns", "pattern_sets")
        if root.representation == "pattern_v1"
        else ("npi_groups_exact", "group_sets_direct")
    )
    factors = []
    edges = 0
    for relation, model in zip(relations, (PTGMembershipFirst, PTGMembershipSecond), strict=True):
        factor = await _copy_relation(
            session,
            tables,
            root,
            relation,
            ownership,
            model,
            hasher,
            edge_allowance=min(MAX_BUILD_EDGES, MAX_BUILD_BYTES // 8) - edges,
        )
        factors.append(factor)
        edges += factor["edge_count"]
    return factors, edges


async def build_in_session(session, *, source_key, snapshot_id, code_system, code, retained_identity):
    """Consume a committed source admission within the candidate transaction."""
    params_by_name = dict(source_key=source_key, snapshot_id=snapshot_id, code_system=code_system, code=code)
    validate_params(params_by_name)
    await native.protected_publisher_owner(session)
    if not isinstance(retained_identity, dict) or any(
        retained_identity.get(name) != selector for name, selector in params_by_name.items()
    ):
        raise PTG2ManifestArtifactError("membership retained source identity differs")
    await lock_identity(session, retained_identity)
    await lock_retained_source(session, retained_identity)
    tables, root, identity = await source_identity(session, **params_by_name)
    if identity != retained_identity:
        raise PTG2ManifestArtifactError("membership retained source identity differs")
    found = await locate(session, identity)
    if found is not None:
        return receipt(found)
    enqueue_live_progress(
        phase="membership source authentication", message="Authenticating the complete source map and CAS closure"
    )
    await _authenticate_root(session, tables, root)
    ownership = await native.precreate_model_family_stage(session, SPEC, uuid4())
    hasher = hashlib.sha256(encoded(identity).encode("ascii"))
    factors, edges = await _copy_factors(session, tables, root, ownership, hasher)
    enqueue_live_progress(
        phase="membership procedure coverage", message="Authenticating complete requested-code occurrence coverage"
    )
    coverage = await _copy_code(
        session, tables, identity, ownership, hasher, byte_allowance=MAX_BUILD_BYTES - edges * 8
    )
    counts_by_name = {"factors": factors, "code_coverage": coverage}
    await _copy(
        session, ownership, PTGMembershipManifest, [(1, encoded(identity), hasher.hexdigest(), encoded(counts_by_name))]
    )
    enqueue_live_progress(
        phase="membership indexes and validation", message="Completing isolated native indexes and set validation"
    )
    await native.complete_model_family_stage(session, SPEC, ownership)
    await _validate_candidate(session, ownership, factors, coverage)
    for table in SPEC.table_names:
        await session.execute(text(f"ANALYZE {native._quoted(ownership.schema_name)}.{native._quoted(table)}"))
    _, _, after = await source_identity(session, **params_by_name, tables=tables)
    if after != identity:
        raise PTG2ManifestArtifactError("membership source changed during candidate build")
    await seal(session, ownership, identity, reader_principals=await _reader_principals(session, tables))
    return receipt(await locate(session, identity))


@asynccontextmanager
async def _publisher_transaction():
    """Own a real commit; Reader and borrowed transactions cannot become publishers."""
    if db.has_reader_session():
        raise RuntimeError("membership publication cannot use a Reader context")
    if db._transaction_binding() is not None:
        raise RuntimeError("membership publication requires independent transactions")
    if db.session_factory is None:
        await db.connect()
    assert db.session_factory is not None
    async with db.session_factory() as session:
        if db.engine is None or session.get_bind().engine is not db.engine.sync_engine:
            raise RuntimeError("membership publication database binding changed")
        async with session.begin():
            yield session


async def _release_failed_retention(identity):
    async with _publisher_transaction() as session:
        await release_unattached_source_retention(session, identity)


async def _drain_failed_retention(identity, error):
    """Drain cancellation cleanup and preserve the receipt if recovery must retry."""
    cleanup = asyncio.create_task(_release_failed_retention(identity))
    try:
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                if cleanup.done():
                    break
        cleanup.result()
    except BaseException as cleanup_error:
        error.add_note("membership source retention requires reconciliation: " + digest(identity))
        error.add_note("retained source identity: " + encoded(identity))
        error.add_note("retention cleanup failed: " + type(cleanup_error).__name__)


async def build_provider_membership_projection(*, source_key, snapshot_id, code_system, code):
    """Commit retention before COPY; a retry adopts ready state or resumes admission."""
    params_by_name = dict(source_key=source_key, snapshot_id=snapshot_id, code_system=code_system, code=code)
    validate_params(params_by_name)
    identity = None
    try:
        async with _publisher_transaction() as session:
            identity = await prepare_source_retention(session, **params_by_name)
        async with _publisher_transaction() as session:
            build_receipt = await build_in_session(session, **params_by_name, retained_identity=identity)
        return build_receipt
    except BaseException as error:
        if identity is not None:
            # Drain cleanup even after repeated cancellation; rollback has finished.
            await _drain_failed_retention(identity, error)
        raise
