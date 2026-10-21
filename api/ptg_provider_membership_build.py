# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build a complete authenticated physical-source/code family in one transaction."""

import hashlib
import struct
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
    encoded,
    validate_params,
)
from api.ptg_membership_source import source_identity
from api.ptg_membership_storage import locate, lock_identity, receipt, seal, source_pin
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
        rows = []
        for owner in owners:
            values = members[owner]
            if tuple(values) != tuple(sorted(set(values))) or any(not 0 <= value <= 0xFFFFFFFF for value in values):
                raise PTG2ManifestArtifactError("membership factor vector is noncanonical")
            for member in values:
                rows.append((owner, member))
                hasher.update(struct.pack(">II", owner, member))
                if len(rows) == 5000:
                    await _copy(session, ownership, model, rows)
                    rows = []
            count += len(values)
        await _copy(session, ownership, model, rows)
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
    params = {"snapshot_key": serving._required_shared_snapshot_key(tables)}
    serving._append_reported_code_system_filter(
        predicates,
        params,
        column="code_metadata.reported_code_system",
        code_system=code_system,
    )
    serving._append_reported_code_value_filter(
        predicates,
        params,
        column="code_metadata.reported_code",
        param_name="reported_code",
        values=serving._ptg2_reported_code_lookup_values(code_system, code),
    )
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT code_metadata.code_key,code_metadata.rate_count FROM {serving._shared_v3_code_table(tables)} code_metadata "
                    f"WHERE {' AND '.join(predicates)} ORDER BY code_metadata.code_key LIMIT :row_limit"
                ),
                params | {"row_limit": MAX_CODE_OCCURRENCES + 1},
            )
        )
        .mappings()
        .all()
    )
    if len(rows) > MAX_CODE_OCCURRENCES or sum(int(row["rate_count"]) for row in rows) > MAX_CODE_OCCURRENCES:
        raise ValueError("membership requested code exceeds the bounded build contract")
    return rows


async def _copy_code(session, tables, identity, ownership, hasher, *, byte_allowance):
    from api import ptg2_serving as serving

    rows = await _physical_code_rows(session, tables, identity["code_system"], identity["code"])
    budget = ForwardReadBudget(
        maximum_fragments=MAX_CODE_OCCURRENCES,
        maximum_raw_payload_bytes=byte_allowance,
        maximum_row_capacity=MAX_CODE_OCCURRENCES + 1,
    )
    coverage = []
    for row in rows:
        entries = await serving._shared_forward_entries_for_code_rows(session, tables, [row], scan_budget=budget)
        if len(entries) != int(row["rate_count"]):
            raise PTG2ManifestArtifactError("membership requested code occurrence coverage is incomplete")
        sets = sorted({int(entry.provider_set_key) for entry in entries})
        set_digest = hashlib.sha256(b"".join(struct.pack(">I", key) for key in sets)).hexdigest()
        covered = {
            "code_key": int(row["code_key"]),
            "occurrence_count": len(entries),
            "set_count": len(sets),
            "set_digest": set_digest,
        }
        coverage.append(covered)
        hasher.update(encoded(covered).encode("ascii"))
        await _copy(session, ownership, PTGMembershipCode, [(covered["code_key"], key) for key in sets])
        await _copy(session, ownership, PTGMembershipCodeCoverage, [tuple(covered.values())])
    return coverage


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


async def build_in_session(session, *, source_key, snapshot_id, code_system, code):
    params = dict(source_key=source_key, snapshot_id=snapshot_id, code_system=code_system, code=code)
    validate_params(params)
    await native.protected_publisher_owner(session)
    tables, root, identity = await source_identity(session, **params)
    await lock_identity(session, identity)
    found = await locate(session, identity)
    if found is not None:
        return receipt(found)
    await source_pin(session, identity, create=True)
    enqueue_live_progress(
        phase="membership source authentication", message="Authenticating the complete source map and CAS closure"
    )
    await _authenticate_root(session, tables, root)
    ownership = await native.precreate_model_family_stage(session, SPEC, uuid4())
    hasher = hashlib.sha256(encoded(identity).encode("ascii"))
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
    enqueue_live_progress(
        phase="membership procedure coverage", message="Authenticating complete requested-code occurrence coverage"
    )
    coverage = await _copy_code(
        session, tables, identity, ownership, hasher, byte_allowance=MAX_BUILD_BYTES - edges * 8
    )
    counts = {"factors": factors, "code_coverage": coverage}
    await _copy(
        session, ownership, PTGMembershipManifest, [(1, encoded(identity), hasher.hexdigest(), encoded(counts))]
    )
    enqueue_live_progress(
        phase="membership indexes and validation", message="Completing isolated native indexes and set validation"
    )
    await native.complete_model_family_stage(session, SPEC, ownership)
    await _validate_candidate(session, ownership, factors, coverage)
    for table in SPEC.table_names:
        await session.execute(text(f"ANALYZE {native._quoted(ownership.schema_name)}.{native._quoted(table)}"))
    _, _, after = await source_identity(session, **params, tables=tables)
    if after != identity:
        raise PTG2ManifestArtifactError("membership source changed during candidate build")
    await seal(session, ownership, identity, reader_principals=await _reader_principals(session, tables))
    return receipt(await locate(session, identity))


async def build_provider_membership_projection(*, source_key, snapshot_id, code_system, code):
    """A lost worker rolls back its whole invisible candidate; a retry adopts ready state."""
    async with db.transaction() as session:
        return await build_in_session(
            session, source_key=source_key, snapshot_id=snapshot_id, code_system=code_system, code=code
        )
