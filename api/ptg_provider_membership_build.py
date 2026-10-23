# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build a complete authenticated physical-source/code family in one transaction."""

import hashlib
import struct
from uuid import uuid4

from sqlalchemy import text

from api.ptg2_db_sidecars import ForwardReadBudget, _forward_code_block_bounds, lookup_code_prefix_occurrences_from_db
from api.ptg2_v4_graph import load_v4_relation_manifest, lookup_v4_relation_members, v4_graph_request_scope
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
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget

_MAX_SOURCE_READ_BYTES = 256 * 1024 * 1024


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
    if (
        not 0 < expected["stored_map_byte_count"] <= min(MAX_MAP_BYTES, _MAX_SOURCE_READ_BYTES)
        or expected["coordinate_count"] > MAX_BUILD_EDGES
    ):
        raise ValueError("membership source map exceeds the bounded build contract")
    actual = await summarize_persisted_v4_snapshot_maps(
        session,
        schema_name=schema,
        snapshot_key=root.snapshot_key,
        batch_rows=32,
        cancel_gc_candidates=False,
    )
    if actual.map_digest != root.map_digest or any(
        getattr(actual, name) != expected_value for name, expected_value in expected.items()
    ):
        raise PTG2ManifestArtifactError("membership source CAS mapping root differs")
    return expected["stored_map_byte_count"]


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
    # Coalesce one sealed locator page so adjacent owners do not repay its I/O.
    owner_batch = manifest.locator_owner_span
    for start in range(manifest.owner_base, manifest.owner_base + manifest.owner_count, owner_batch):
        owners = tuple(range(start, min(start + owner_batch, manifest.owner_base + manifest.owner_count)))
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
        if sum(map(len, members.values())) > min(MAX_FACTOR_BATCH_MEMBERS, edge_allowance - count):
            raise PTG2ManifestArtifactError("membership factor batch exceeds its retained member allowance")
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


async def _reserve_forward_maps(session, tables, identity, code_key, budget):
    """Reserve complete discovery and mapping pack visits before forward reads."""
    from api import ptg2_serving as serving

    if identity["finalizer"]["state"] == "legacy_relational":
        return
    schema = serving._payload_schema(tables)
    lower, upper = _forward_code_block_bounds(code_key)
    counts = (
        (
            await session.execute(
                text(
                    f"SELECT count(*) AS pages,COALESCE(sum(block.raw_byte_count),0) AS raw_bytes,"
                    f"count(*) FILTER (WHERE block.block_hash IS NULL OR block.raw_byte_count<=0) AS invalid "
                    f'FROM "{schema}".ptg2_v4_finalizer_map_pack pack '
                    f'LEFT JOIN "{schema}".ptg2_v3_block block ON block.block_hash=pack.map_block_hash '
                    "WHERE pack.snapshot_key=:key AND pack.object_kind='by_code_provider_shard_v1' "
                    "AND pack.last_block_key>=:lower AND pack.first_block_key<:upper"
                ),
                {"key": serving._required_shared_snapshot_key(tables), "lower": lower, "upper": upper},
            )
        )
        .mappings()
        .one()
    )
    if counts["invalid"]:
        raise PTG2ManifestArtifactError("membership forward map byte metadata is invalid")
    # The unfiltered prefix discovers the full code range, then streams its
    # mapping once. Range packs conservatively cover both selections.
    budget.claim_page_capacity(page_count=2 * counts["pages"], raw_payload_bytes=2 * counts["raw_bytes"])


async def _copy_code(session, tables, identity, ownership, hasher, *, byte_allowance):
    from api import ptg2_serving as serving

    code_rows = await _physical_code_rows(session, tables, identity["code_system"], identity["code"])
    budget = ForwardReadBudget(
        maximum_fragments=MAX_CODE_OCCURRENCES,
        maximum_raw_payload_bytes=byte_allowance,
        maximum_row_capacity=MAX_CODE_OCCURRENCES + 1,
    )
    coverage_rows = []
    provider_sets = set()
    for code_row in code_rows:
        await _reserve_forward_maps(session, tables, identity, int(code_row["code_key"]), budget)
        limit = int(code_row["rate_count"]) + 1
        budget.reserve_row_capacity(read_rows=limit, result_rows=limit)
        try:
            entries = await lookup_code_prefix_occurrences_from_db(
                session,
                int(code_row["code_key"]),
                limit=limit,
                shared_snapshot_key=serving._required_shared_snapshot_key(tables),
                source_count=serving._required_source_count(tables),
                scan_budget=budget,
                **serving._version_three_forward_lookup_hints(tables),
                schema_name=serving._payload_schema(tables),
            )
        finally:
            budget.release_row_capacity(read_rows=limit, result_rows=limit)
        if len(entries) != int(code_row["rate_count"]):
            raise PTG2ManifestArtifactError("membership requested code occurrence coverage is incomplete")
        sets = sorted({int(entry[0]) for entry in entries})
        provider_sets.update(sets)
        set_digest = hashlib.sha256(b"".join(struct.pack(">I", key) for key in sets)).hexdigest()
        coverage_by_field = {
            "code_key": int(code_row["code_key"]),
            "occurrence_count": len(entries),
            "set_count": len(sets),
            "set_digest": set_digest,
        }
        coverage_rows.append(coverage_by_field)
        hasher.update(encoded(coverage_by_field).encode("ascii"))
        await _copy(session, ownership, PTGMembershipCode, [(coverage_by_field["code_key"], key) for key in sets])
        await _copy(session, ownership, PTGMembershipCodeCoverage, [tuple(coverage_by_field.values())])
        del entries
    return coverage_rows, tuple(sorted(provider_sets)), budget.raw_payload_bytes


async def _copy_code_factor(session, tables, root, ownership, provider_sets, hasher, *, edge_allowance):
    """Transpose complete covered-set owners without expanding other codes."""
    from api import ptg2_serving as serving

    relations_by_representation = {
        "pattern_v1": ("pattern_sets", "set_patterns"),
        "direct_v1": ("group_sets_direct", "set_groups_direct"),
    }
    relation, inverse = relations_by_representation[root.representation]
    schema = serving._payload_schema(tables)
    manifest = await load_v4_relation_manifest(
        session, snapshot_key=root.snapshot_key, relation=relation, schema_name=schema
    )
    if manifest.owner_count > MAX_BUILD_OWNERS:
        raise ValueError("membership factor relation exceeds the bounded build contract")
    inverse_manifest = await load_v4_relation_manifest(
        session, snapshot_key=root.snapshot_key, relation=inverse, schema_name=schema
    )
    if inverse_manifest.logical_member_count != manifest.logical_member_count or any(
        not inverse_manifest.owner_base <= key < inverse_manifest.owner_base + inverse_manifest.owner_count
        for key in provider_sets
    ):
        raise PTG2ManifestArtifactError("membership inverse factor domain differs from the sealed source")
    hasher.update(relation.encode("ascii") + b"\0")
    count = 0
    for start in range(0, len(provider_sets), 128):
        owners = provider_sets[start : start + 128]
        members = await lookup_v4_relation_members(
            session,
            snapshot_key=root.snapshot_key,
            relation=inverse,
            owner_keys=owners,
            schema_name=schema,
            max_members=min(MAX_FACTOR_BATCH_MEMBERS, edge_allowance - count),
        )
        if set(members) != set(owners):
            raise PTG2ManifestArtifactError("membership factor owner coverage is incomplete")
        if sum(map(len, members.values())) > min(MAX_FACTOR_BATCH_MEMBERS, edge_allowance - count):
            raise PTG2ManifestArtifactError("membership factor batch exceeds its retained member allowance")
        factor_rows = []
        for owner in owners:
            factors = members[owner]
            if tuple(factors) != tuple(sorted(set(factors))) or any(
                not manifest.owner_base <= factor < manifest.owner_base + manifest.owner_count for factor in factors
            ):
                raise PTG2ManifestArtifactError("membership inverse factor vector is noncanonical or out of domain")
            for factor in factors:
                factor_rows.append((factor, owner))
                hasher.update(struct.pack(">II", factor, owner))
                if len(factor_rows) == 5000:
                    await _copy(session, ownership, PTGMembershipSecond, factor_rows)
                    factor_rows = []
            count += len(factors)
        await _copy(session, ownership, PTGMembershipSecond, factor_rows)
    return dict(relation=relation, owner_base=manifest.owner_base, owner_count=manifest.owner_count, edge_count=count)


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
    """Build or adopt one complete source/code family within the caller transaction."""
    params_by_name = dict(source_key=source_key, snapshot_id=snapshot_id, code_system=code_system, code=code)
    validate_params(params_by_name)
    await native.protected_publisher_owner(session)
    tables, root, identity = await source_identity(session, **params_by_name)
    await lock_identity(session, identity)
    found = await locate(session, identity)
    if found is not None:
        return receipt(found)
    await source_pin(session, identity, create=True)
    enqueue_live_progress(
        phase="membership source authentication", message="Authenticating the complete source map and CAS closure"
    )
    map_bytes = await _authenticate_root(session, tables, root)
    ownership = await native.precreate_model_family_stage(session, SPEC, uuid4())
    hasher = hashlib.sha256(encoded(identity).encode("ascii"))
    enqueue_live_progress(
        phase="membership procedure coverage", message="Authenticating complete requested-code occurrence coverage"
    )
    coverage, provider_sets, code_bytes = await _copy_code(
        session, tables, identity, ownership, hasher, byte_allowance=_MAX_SOURCE_READ_BYTES - map_bytes
    )
    edge_allowance = min(MAX_BUILD_EDGES, (MAX_BUILD_BYTES - code_bytes) // 8)
    read_budget = RegistryPTGGraphReadBudget(maximum_bytes=_MAX_SOURCE_READ_BYTES - map_bytes - code_bytes)
    with v4_graph_request_scope(read_budget=read_budget):
        first = await _copy_relation(
            session,
            tables,
            root,
            "npi_patterns" if root.representation == "pattern_v1" else "npi_groups_exact",
            ownership,
            PTGMembershipFirst,
            hasher,
            edge_allowance=edge_allowance,
        )
        second = await _copy_code_factor(
            session, tables, root, ownership, provider_sets, hasher, edge_allowance=edge_allowance - first["edge_count"]
        )
    factors = [first, second]
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


async def build_provider_membership_projection(*, source_key, snapshot_id, code_system, code):
    """A lost worker rolls back its whole invisible candidate; a retry adopts ready state."""
    async with db.transaction() as session:
        return await build_in_session(
            session, source_key=source_key, snapshot_id=snapshot_id, code_system=code_system, code=code
        )
