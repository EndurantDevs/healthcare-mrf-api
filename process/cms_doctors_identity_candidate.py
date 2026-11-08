# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Private Doctors identity reductions share the existing source-family cut."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from db.models import CMSDoctorGroupSite
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_source_storage as storage
from process.ext.utils import make_class
from process.provider_directory_cms_identity_candidate import CMSIdentityReprepareRequired
from process.provider_directory_generic_source_policy import (
    bound_publisher,
    source_copy_session,
    source_owner_transaction,
    source_transaction,
)
from process.provider_directory_generic_source_provision import capture_generic_source_global_roots
from process.reference_family_archive import native_copy_projection

_CONTEXT_KEY = "doctors_identity_candidates"
_SPECS = {
    "group": (
        "provider_directory_cms_doctors_group_binding",
        "org_pac_id",
        "organization_id",
        "provider_directory_organization_identity",
        64,
    ),
    "site": (
        "provider_directory_cms_doctors_site_binding",
        "adrs_id",
        "site_id",
        "provider_directory_site_identity",
        256,
    ),
}
_ACTIVE = ContextVar("cms_doctors_identity_read", default=None)
_PAGE_ROWS = 512
_COPY_BYTES = 16 * 1024 * 1024


def reprepare_publication_conflict(error):
    """A winner before map publication still invalidates every prepared output."""
    from process.entity_address_cutover_contract import postgres_sqlstate

    if postgres_sqlstate(error) in {"40001", "23505"}:
        raise CMSIdentityReprepareRequired("cms_doctors_identity_whole_candidate_reprepare_required") from error


def _require(condition, code):
    if not condition:
        raise RuntimeError("cms_doctors_identity_" + code)


@dataclass(frozen=True)
class DoctorsIdentityCandidate:
    kind: str
    import_date: str
    source_name: str
    source_oid: int
    generation_id: str
    source_rows: int
    artifact_sha256: str
    policy_identity: tuple
    table: dict
    count: int


def _policy_identity(policy):
    return (policy.schema, policy.owner_role, policy.publisher_role, policy.runtime_roles, policy.control_runtime_roles)


def _source_coordinates(ctx, import_date, schema, receipt):
    context = ctx.get("context") or {}
    _require(
        context.get("group_site_stage_owned") or context.get("publication_state") == "prepared", "source_not_owned"
    )
    _require(ctx.get("import_date") == import_date and type(receipt) is dict, "source_changed")
    artifact = context.get("artifact", {}).get("content_sha256")
    _require(type(artifact) is str and len(artifact) == 64, "artifact_required")
    _require(type(receipt.get("source_rows")) is int and receipt["source_rows"] > 0, "source_count_changed")
    _require(type(receipt.get("generation_id")) is str and len(receipt["generation_id"]) == 64, "generation_changed")
    stage = make_class(CMSDoctorGroupSite, import_date).__table__
    _require(stage.schema == schema, "schema_changed")
    return stage, artifact


async def _source_identity(database, stage, receipt, *, expected_oid=None, require_logged=False):
    from process.cms_doctors_groups import validate_group_site_stage
    from process.cms_doctors_organizations import _lock_group_stage
    from process.cms_doctors_preparation import _assert_stage_indexes

    publisher = bound_publisher()
    session = source_copy_session()
    oid = await _lock_group_stage(session, stage, expected_oid)
    await _lock_group_stage(session, stage, oid, identifier_column="adrs_id")
    owner = await publisher.connection.fetchval(
        "SELECT c.relowner=(SELECT oid FROM pg_roles WHERE rolname=$2) AND c.relkind='r' "
        "AND c.relpersistence IN ('p','u') AND (NOT $3 OR c.relpersistence='p') "
        "AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity FROM pg_class c WHERE c.oid=$1::oid",
        oid,
        publisher.policy.owner_role,
        require_logged,
    )
    _require(owner is True, "source_owner_changed")
    import_date = stage.name.removeprefix(CMSDoctorGroupSite.__tablename__ + "_")
    await validate_group_site_stage(import_date, stage.schema, receipt)
    await _assert_stage_indexes(session, stage.schema, make_class(CMSDoctorGroupSite, import_date))
    return oid


async def require_doctors_identity_roots(database):
    """Require the native-admin cut; managed maps are never startup-created."""
    async with source_transaction(database):
        publisher = bound_publisher()
        await capture_generic_source_global_roots(publisher, tuple(spec[0] for spec in _SPECS.values()))


async def _copy_pages(database, stage, name, spec, decision_at, cancel_check):
    _, key, identity_column, _, _ = spec
    publisher = bound_publisher()
    source_relation = storage_relation(str(stage.schema), str(stage.name))
    incumbent = storage_relation(str(stage.schema), spec[0])
    key, identity_column = storage_identifier(key), storage_identifier(identity_column)
    invalid = await publisher.connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {source_relation} WHERE {key} IS NOT NULL AND {key}<>'' AND char_length({key})>$1)",
        spec[4],
    )
    _require(invalid is False, "source_key_too_large")
    after_key = ""
    while True:
        keys = await publisher.connection.fetch(
            f"SELECT DISTINCT {key} FROM {source_relation} WHERE {key} IS NOT NULL AND {key}<>'' AND {key}>$1 "
            f"ORDER BY {key} LIMIT {_PAGE_ROWS}",
            after_key,
        )
        if not keys:
            return
        _require(
            all(type(entry[0]) is str and entry[0] and entry[0] == entry[0].strip() for entry in keys),
            "source_key_invalid",
        )
        last_key = keys[-1][0]
        query = (
            f"SELECT selected.{key},coalesce(incumbent.{identity_column},gen_random_uuid()),"
            f"coalesce(incumbent.created_at,$3::timestamptz) FROM (SELECT DISTINCT {key} FROM {source_relation} "
            f"WHERE {key}>$1 AND {key}<=$2 AND {key} IS NOT NULL AND {key}<>'') selected "
            f"LEFT JOIN {incumbent} incumbent USING({key}) ORDER BY selected.{key}"
        )
        await native_copy_projection(
            source_copy_session(),
            query,
            after_key,
            last_key,
            decision_at,
            schema_name=str(stage.schema),
            table_name=name,
            columns=(spec[1], spec[2], "created_at"),
            max_bytes=_COPY_BYTES,
            timeout=300,
        )
        after_key = last_key
        await cancel_check()


async def _complete(connection, schema, source_name, proof, spec):
    root, key, identity_column, canonical_root, _ = spec
    source, candidate = storage_relation(schema, source_name), storage_relation(schema, proof["name"])
    incumbent, canonical = storage_relation(schema, root), storage_relation(schema, canonical_root)
    key, identity_column = storage_identifier(key), storage_identifier(identity_column)
    invalid = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {source} source LEFT JOIN {candidate} candidate USING({key}) "
        f"WHERE source.{key} IS NOT NULL AND source.{key}<>'' AND candidate.{key} IS NULL) "
        f"OR EXISTS(SELECT FROM {candidate} candidate WHERE NOT EXISTS(SELECT FROM {source} source "
        f"WHERE source.{key}=candidate.{key})) OR EXISTS(SELECT FROM {candidate} candidate "
        f"LEFT JOIN {incumbent} incumbent USING({key}) LEFT JOIN {canonical} canonical "
        f"ON canonical.{identity_column}=candidate.{identity_column} WHERE "
        f"(incumbent.{key} IS NOT NULL AND (incumbent.{identity_column},incumbent.created_at) "
        f"IS DISTINCT FROM (candidate.{identity_column},candidate.created_at)) "
        f"OR (canonical.{identity_column} IS NOT NULL AND incumbent.{key} IS NULL) "
        f"OR (incumbent.{key} IS NOT NULL AND canonical.{identity_column} IS NULL))"
    )
    if invalid:
        raise CMSIdentityReprepareRequired("cms_doctors_identity_whole_candidate_reprepare_required")
    return int(await connection.fetchval(f"SELECT count(*) FROM {candidate}"))


async def prepare_doctors_identity_kind(database, ctx, import_date, schema, receipt, *, kind, cancel_check):
    """COPY exact source keys privately; finish all indexes before set validation."""
    _require(kind in _SPECS, "kind_invalid")
    stage, artifact = _source_coordinates(ctx, import_date, schema, receipt)
    context = ctx["context"]
    _require(kind not in context.get(_CONTEXT_KEY, {}), "already_prepared")
    spec = _SPECS[kind]
    async with source_transaction(database):
        publisher = bound_publisher()
        _require(publisher.policy.schema == schema, "schema_changed")
        oid = await _source_identity(database, stage, receipt)
        name = "cmsdi_" + uuid4().hex + "_" + kind
        relation = storage_relation(schema, name)
        await publisher.connection.execute(
            f"CREATE TABLE {relation} (LIKE {storage_relation(schema, spec[0])} "
            "INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING CONSTRAINTS INCLUDING STORAGE)"
        )
        await storage._close_grants(publisher.connection, publisher.policy, name)
        await _copy_pages(database, stage, name, spec, datetime.now(timezone.utc), cancel_check)
        await publisher.connection.execute(f"ALTER TABLE {relation} ADD PRIMARY KEY ({storage_identifier(spec[1])})")
        await publisher.connection.execute(f"ALTER TABLE {relation} ADD UNIQUE ({storage_identifier(spec[2])})")
        await publisher.connection.execute(f"ANALYZE {relation}")
        proof = (await storage._capture(publisher.connection, publisher.policy, [name]))[name]
        count = await _complete(publisher.connection, schema, str(stage.name), proof, spec)
        candidate = DoctorsIdentityCandidate(
            kind,
            import_date,
            str(stage.name),
            oid,
            receipt["generation_id"],
            receipt["source_rows"],
            artifact,
            _policy_identity(publisher.policy),
            proof,
            count,
        )
        # Register exact OID custody before the outer transaction can commit.
        context.setdefault(_CONTEXT_KEY, {})[kind] = candidate
        await cancel_check()
    return count


def _candidates(ctx):
    candidates = (ctx.get("context") or {}).get(_CONTEXT_KEY)
    _require(type(candidates) is dict and set(candidates) == set(_SPECS), "complete_candidate_required")
    _require(
        all(type(value) is DoctorsIdentityCandidate and value.kind == kind for kind, value in candidates.items()),
        "candidate_changed",
    )
    return candidates


async def _verify_candidate(database, candidate, ctx):
    publisher = bound_publisher()
    _require(candidate.policy_identity == _policy_identity(publisher.policy), "policy_changed")
    receipt_by_field = {"source_rows": candidate.source_rows, "generation_id": candidate.generation_id}
    stage, artifact = _source_coordinates(ctx, candidate.import_date, publisher.policy.schema, receipt_by_field)
    _require(stage.name == candidate.source_name and artifact == candidate.artifact_sha256, "source_changed")
    await _source_identity(database, stage, receipt_by_field, expected_oid=candidate.source_oid, require_logged=True)
    actual = await storage._capture(publisher.connection, publisher.policy, [candidate.table["name"]])
    _require(actual[candidate.table["name"]] == candidate.table, "storage_changed")
    count = await _complete(
        publisher.connection, publisher.policy.schema, str(stage.name), candidate.table, _SPECS[candidate.kind]
    )
    _require(count == candidate.count, "count_changed")


async def _publish_kind(connection, schema, candidate):
    root, key, identity_column, canonical_root, _ = _SPECS[candidate.kind]
    stage, target = storage_relation(schema, candidate.table["name"]), storage_relation(schema, root)
    canonical = storage_relation(schema, canonical_root)
    key, identity_column = storage_identifier(key), storage_identifier(identity_column)
    conflict = await connection.fetchval(
        f"WITH needed AS MATERIALIZED (SELECT candidate.{identity_column},candidate.created_at FROM {stage} candidate "
        f"LEFT JOIN {target} incumbent USING({key}) WHERE incumbent.{key} IS NULL), inserted AS ("
        f"INSERT INTO {canonical} ({identity_column},created_at) SELECT {identity_column},created_at FROM needed "
        f"ORDER BY {identity_column} ON CONFLICT ({identity_column}) DO NOTHING RETURNING {identity_column}) "
        f"SELECT EXISTS(SELECT FROM needed LEFT JOIN inserted USING({identity_column}) "
        f"WHERE inserted.{identity_column} IS NULL)"
    )
    if conflict:
        raise CMSIdentityReprepareRequired("cms_doctors_identity_whole_candidate_reprepare_required")
    await connection.execute(
        f"INSERT INTO {target} ({key},{identity_column},created_at) SELECT {key},{identity_column},created_at "
        f"FROM {stage} ORDER BY {key} ON CONFLICT ({key}) DO NOTHING"
    )
    changed = await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {stage} candidate LEFT JOIN {target} incumbent USING({key}) WHERE "
        f"incumbent.{key} IS NULL OR (candidate.{identity_column},candidate.created_at) "
        f"IS DISTINCT FROM (incumbent.{identity_column},incumbent.created_at))"
    )
    if changed:
        raise CMSIdentityReprepareRequired("cms_doctors_identity_whole_candidate_reprepare_required")


async def publish_doctors_identities(database, ctx):
    """Share the caller's family-swap transaction; rollback even when caller catches."""
    import asyncpg

    candidates = _candidates(ctx)
    try:
        async with source_owner_transaction(database) as publisher:
            names = sorted(
                storage_relation(publisher.policy.schema, value.table["name"]) for value in candidates.values()
            )
            await publisher.connection.execute("LOCK TABLE " + ",".join(names) + " IN SHARE MODE NOWAIT")
            for candidate in candidates.values():
                await _verify_candidate(database, candidate, ctx)
            for candidate in candidates.values():
                await _publish_kind(publisher.connection, publisher.policy.schema, candidate)
    except (asyncpg.SerializationError, asyncpg.UniqueViolationError) as error:
        raise CMSIdentityReprepareRequired("cms_doctors_identity_whole_candidate_reprepare_required") from error


async def discard_doctors_identities(database, ctx):
    """Finish exact private cleanup before propagating cancellation."""
    if not (ctx.get("context") or {}).get(_CONTEXT_KEY):
        return
    _require(database._transaction_binding() is None, "cleanup_transaction_active")
    task = asyncio.create_task(_discard_identities(database, ctx))
    is_cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            is_cancelled = True
    task.result()
    if is_cancelled:
        raise asyncio.CancelledError


async def _discard_identities(database, ctx):
    """Drop only exact task-created map OIDs, preserving any replacement."""
    context = ctx.get("context") or {}
    candidates = context.get(_CONTEXT_KEY, {})
    if not candidates:
        return
    async with source_transaction(database):
        publisher = bound_publisher()
        await publisher.connection.execute("SET LOCAL lock_timeout='500ms'")
        await publisher.connection.execute("SET LOCAL statement_timeout='5s'")
        for kind, candidate in candidates.items():
            _require(type(candidate) is DoctorsIdentityCandidate and kind == candidate.kind, "candidate_changed")
            _require(candidate.policy_identity == _policy_identity(publisher.policy), "policy_changed")
            name = candidate.table["name"]
            oid = await publisher.connection.fetchval(
                "SELECT to_regclass($1)::oid::bigint", storage_relation(publisher.policy.schema, name)
            )
            if oid is not None:
                await publisher.connection.execute(
                    f"LOCK TABLE {storage_relation(publisher.policy.schema, name)} IN ACCESS EXCLUSIVE MODE NOWAIT"
                )
                actual = await storage._capture(publisher.connection, publisher.policy, [name])
                _require(actual[name] == candidate.table, "cleanup_storage_changed")
                await publisher.connection.execute(f"DROP TABLE {storage_relation(publisher.policy.schema, name)}")
    context.pop(_CONTEXT_KEY, None)


@asynccontextmanager
async def doctors_identity_read_scope(database, ctx):
    """Low-volume compatibility can read only the already prepared batch maps."""
    candidates = _candidates(ctx)
    _require(_ACTIVE.get() is None, "read_scope_nested")
    async with source_transaction(database):
        for candidate in candidates.values():
            await _verify_candidate(database, candidate, ctx)
        token = _ACTIVE.set((asyncio.current_task(), source_copy_session(), candidates))
        try:
            yield
        finally:
            _ACTIVE.reset(token)


async def prepared_doctors_ids(session, kind, keys):
    """One indexed map read, never a mint or global validation fallback."""
    scope = _ACTIVE.get()
    _require(
        scope is not None and scope[0] is asyncio.current_task() and scope[1] is session, "prepared_read_scope_required"
    )
    candidate = scope[2][kind]
    _, key, identity_column, _, _ = _SPECS[kind]
    publisher = bound_publisher()
    relation = storage_relation(publisher.policy.schema, candidate.table["name"])
    rows = await publisher.connection.fetch(
        f"SELECT {storage_identifier(key)},{storage_identifier(identity_column)} FROM {relation} "
        f"WHERE {storage_identifier(key)}=ANY($1::varchar[])",
        list(dict.fromkeys(keys)),
    )
    ids_by_key = dict(rows)
    _require(set(ids_by_key) == set(keys), "prepared_keys_missing")
    return [ids_by_key[key] for key in keys]
