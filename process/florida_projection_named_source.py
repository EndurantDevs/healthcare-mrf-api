# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named projection adapters for the existing Source-profile publication lifecycle."""

import hashlib
from collections.abc import Mapping

from sqlalchemy import text

from db import models
from process import florida_projection_archive as legacy
from process import reference_family_archive as native

PROJECTION = legacy.PROJECTION
FL_MQA_SOURCE_KEY = legacy.FL_MQA_SOURCE_KEY
require = legacy.require
_table = legacy._table
_role_ident = legacy._role_ident
_cutover_name = legacy._cutover_name
_publication_lock = legacy._publication_lock
_copy_named_projection_candidate = legacy._copy_named_projection_candidate
require_native_publication_order = legacy.require_native_publication_order


_LEGACY_RETAINED_LIMIT = 16


def _retained_name(run_id):
    """Named cutovers retain a predecessor under a name derived from its run."""
    return PROJECTION + "_r_" + hashlib.sha256(run_id.encode()).hexdigest()[:32]


async def _heap_generation(session, schema, name):
    """Read one row of a published single-generation heap, never scanning it."""
    if await native._relation_oid(session, schema, name) is None:
        return None
    return await session.scalar(text(f"SELECT generation_id FROM {_table(schema, name)} LIMIT 1"))


async def named_source_projection(session, schema, run_id):
    """Locate one serving generation by its qualified name, never by saved physical ID."""
    if run_id is None:
        return {"schema": schema, "table_name": PROJECTION}
    for name in (PROJECTION, _retained_name(run_id)):
        if await _heap_generation(session, schema, name) == run_id:
            return {"schema": schema, "table_name": name}
    # Legacy receipts predate run-derived names; read one row from each of a bounded set of retained heaps.
    names = (
        (
            await session.execute(
                text(
                    "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=:schema AND c.relkind='r' AND c.relname ~ :pattern ORDER BY c.relname LIMIT :limit"
                ),
                {
                    "schema": schema,
                    "pattern": "^" + PROJECTION + "(_old|_retained_[0-9]+)$",
                    "limit": _LEGACY_RETAINED_LIMIT + 1,
                },
            )
        )
        .scalars()
        .all()
    )
    require(len(names) <= _LEGACY_RETAINED_LIMIT, "Florida legacy retained heaps exceed their bound")
    matches = [name for name in names if await _heap_generation(session, schema, name) == run_id]
    require(len(matches) == 1, "Florida serving generation is missing or ambiguous")
    return {"schema": schema, "table_name": matches[0]}


async def named_publication_pointer(session, schema):
    """Return the logical predecessor and qualified live/rollback names."""
    row = (
        (
            await session.execute(
                text(
                    f"SELECT current_run_id,previous_run_id FROM {_table(schema, 'provider_profile_source_publication')} WHERE source_key=:source"
                ),
                {"source": FL_MQA_SOURCE_KEY},
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        current = await session.scalar(text(f"SELECT generation_id FROM {_table(schema, PROJECTION)} LIMIT 1"))
        previous = None
        if await native._relation_oid(session, schema, PROJECTION + "_old") is not None:
            previous = await session.scalar(
                text(f"SELECT generation_id FROM {_table(schema, PROJECTION + '_old')} LIMIT 1")
            )
    else:
        current, previous = row["current_run_id"], row["previous_run_id"]
    return {
        "current_run_id": current,
        "previous_run_id": previous,
        "current_relation": await named_source_projection(session, schema, current),
        "previous_relation": await named_source_projection(session, schema, previous) if previous else None,
    }


def named_pointer_equal(observed, expected):
    """Compare current names exactly while accepting saved logical predecessor runs."""
    if "current_relation" in expected:
        return observed == expected
    return all(observed.get(key) == expected.get(key) for key in ("current_run_id", "previous_run_id"))


async def named_projection_seal(session, schema, name):
    """Require the declared persistent standalone model heap and actual SELECT access."""
    require(
        await session.scalar(
            text(
                "SELECT c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition AND NOT c.relrowsecurity "
                "AND NOT c.relforcerowsecurity AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) "
                "FROM pg_class c WHERE c.oid=to_regclass(:relation)"
            ),
            {"relation": _table(schema, name)},
        ),
        "Florida named projection shape differs",
    )
    await session.execute(text(f"SELECT * FROM {_table(schema, name)} LIMIT 0"))
    return {"schema": schema, "table_name": name}


async def freeze_named_projection(session, schema, name, *, roles=None):
    """Close actual table and column grants without changing the shared schema."""
    from types import SimpleNamespace

    from process.provider_directory_generic_source_storage import _close_grants

    owner, readers = native.model_family_roles(roles)
    await session.execute(text(f"ALTER TABLE {_table(schema, name)} OWNER TO {_role_ident(owner)}"))
    driver = await native._native_model_copy_driver(session, ("execute", "fetch", "fetchval"))
    await _close_grants(driver, SimpleNamespace(schema=schema, runtime_roles=readers), name, readable=True)
    return await named_projection_seal(session, schema, name)


async def freeze_existing_source_projection(session, schema, name, *, roles=None):
    """Retain existing effective readers while closing writes on a live generation."""
    grants = await _named_projection_reads(session, schema, name)
    seal = await freeze_named_projection(session, schema, name, roles=roles)
    await _replay_named_projection_reads(session, schema, name, grants)
    return seal


async def _named_projection_reads(session, schema, name):
    return (
        (
            await session.execute(
                text(
                    "SELECT role.rolname,a.grantee=0 AS public,a.is_grantable,a.grantee=c.relowner AS owner "
                    "FROM pg_class c CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a "
                    "LEFT JOIN pg_roles role ON role.oid=a.grantee WHERE c.oid=to_regclass(:relation) AND a.privilege_type='SELECT'"
                ),
                {"relation": _table(schema, name)},
            )
        )
        .mappings()
        .all()
    )


async def _replay_named_projection_reads(session, schema, name, grants):
    require(len(grants) <= 128, "Florida read ACL exceeds its bound")
    for grant in grants:
        role = "PUBLIC" if grant["public"] else _role_ident(grant["rolname"])
        suffix = " WITH GRANT OPTION" if grant["is_grantable"] and not grant["owner"] else ""
        await session.execute(text(f"GRANT SELECT ON TABLE {_table(schema, name)} TO {role}" + suffix))


async def _require_named_projection_dependencies(session, schema, name):
    require(
        not await session.scalar(
            text(
                "SELECT EXISTS(SELECT FROM pg_constraint WHERE contype='f' AND confrelid=to_regclass(:relation) "
                "UNION ALL SELECT FROM pg_depend d JOIN pg_rewrite r ON r.oid=d.objid "
                "WHERE d.classid='pg_rewrite'::regclass AND d.refobjid=to_regclass(:relation) AND r.ev_class<>to_regclass(:relation))"
            ),
            {"relation": _table(schema, name)},
        ),
        "Florida projection has external dependents",
    )


def validate_named_cutover(value, expected_run_id, cutover_id, *, schema=None):
    """Accept old saved table names without interpreting their obsolete numeric tokens."""
    require(isinstance(value, Mapping) and set(value) == {"expected", "cutover"}, "Florida named cutover differs")
    seal_by_field = value["cutover"]
    require(
        isinstance(value["expected"], Mapping)
        and value["expected"].get("current_run_id") == expected_run_id
        and isinstance(seal_by_field, Mapping),
        "Florida named candidate differs",
    )
    if set(seal_by_field) == {"relation_oid", "owner_oid", "table_name"}:
        require(isinstance(schema, str), "Florida saved candidate schema is unavailable")
        seal_by_field = {"schema": schema, "table_name": seal_by_field["table_name"]}
    require(
        set(seal_by_field) == {"schema", "table_name"} and seal_by_field["table_name"] == _cutover_name(cutover_id),
        "Florida named candidate differs",
    )
    _table(seal_by_field["schema"], seal_by_field["table_name"])
    require(schema is None or seal_by_field["schema"] == schema, "Florida candidate schema differs")
    return {"expected": dict(value["expected"]), "cutover": dict(seal_by_field)}


async def prepare_named_cutover(session, *, prepared, schema, expected, cutover_id, source_copy, deadline, roles=None):
    """Copy and compare all projection content under the original byte and time bounds."""
    await _publication_lock(session, schema)
    require(
        named_pointer_equal(await named_publication_pointer(session, schema), expected), "Florida predecessor changed"
    )
    await require_native_publication_order(session, prepared, schema, expected)
    name = await _copy_named_projection_candidate(session, prepared, schema, cutover_id, source_copy, deadline)
    seal = await freeze_named_projection(session, schema, name, roles=roles)
    return validate_named_cutover({"expected": dict(expected), "cutover": seal}, expected["current_run_id"], cutover_id)


async def publish_named_source_projection(session, schema, seal, expected, run_id, *, roles=None):
    """Switch verified named heaps atomically while retaining the prior generation."""
    await _publication_lock(session, schema)
    pointer = await named_publication_pointer(session, schema)
    require(named_pointer_equal(pointer, expected), "Florida retained predecessor changed")
    name = seal["table_name"]
    require(seal.get("schema", schema) == schema and name != PROJECTION, "Florida named candidate differs")
    # Both heaps are frozen to SELECT; the bounded native freeze and RENAME DDL take their exclusive locks.
    await native._lock_family(session, schema, (PROJECTION, name), "ACCESS SHARE", nowait=True)
    await named_projection_seal(session, schema, name)
    require(
        await session.scalar(
            text(f"SELECT NOT EXISTS(SELECT FROM {_table(schema, name)} WHERE generation_id<>:run)"), {"run": run_id}
        )
        and await session.scalar(text(f"SELECT EXISTS(SELECT FROM {_table(schema, name)})")),
        "Florida candidate data scope differs",
    )
    await _require_named_projection_dependencies(session, schema, PROJECTION)
    grants = await _named_projection_reads(session, schema, PROJECTION)
    await freeze_named_projection(session, schema, PROJECTION, roles=roles)
    await _replay_named_projection_reads(session, schema, PROJECTION, grants)
    await freeze_named_projection(session, schema, name, roles=roles)
    await _replay_named_projection_reads(session, schema, name, grants)
    current = pointer["current_run_id"]
    if current is None:
        require(
            not await session.scalar(text(f"SELECT EXISTS(SELECT FROM {_table(schema, PROJECTION)})")),
            "Florida bootstrap predecessor is not empty",
        )
        await session.execute(text(f"DROP TABLE {_table(schema, PROJECTION)} RESTRICT"))
    else:
        retained = _retained_name(current)
        require(await native._relation_oid(session, schema, retained) is None, "Florida retained alias exists")
        await session.execute(text(f"ALTER TABLE {_table(schema, PROJECTION)} RENAME TO {native._quoted(retained)}"))
    await session.execute(text(f"ALTER TABLE {_table(schema, name)} RENAME TO {native._quoted(PROJECTION)}"))
    await session.execute(
        text(
            f"INSERT INTO {_table(schema, 'provider_profile_source_publication')}(source_key,current_run_id,previous_run_id,published_at) VALUES(:source,:run,:previous,clock_timestamp()) ON CONFLICT(source_key) DO UPDATE SET current_run_id=EXCLUDED.current_run_id,previous_run_id=EXCLUDED.previous_run_id,published_at=EXCLUDED.published_at"
        ),
        {"source": FL_MQA_SOURCE_KEY, "run": run_id, "previous": current},
    )
    return {"run_id": run_id, "relation": {"schema": schema, "table_name": PROJECTION}}


async def rollback_named_source_projection(session, prepared, schema, expected, projection=None, *, roles=None):
    """Restore the logical predecessor only after complete typed content parity."""
    pointer = await named_publication_pointer(session, schema)
    require(
        named_pointer_equal(pointer, expected) and pointer["previous_run_id"] == prepared.manifest["run_id"],
        "Florida rollback predecessor differs",
    )
    target = pointer["previous_relation"]
    await native._lock_family(session, schema, (target["table_name"],), "ACCESS SHARE", nowait=True)
    require(
        await native._is_model_table_equal(
            session,
            models.ProviderProfileProjection,
            left_schema=prepared.ownership.schema_name,
            left_name=PROJECTION,
            right_schema=schema,
            right_name=target["table_name"],
        ),
        "Florida rollback predecessor content differs",
    )
    return await publish_named_source_projection(
        session, schema, target, pointer, prepared.manifest["run_id"], roles=roles
    )
