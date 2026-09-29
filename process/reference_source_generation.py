# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Writer-fenced revision authority for existing reference result families."""

from sqlalchemy import text

from process import reference_family_result_generation as generation

REVISION_FUNCTION = "advance_reference_source_generation"
REVISION_TRIGGER = "reference_source_generation_revision_guard"


def _family(importer_id, schema_name):
    importer = generation._importer_id(importer_id)
    if importer == "label":
        raise ValueError("Label source revisions belong to its native publication ledger")
    schema = generation._schema_name(schema_name)
    authority_schema = generation._authority_schema(importer, schema)
    return importer, schema, authority_schema


async def _guarded_oids(database, *, schema_name, authority_schema, relation_names):
    return tuple(
        int(guard[0])
        for guard in await generation._all(
            database,
            text(
                "SELECT tgrelid::bigint FROM pg_catalog.pg_trigger "
                "JOIN pg_catalog.pg_class relation ON relation.oid=tgrelid "
                "WHERE tgrelid = ANY(ARRAY(SELECT pg_catalog.to_regclass("
                "format('%I.%I', CAST(:schema AS text), name)) FROM unnest(CAST(:names AS text[])) name)) "
                "AND tgname=:trigger AND tgfoid=pg_catalog.to_regprocedure(:function) "
                "AND tgenabled='A' AND tgtype=60 AND NOT tgisinternal "
                "AND tgnargs=0 AND tgattr::text='' AND tgqual IS NULL "
                "AND tgoldtable IS NULL AND tgnewtable IS NULL "
                "AND relation.relkind='r' AND relation.relpersistence='p' "
                "AND NOT relation.relrowsecurity AND NOT relation.relforcerowsecurity "
                "AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits "
                "WHERE inhrelid=relation.oid OR inhparent=relation.oid)"
            ),
            schema=schema_name,
            names=list(relation_names),
            trigger=REVISION_TRIGGER,
            function=f'{generation._quoted(authority_schema)}."{REVISION_FUNCTION}"()',
        )
    )


async def install_reference_revision_guards(database, *, importer_id, schema_name):
    """Install guards inside the publisher transaction, without granting privileges.

    Missing guards require the relation owner and EXECUTE on the authority function;
    migration provisions existing supported static-family owners. Absent or changed
    owners require explicit function privilege provisioning before installation.
    """
    importer, schema, authority_schema = _family(importer_id, schema_name)
    names = generation.RELATION_NAMES_BY_IMPORTER[importer]
    relations = ", ".join(f"{generation._quoted(schema)}.{generation._quoted(name)}" for name in names)
    await _execute_guard_statement(database, f"LOCK TABLE {relations} IN SHARE MODE NOWAIT")
    expected = await generation.current_reference_family_relation_oids(
        database, importer_id=importer, schema_name=schema
    )
    guarded = await _guarded_oids(database, schema_name=schema, authority_schema=authority_schema, relation_names=names)
    if len(guarded) == len(expected) and set(guarded) == set(expected):
        return
    function = f'{generation._quoted(authority_schema)}."{REVISION_FUNCTION}"'
    for name in names:
        relation = f"{generation._quoted(schema)}.{generation._quoted(name)}"
        for statement in (
            f"LOCK TABLE {relation} IN SHARE ROW EXCLUSIVE MODE NOWAIT",
            f'DROP TRIGGER IF EXISTS "{REVISION_TRIGGER}" ON {relation}',
            f'CREATE TRIGGER "{REVISION_TRIGGER}" AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE '
            f"ON {relation} FOR EACH STATEMENT EXECUTE FUNCTION {function}()",
            f'ALTER TABLE {relation} ENABLE ALWAYS TRIGGER "{REVISION_TRIGGER}"',
        ):
            await _execute_guard_statement(database, statement)
    guarded = await _guarded_oids(database, schema_name=schema, authority_schema=authority_schema, relation_names=names)
    if len(guarded) != len(expected) or set(guarded) != set(expected):
        raise RuntimeError("reference source revision guards are unavailable")


async def _execute_guard_statement(database, statement):
    if hasattr(database, "status"):
        await database.status(statement)
    else:
        await database.execute(text(statement))


async def require_reference_revision_tracking(database, *, importer_id, schema_name):
    """Reject pre-guard history without rewriting or bootstrapping its evidence."""
    importer, schema, authority_schema = _family(importer_id, schema_name)
    tracked = await generation._first(
        database,
        text(
            f"SELECT source_revision_tracked FROM {generation._quoted(authority_schema)}."
            f'"{generation.TABLE_NAME}" WHERE importer_id=:importer'
        ),
        importer=importer,
    )
    if tracked is None or generation._row_mapping(tracked)["source_revision_tracked"] is not True:
        raise RuntimeError("reference source revision tracking is unavailable")
    expected = await generation.current_reference_family_relation_oids(
        database, importer_id=importer, schema_name=schema
    )
    guarded = await _guarded_oids(
        database,
        schema_name=schema,
        authority_schema=authority_schema,
        relation_names=generation.RELATION_NAMES_BY_IMPORTER[importer],
    )
    if len(guarded) != len(expected) or set(guarded) != set(expected):
        raise RuntimeError("reference source revision guards are unavailable")


async def _lock_source(session, *, importer_id, schema_name, expected_relation_oids, bootstrap=False):
    if not callable(getattr(session, "in_transaction", None)) or not session.in_transaction():
        raise ValueError("reference source observation requires a caller transaction")
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise ValueError("reference source observation requires read committed")
    importer, schema, authority_schema = _family(importer_id, schema_name)
    expected = generation._relation_oids(importer, expected_relation_oids)
    names = generation.RELATION_NAMES_BY_IMPORTER[importer]
    relations = ", ".join(f"{generation._quoted(schema)}.{generation._quoted(name)}" for name in names)
    await session.execute(text(f"LOCK TABLE {relations} IN SHARE MODE NOWAIT"))
    # Keep relation-DDL admission fail-fast without blocking unrelated families.
    ledger = f'{generation._quoted(authority_schema)}."{generation.TABLE_NAME}"'
    await session.execute(text(f"LOCK TABLE {ledger} IN ROW SHARE MODE NOWAIT"))
    row_lock = "FOR UPDATE" if bootstrap else "FOR SHARE"
    locked_importer = await session.scalar(
        text(f"SELECT importer_id FROM {ledger} WHERE importer_id=:importer {row_lock} NOWAIT"),
        {"importer": importer},
    )
    if locked_importer is None:
        raise RuntimeError("reference family generation authority is unavailable")
    current = await generation.current_reference_family_relation_oids(session, importer_id=importer, schema_name=schema)
    if current != expected:
        raise RuntimeError("reference source relation identity changed")
    return importer, schema


async def require_reference_source_generation(session, *, importer_id, schema_name, expected_relation_oids):
    """Return actual bound authority while retaining short writer fences."""
    importer, schema = await _lock_source(
        session, importer_id=importer_id, schema_name=schema_name, expected_relation_oids=expected_relation_oids
    )
    await require_reference_revision_tracking(session, importer_id=importer, schema_name=schema)
    authority = await generation.read_reference_family_result_generation_authority(
        session, importer_id=importer, schema_name=schema
    )
    if authority.serving_generation is None or authority.relation_oids != tuple(expected_relation_oids):
        raise RuntimeError("reference source serving generation is unavailable or drifted")
    return authority


async def bootstrap_reference_source_generation(session, *, importer_id, schema_name, expected_relation_oids):
    """Explicitly establish a new local boundary, never historical provenance."""
    importer, schema = await _lock_source(
        session,
        importer_id=importer_id,
        schema_name=schema_name,
        expected_relation_oids=expected_relation_oids,
        bootstrap=True,
    )
    authority_schema = generation._authority_schema(importer, schema)
    tracked = await session.scalar(
        text(
            f"SELECT source_revision_tracked FROM {generation._quoted(authority_schema)}."
            f'"{generation.TABLE_NAME}" WHERE importer_id=:importer'
        ),
        {"importer": importer},
    )
    if tracked:
        return await require_reference_source_generation(
            session, importer_id=importer, schema_name=schema, expected_relation_oids=expected_relation_oids
        )
    return await generation.publish_local_reference_family_generation(session, importer_id=importer, schema_name=schema)
