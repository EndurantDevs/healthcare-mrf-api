# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare equivalent historical revision rows for the existing named ledger migration."""

from collections.abc import Mapping

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_native_inputs as inputs
from process import provider_directory_cms_native_registration as registration
from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot

_TABLE = "cms_native_input_revision"
_GUARDS = {"cms_native_input_revision_immutable": 27, "cms_native_input_revision_no_truncate": 34}
_COLUMNS = ("schema_name", "table_name", "revision", "mutation_xid")

_LEGACY_COLUMNS = (
    ("relation_oid", "bigint"),
    ("schema_name", "name"),
    ("table_name", "name"),
    ("revision", "bigint"),
    ("mutation_xid", "xid8"),
)


def _require(condition, reason):
    if not condition:
        raise RuntimeError("cms_native_metadata_" + reason)


def _roles(writer_roles, reader_roles, *roles):
    _require(isinstance(writer_roles, tuple) and bool(writer_roles), "writer_roles_required")
    _require(isinstance(reader_roles, tuple), "reader_roles_required")
    for role in (*roles, *writer_roles, *reader_roles):
        storage_identifier(role)
    return tuple(dict.fromkeys((*writer_roles, *reader_roles, roles[-1])))


async def _columns(connection, relation):
    columns = await connection.fetch(
        "SELECT a.attname::text AS name,format_type(a.atttypid,a.atttypmod) AS type "
        "FROM pg_catalog.pg_attribute a WHERE a.attrelid=to_regclass($1) AND a.attnum>0 "
        "AND NOT a.attisdropped ORDER BY a.attnum",
        relation,
    )
    return tuple((column["name"], column["type"]) for column in columns)


async def _legacy_projection(connection, relation):
    counts = await connection.fetchrow(
        f"""WITH logical AS (SELECT DISTINCT schema_name,table_name,revision,mutation_xid FROM {relation})
        SELECT (SELECT count(*) FROM {relation}) AS raw_rows,(SELECT count(*) FROM logical) AS named_rows,
        EXISTS(SELECT FROM logical WHERE schema_name IS NULL OR table_name IS NULL OR revision IS NULL
            OR schema_name::text !~ '^[A-Za-z_][A-Za-z0-9_]{{0,62}}$'
            OR table_name::text !~ '^[A-Za-z_][A-Za-z0-9_]{{0,62}}$' OR revision<0) AS invalid,
        EXISTS(SELECT FROM logical GROUP BY schema_name,table_name HAVING count(*)>1) AS divergent"""
    )
    _require(not counts["invalid"], "recorded_names_or_revision_invalid")
    _require(not counts["divergent"], "duplicate_revisions_diverge")
    return dict(counts)


async def _guard_definitions(connection, relation, schema):
    previous_path = await connection.fetchval("SHOW search_path")
    await connection.execute("SELECT pg_catalog.set_config('search_path','pg_catalog',true)")
    guards = await connection.fetch(
        "SELECT t.tgname,t.tgtype,t.tgenabled::text AS enabled,pg_get_triggerdef(t.oid,true) AS definition,"
        "t.tgnargs=0 AND t.tgqual IS NULL AND t.tgattr=''::int2vector "
        "AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgconstraint=0 "
        "AND t.tgfoid=to_regprocedure($2) AS safe FROM pg_catalog.pg_trigger t "
        "WHERE t.tgrelid=to_regclass($1) AND NOT t.tgisinternal ORDER BY t.tgname",
        relation,
        storage_relation(schema, "cms_native_input_revision_guard") + "()",
    )
    await connection.execute("SELECT pg_catalog.set_config('search_path',$1,true)", previous_path)
    _require(
        len(guards) == 2
        and {guard["tgname"] for guard in guards} == set(_GUARDS)
        and all(
            guard["tgtype"] == _GUARDS[guard["tgname"]] and guard["enabled"] == "A" and guard["safe"] is True
            for guard in guards
        ),
        "existing_ledger_guards_changed",
    )
    return guards


async def _rewrite_existing_functions(connection, schema):
    bodies_by_name = {
        "cms_native_input_advance": inputs._advance_body(schema),
        "cms_native_input_revision_guard": inputs._ledger_guard_body(),
    }
    previous_by_name = {
        "cms_native_input_advance": bodies_by_name["cms_native_input_advance"].replace(
            "WHERE schema_name=TG_TABLE_SCHEMA AND table_name=TG_TABLE_NAME;", "WHERE relation_oid=TG_RELID::bigint;"
        ),
        "cms_native_input_revision_guard": bodies_by_name["cms_native_input_revision_guard"].replace(
            "AND NEW.schema_name=OLD.schema_name",
            "AND NEW.relation_oid=OLD.relation_oid AND NEW.schema_name=OLD.schema_name",
        ),
    }
    for name, body in bodies_by_name.items():
        function = await connection.fetchrow(
            "SELECT prosrc,pg_get_functiondef(oid) AS definition FROM pg_catalog.pg_proc WHERE oid=to_regprocedure($1)",
            storage_relation(schema, name) + "()",
        )
        _require(
            function is not None and function["prosrc"] in (previous_by_name[name], body),
            "existing_function_body_changed",
        )
        if function["prosrc"] != body:
            definition = function["definition"]
            _require(definition.count(previous_by_name[name]) == 1, "existing_function_definition_changed")
            await connection.execute(definition.replace(previous_by_name[name], body))


async def _verify_named_projection(connection, schema, retained):
    canonical = storage_relation(schema, _TABLE)
    _require(
        await _columns(connection, canonical)
        == (("schema_name", "name"), ("table_name", "name"), ("revision", "bigint"), ("mutation_xid", "xid8")),
        "named_ledger_layout_changed",
    )
    layout = await connection.fetchval(
        "SELECT count(*)=4 AND bool_and(a.attnotnull=(a.attname<>'mutation_xid') "
        "AND coalesce(pg_get_expr(d.adbin,d.adrelid),'')=CASE WHEN a.attname='revision' THEN '0' ELSE '' END "
        "AND a.attidentity='' AND a.attgenerated='') FROM pg_catalog.pg_attribute a "
        "LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum "
        "WHERE a.attrelid=to_regclass($1) AND a.attnum>0 AND NOT a.attisdropped",
        canonical,
    )
    _require(layout is True, "named_ledger_layout_changed")
    nonnegative = await connection.fetchval(
        "SELECT count(*)=1 AND bool_and(convalidated AND NOT condeferrable "
        "AND regexp_replace(pg_get_constraintdef(oid),'[[:space:]()]','','g')='CHECKrevision>=0') "
        "FROM pg_catalog.pg_constraint WHERE conrelid=to_regclass($1) AND contype='c'",
        canonical,
    )
    _require(nonnegative is True, "named_revision_constraint_changed")
    primary = await connection.fetchval(
        "SELECT array_agg(a.attname::text ORDER BY key.position) FROM pg_catalog.pg_constraint c "
        "CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY key(number,position) "
        "JOIN pg_catalog.pg_attribute a ON a.attrelid=c.conrelid AND a.attnum=key.number "
        "WHERE c.conrelid=to_regclass($1) AND c.contype='p' AND c.convalidated AND NOT c.condeferrable",
        canonical,
    )
    _require(tuple(primary or ()) == _COLUMNS[:2], "named_ledger_key_changed")
    fields = ",".join(_COLUMNS)
    same = await connection.fetchval(
        f"SELECT NOT EXISTS((SELECT {fields} FROM {canonical} EXCEPT SELECT {fields} FROM {retained}) "
        f"UNION ALL (SELECT {fields} FROM {retained} EXCEPT SELECT {fields} FROM {canonical}))"
    )
    _require(same is True, "retained_projection_changed")
    ledger = await registration._native_registration_ledger(connection, schema)
    await registration._native_registration_function(connection, schema, ledger)


async def _relocate_ledger(connection, schema, retained_qname):
    original = storage_relation(schema, _TABLE)
    retained_schema, retained_table = retained_qname
    current = original
    if retained_table != _TABLE:
        await connection.execute(f"ALTER TABLE {current} RENAME TO {storage_identifier(retained_table)}")
        current = storage_relation(schema, retained_table)
    if retained_schema != schema:
        await connection.execute(f"ALTER TABLE {current} SET SCHEMA {storage_identifier(retained_schema)}")
    retained = storage_relation(*retained_qname)
    await connection.execute(
        f"CREATE TABLE {original} (schema_name name NOT NULL,table_name name NOT NULL,"
        "revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),mutation_xid xid8,"
        f"CONSTRAINT {_TABLE}_named_pkey PRIMARY KEY(schema_name,table_name))"
    )
    fields = ",".join(_COLUMNS)
    await connection.execute(f"INSERT INTO {original}({fields}) SELECT DISTINCT {fields} FROM {retained}")


async def _relocate_guards(connection, schema, retained_qname, guards):
    original, retained = storage_relation(schema, _TABLE), storage_relation(*retained_qname)
    for guard in guards:
        await connection.execute(f"DROP TRIGGER {storage_identifier(guard['tgname'])} ON {retained}")
        await connection.execute(guard["definition"])
        await connection.execute(f"ALTER TABLE {original} ENABLE ALWAYS TRIGGER {storage_identifier(guard['tgname'])}")


async def _close_configured_access(connection, schema, retained_qname, owners, counter, writers, readers, selected):
    ledger_owner, archive_owner = owners
    retained_schema, retained_table = retained_qname
    await freeze_fhir_source_snapshot(
        connection,
        retained_schema,
        namespace_owner=archive_owner,
        reader_roles=readers,
        application_roles=(),
        relation_names=(retained_table,),
        freeze_schema=False,
    )
    await freeze_fhir_source_snapshot(
        connection,
        schema,
        namespace_owner=ledger_owner,
        reader_roles=selected,
        application_roles=(),
        relation_names=(_TABLE,),
        freeze_schema=False,
    )
    canonical = storage_relation(schema, _TABLE)
    await connection.execute(
        f"GRANT INSERT(schema_name,table_name) ON TABLE {canonical} TO " + ",".join(map(storage_identifier, writers))
    )
    await connection.execute(
        f"GRANT UPDATE(revision,mutation_xid) ON TABLE {canonical} TO {storage_identifier(counter)}"
    )


async def _replay_preparation(connection, schema, retained):
    _require(await _columns(connection, retained) == _LEGACY_COLUMNS, "retained_layout_changed")
    counts = await _legacy_projection(connection, retained)
    _require(counts["raw_rows"] > counts["named_rows"], "equivalent_duplicates_required")
    _require(
        not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_catalog.pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal)",
            retained,
        ),
        "retained_guards_remain",
    )
    await _verify_named_projection(connection, schema, retained)
    return counts


async def _prepare_legacy(connection, schema, retained_qname, columns, closure, assert_idle):
    canonical, retained = storage_relation(schema, _TABLE), storage_relation(*retained_qname)
    _require(columns == _LEGACY_COLUMNS, "legacy_ledger_layout_changed")
    counts = await _legacy_projection(connection, canonical)
    _require(counts["raw_rows"] > counts["named_rows"], "equivalent_duplicates_required")
    guards = await _guard_definitions(connection, canonical, schema)
    await assert_idle(connection, schema)
    await _rewrite_existing_functions(connection, schema)
    await _relocate_ledger(connection, schema, retained_qname)
    await _close_configured_access(connection, schema, retained_qname, **closure)
    await _relocate_guards(connection, schema, retained_qname, guards)
    await _verify_named_projection(connection, schema, retained)
    return counts


async def _require_native_admin(connection, selected, owners):
    actor = await connection.fetchrow(
        "SELECT session_user::text AS session,current_user::text AS current,"
        "(SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user) AS native_admin"
    )
    _require(
        actor["session"] == actor["current"]
        and actor["native_admin"] is True
        and actor["current"] not in (*selected, *owners),
        "independent_admin_required",
    )


async def _prepare_locked_ledger(connection, schema, retained_qname, closure, assert_idle):
    canonical, retained = storage_relation(schema, _TABLE), storage_relation(*retained_qname)
    await connection.execute(f"LOCK TABLE {canonical} IN ACCESS EXCLUSIVE MODE NOWAIT")
    exists = await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", retained)
    columns = await _columns(connection, canonical)
    if exists:
        _require(columns and columns[0][0] == "schema_name", "retained_name_already_exists")
        await connection.execute(f"LOCK TABLE {retained} IN ACCESS SHARE MODE NOWAIT")
        counts = await _replay_preparation(connection, schema, retained)
        # A retry applies the requested owners and grants again, so a corrected role takes effect.
        await _close_configured_access(connection, schema, retained_qname, **closure)
        status = "already_prepared"
    else:
        counts = await _prepare_legacy(connection, schema, retained_qname, columns, closure, assert_idle)
        status = "prepared"
    return dict(
        status=status,
        ledger_qname=(schema, _TABLE),
        retained_qname=retained_qname,
        raw_rows=counts["raw_rows"],
        named_rows=counts["named_rows"],
    )


async def prepare_cms_native_input_metadata_transition(
    connection,
    *,
    ledger_schema,
    retained_qname,
    owner_roles,
    counter_role,
    writer_roles,
    reader_roles,
    assert_idle,
):
    """Retain equivalent duplicate rows and prepare the named key in the caller's admin transaction."""
    canonical = storage_relation(ledger_schema, _TABLE)
    _require(
        isinstance(owner_roles, Mapping) and set(owner_roles) == {"ledger", "archive"},
        "configured_owner_roles_required",
    )
    ledger_owner_role, archive_owner_role = owner_roles["ledger"], owner_roles["archive"]
    _require(isinstance(retained_qname, tuple) and len(retained_qname) == 2, "retained_qname_required")
    retained = storage_relation(*retained_qname)
    _require(retained != canonical and callable(assert_idle), "explicit_retention_and_idle_required")
    selected = _roles(writer_roles, reader_roles, ledger_owner_role, archive_owner_role, counter_role)
    _require(connection.is_in_transaction() is True, "existing_admin_transaction_required")
    await _require_native_admin(connection, selected, (ledger_owner_role, archive_owner_role))
    async with connection.transaction():
        previous_timeout = await connection.fetchval("SHOW lock_timeout")
        await connection.execute("SET LOCAL lock_timeout='5s'")
        await assert_idle(connection, ledger_schema)
        closure_by_role = dict(
            owners=(ledger_owner_role, archive_owner_role),
            counter=counter_role,
            writers=writer_roles,
            readers=reader_roles,
            selected=selected,
        )
        preparation_result = await _prepare_locked_ledger(
            connection, ledger_schema, retained_qname, closure_by_role, assert_idle
        )
        await assert_idle(connection, ledger_schema)
        await connection.execute("SELECT set_config('lock_timeout',$1,true)", previous_timeout)
    return preparation_result
