# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewed trigger-only authority for a bounded address-alias generation fence."""

from __future__ import annotations

import hashlib

from sqlalchemy import text

from process import entity_address_snapshot_alias as alias

IMMUTABLE_SOURCE_SHA256 = "2bddc86190d746a057c6b941b08d9632bccdb52298f224192a399395f3207d8f"
REVOCATION_COLUMNS = ("revoked_at", "revoked_reason", "revoked_by", "revoke_run_id", "updated_at")
STATE_CHECK_CONTRACT = (
    ("address_alias_state_v1_generation_ck", "CHECK ((generation >= 0))", True, False, False),
    ("address_alias_state_v1_ruleset_ck", "CHECK ((active_ruleset_version = 1))", True, False, False),
    ("address_alias_state_v1_schema_ck", "CHECK ((schema_version = 2))", True, False, False),
    ("address_alias_state_v1_singleton_ck", "CHECK (singleton)", True, False, False),
)
STATE_DEFAULT_CONTRACT = (
    ("singleton", "true"),
    ("schema_version", "2"),
    ("active_ruleset_version", "1"),
    ("generation", "0"),
    ("updated_at", "now()"),
)
GUARDS = {
    "addr_alias_immutable_v1": ("address_alias_v1_immutable_trg", 27, None, None, False),
    "addr_alias_generation_after_insert_v1": (
        "address_alias_v1_generation_insert_trg",
        4,
        None,
        "new_alias_rows",
        True,
    ),
    "addr_alias_generation_after_update_v1": (
        "address_alias_v1_generation_update_trg",
        16,
        "old_alias_rows",
        "new_alias_rows",
        True,
    ),
    "addr_alias_generation_after_delete_v1": (
        "address_alias_v1_generation_delete_trg",
        8,
        "old_alias_rows",
        None,
        True,
    ),
}


def generation_function_bodies(schema):
    """Return fixed reviewed bodies with only a validated local namespace substituted."""
    schema = alias._schema_name(schema)
    predicates_by_event = {
        "insert": "SELECT 1 FROM new_alias_rows WHERE revoked_at IS NULL",
        "delete": "SELECT 1 FROM old_alias_rows WHERE revoked_at IS NULL",
        "update": """SELECT 1 FROM old_alias_rows old_row JOIN new_alias_rows new_row USING (alias_id)
            WHERE (old_row.revoked_at IS NULL OR new_row.revoked_at IS NULL)
            AND ROW(old_row.source_address_key,old_row.source_identity_key,old_row.target_address_key,
                    old_row.target_identity_key,old_row.alias_kind,old_row.ruleset_version,old_row.revoked_at)
                IS DISTINCT FROM
                ROW(new_row.source_address_key,new_row.source_identity_key,new_row.target_address_key,
                    new_row.target_identity_key,new_row.alias_kind,new_row.ruleset_version,new_row.revoked_at)""",
    }
    return {
        f"addr_alias_generation_after_{event}_v1": f"""
BEGIN
    IF EXISTS ({predicate}) THEN
        PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('address_numeric_grid_alias_v1'));
        UPDATE "{schema}"."address_alias_state_v1"
           SET generation = generation + 1, updated_at = pg_catalog.now()
         WHERE singleton IS TRUE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'address alias singleton state is missing' USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NULL;
END;
"""
        for event, predicate in predicates_by_event.items()
    }


def generation_guard_statements(schema):
    """Install code only; protected ownership and ordinary ACL provisioning are explicit."""
    schema = alias._schema_name(schema)
    statements = [
        f'CREATE OR REPLACE FUNCTION "{schema}"."{name}"() RETURNS trigger LANGUAGE plpgsql '
        f"SECURITY DEFINER SET search_path=pg_catalog AS $guard${body}$guard$"
        for name, body in generation_function_bodies(schema).items()
    ]
    statements.append(
        f'ALTER FUNCTION "{schema}".addr_alias_immutable_v1() SECURITY INVOKER SET search_path=pg_catalog'
    )
    for name, (trigger, *_shape) in GUARDS.items():
        statements.append(f'REVOKE ALL ON FUNCTION "{schema}"."{name}"() FROM PUBLIC')
        statements.append(f'ALTER TABLE "{schema}".address_alias_v1 ENABLE ALWAYS TRIGGER "{trigger}"')
    return tuple(statements)


def _require(condition, reason):
    if not condition:
        raise alias.EntityAddressSnapshotAliasError("entity-address alias guard " + reason)


async def _relation_catalog(session, schema):
    catalog_rows = (
        (
            await session.execute(
                text(
                    "SELECT c.oid,c.relname,c.relkind::text,c.relpersistence::text,c.relrowsecurity,c.relforcerowsecurity,"
                    "c.relispartition,"
                    "EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite r WHERE r.ev_class=c.oid) "
                    "OR EXISTS(SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhrelid=c.oid OR i.inhparent=c.oid) AS unsafe "
                    "FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=:schema AND c.relname=ANY(:names) ORDER BY c.relname"
                ),
                {"schema": schema, "names": [alias._ALIAS_TABLE, alias._STATE_TABLE]},
            )
        )
        .mappings()
        .all()
    )
    _require(
        len(catalog_rows) == 2
        and {catalog_row["relname"] for catalog_row in catalog_rows} == {alias._ALIAS_TABLE, alias._STATE_TABLE}
        and all(
            catalog_row["relkind"] == "r"
            and catalog_row["relpersistence"] == "p"
            and not any(
                catalog_row[field] for field in ("relrowsecurity", "relforcerowsecurity", "relispartition", "unsafe")
            )
            for catalog_row in catalog_rows
        ),
        "relation authority is unavailable",
    )
    for name, columns, key in (
        (alias._STATE_TABLE, alias._STATE_COLUMNS, "singleton"),
        (alias._ALIAS_TABLE, alias._ALIAS_COLUMNS, "alias_id"),
    ):
        await alias._require_relation_shape(
            session, schema_name=schema, table_name=name, expected_columns=columns, expected_primary_key=(key,)
        )
    return catalog_rows


async def _require_state_expressions(session, state_oid):
    """A trigger's privileged state update must not enter ordinary executable code."""
    await _require_state_expression_contract(session, state_oid)
    unsafe = await session.scalar(
        text(
            "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid "
            "WHERE a.attrelid=:oid AND a.attnum>0 AND NOT a.attisdropped AND "
            "(t.typnamespace<>'pg_catalog'::regnamespace OR a.attgenerated<>'' OR a.attidentity<>'')) "
            "OR EXISTS(SELECT 1 FROM pg_catalog.pg_constraint c WHERE c.conrelid=:oid AND c.contype NOT IN ('p','n','c')) "
            "OR EXISTS(SELECT 1 FROM pg_catalog.pg_depend d WHERE "
            "((d.classid='pg_constraint'::regclass AND d.objid IN (SELECT oid FROM pg_catalog.pg_constraint WHERE conrelid=:oid)) "
            "OR (d.classid='pg_attrdef'::regclass AND d.objid IN (SELECT oid FROM pg_catalog.pg_attrdef WHERE adrelid=:oid))) "
            "AND ((d.refclassid='pg_proc'::regclass AND d.refobjid IN "
            "(SELECT oid FROM pg_catalog.pg_proc WHERE pronamespace<>'pg_catalog'::regnamespace)) "
            "OR (d.refclassid='pg_operator'::regclass AND d.refobjid IN "
            "(SELECT oid FROM pg_catalog.pg_operator WHERE oprnamespace<>'pg_catalog'::regnamespace)))) "
            "OR EXISTS(SELECT 1 FROM pg_catalog.pg_index i,pg_catalog.unnest(i.indclass) cls(oid) "
            "JOIN pg_catalog.pg_opclass op ON op.oid=cls.oid WHERE i.indrelid=:oid "
            "AND (op.opcnamespace<>'pg_catalog'::regnamespace OR i.indexprs IS NOT NULL OR i.indpred IS NOT NULL))"
        ),
        {"oid": state_oid},
    )
    _require(unsafe is False, "state expressions are unsupported")


async def _require_state_expression_contract(session, state_oid):
    checks = (
        await session.execute(
            text(
                "SELECT conname,pg_catalog.pg_get_constraintdef(oid),convalidated,condeferrable,condeferred "
                "FROM pg_catalog.pg_constraint WHERE conrelid=:oid AND contype='c' ORDER BY conname"
            ),
            {"oid": state_oid},
        )
    ).all()
    defaults = (
        await session.execute(
            text(
                "SELECT a.attname,pg_catalog.pg_get_expr(d.adbin,d.adrelid) "
                "FROM pg_catalog.pg_attrdef d JOIN pg_catalog.pg_attribute a "
                "ON a.attrelid=d.adrelid AND a.attnum=d.adnum "
                "WHERE d.adrelid=:oid ORDER BY a.attnum"
            ),
            {"oid": state_oid},
        )
    ).all()
    _require(
        tuple(checks) == STATE_CHECK_CONTRACT and tuple(defaults) == STATE_DEFAULT_CONTRACT,
        "state expressions are unsupported",
    )


async def _trigger_catalog(session, schema, relation_oids):
    catalog_rows = (
        (
            await session.execute(
                text(
                    "SELECT t.oid,t.tgname,t.tgtype,t.tgenabled::text,t.tgoldtable,t.tgnewtable,c.relname,"
                    "p.oid AS function_oid,p.proname,p.prosecdef,p.prosrc,"
                    "t.tgconstraint=0 AND NOT t.tgdeferrable AND NOT t.tginitdeferred AND t.tgnargs=0 "
                    "AND t.tgargs=''::bytea AND t.tgattr::text='' AND t.tgqual IS NULL "
                    "AND p.pronamespace=c.relnamespace AND p.prokind='f' "
                    "AND p.pronargs=0 AND p.pronargdefaults=0 AND p.prorettype='pg_catalog.trigger'::regtype "
                    "AND NOT p.proretset AND p.provolatile='v' AND p.proparallel='u' AND NOT p.proleakproof "
                    "AND p.proconfig=ARRAY['search_path=pg_catalog']::text[] AND l.lanname='plpgsql' AS valid "
                    "FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid "
                    "JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid JOIN pg_catalog.pg_language l ON l.oid=p.prolang "
                    "WHERE t.tgrelid=ANY(:oids) AND NOT t.tgisinternal ORDER BY t.tgname"
                ),
                {"oids": relation_oids},
            )
        )
        .mappings()
        .all()
    )
    expected_by_function = {
        name: hashlib.sha256(body.encode()).hexdigest() for name, body in generation_function_bodies(schema).items()
    }
    expected_by_function["addr_alias_immutable_v1"] = IMMUTABLE_SOURCE_SHA256
    _require(
        len(catalog_rows) == 4 and {catalog_row["proname"] for catalog_row in catalog_rows} == set(GUARDS),
        "trigger inventory differs",
    )
    for catalog_row in catalog_rows:
        actual = (
            catalog_row["tgname"],
            catalog_row["tgtype"],
            catalog_row["tgoldtable"],
            catalog_row["tgnewtable"],
            catalog_row["prosecdef"],
        )
        _require(
            catalog_row["valid"] is True
            and catalog_row["tgenabled"] == "A"
            and catalog_row["relname"] == alias._ALIAS_TABLE
            and actual == GUARDS[catalog_row["proname"]]
            and hashlib.sha256(catalog_row["prosrc"].encode()).hexdigest()
            == expected_by_function[catalog_row["proname"]],
            "trigger authority differs",
        )
    return [dict(catalog_row) for catalog_row in catalog_rows]


async def require_entity_address_alias_guard(session, *, schema):
    """Check relation shape, state expressions and guard bodies without mutating anything."""
    schema = alias._schema_name(schema)
    catalog_rows = await _relation_catalog(session, schema)
    table_oid_by_name = {row["relname"]: row["oid"] for row in catalog_rows}
    await _require_state_expressions(session, table_oid_by_name[alias._STATE_TABLE])
    await _trigger_catalog(session, schema, list(table_oid_by_name.values()))


async def lock_entity_address_alias_capture(session, *, schema):
    """Use installed writer guards for SELECT-only capture; otherwise retain legacy SHARE."""
    schema = alias._schema_name(schema)
    await alias._lock_alias_relations(session, schema, provisional=True)
    # The alias migration created this function; only the guard migration makes it SECURITY DEFINER.
    is_guarded = await session.scalar(
        text(
            "SELECT COALESCE((SELECT p.prosecdef FROM pg_catalog.pg_proc p "
            "WHERE p.oid=pg_catalog.to_regprocedure(:function)),false)"
        ),
        {"function": f'"{schema}".addr_alias_generation_after_insert_v1()'},
    )
    if is_guarded:
        await require_entity_address_alias_guard(session, schema=schema)
    else:
        await alias._lock_alias_relations(session, schema)
