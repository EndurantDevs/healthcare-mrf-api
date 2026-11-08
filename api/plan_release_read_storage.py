# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain authorized legacy reads between structural migration and provisioning."""

import re
from datetime import datetime

from sanic.exceptions import ServiceUnavailable
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.network_read_execution import install_network_read_inventory
from db.plan_release_storage import BINDING_TABLE, LEGACY_BINDING_TABLE, PIN_TABLE, REVISION_TABLE, storage_relation
from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import NetworkReadIdentityError, verify_bound_network_names
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_custody import _document, _require, _verify_header, _verify_payload


def _api_catalog_expression(expression):
    """Deparse only ordinary column Vars against the fixed catalog namespace.

    PostgreSQL opens the supplied relation even for metadata-only deparsing.
    The catalog already read by this query supplies names for the same positive
    attribute numbers; the returned alias map restores the real column names.
    """
    variable_pattern = r"[{]VAR :varno ([0-9]+) :varattno (-?[0-9]+) [^{}]* :varlevelsup ([0-9]+) [^{}]*[}]"
    variables = f"regexp_matches({expression}::text, '{variable_pattern}', 'g')"
    return f"""CASE WHEN {expression} IS NULL THEN NULL
 WHEN (SELECT count(*) FROM {variables})=regexp_count({expression}::text,'[{{]VAR ')
 AND NOT EXISTS(SELECT FROM {variables} variable(fields)
  WHERE fields[1]<>'1' OR fields[2]::integer<=0 OR fields[3]<>'0' OR NOT EXISTS(
   SELECT FROM pg_attribute actual JOIN pg_attribute alias ON alias.attnum=actual.attnum
   WHERE actual.attrelid=c.oid AND actual.attnum=fields[2]::integer AND NOT actual.attisdropped
    AND alias.attrelid='pg_catalog.pg_class'::regclass AND NOT alias.attisdropped))
 THEN pg_get_expr({expression},'pg_catalog.pg_class'::regclass)
 ELSE 'invalid_catalog_expression' END"""


def _api_catalog_sql():
    """Keep custody checks while avoiding deparser locks on application tables."""
    replacement_by_expression = {
        "pg_get_expr(c.relpartbound,c.oid)": "pg_get_expr(c.relpartbound,0)",
        "pg_get_expr(d.adbin,d.adrelid)": "pg_get_expr(d.adbin,0)",
        "pg_get_expr(i.indpred,i.indrelid)": _api_catalog_expression("i.indpred"),
        "pg_get_expr(i.indexprs,i.indrelid)": "CASE WHEN i.indexprs IS NULL THEN NULL ELSE 'expression' END",
        "pg_get_constraintdef(oid)": (
            "CASE WHEN contype='c' THEN 'CHECK ('||"
            + _api_catalog_expression("conbin")
            + "||')'||CASE WHEN connoinherit THEN ' NO INHERIT' ELSE '' END ELSE NULL END"
        ),
        "'valid',convalidated,": (
            "'valid',convalidated AND NOT condeferrable AND NOT condeferred "
            "AND coalesce((to_jsonb(pg_constraint)->>'conenforced')::boolean,true),"
        ),
        "pg_get_partkeydef(c.oid)": """(SELECT 'LIST (serving_revision_id)' FROM pg_partitioned_table p
 JOIN pg_attribute a ON a.attrelid=p.partrelid AND a.attnum=p.partattrs[0]
 JOIN pg_opclass op ON op.oid=p.partclass[0] JOIN pg_am am ON am.oid=op.opcmethod
 WHERE p.partrelid=c.oid AND p.partstrat='l' AND p.partnatts=1 AND p.partexprs IS NULL
 AND a.attname='serving_revision_id' AND NOT a.attisdropped AND p.partcollation[0]=a.attcollation
 AND op.opcnamespace='pg_catalog'::regnamespace AND op.opcname='text_ops' AND am.amname='btree')""",
        "c.relowner::bigint,c.relrowsecurity,c.relforcerowsecurity,": """c.relowner::bigint,c.relrowsecurity,c.relforcerowsecurity,
 (SELECT jsonb_object_agg(alias.attname,actual.attname)
  FROM pg_attribute actual JOIN pg_attribute alias ON alias.attnum=actual.attnum
  WHERE actual.attrelid=c.oid AND actual.attnum>0 AND NOT actual.attisdropped
  AND alias.attrelid='pg_catalog.pg_class'::regclass AND NOT alias.attisdropped) AS expression_aliases,""",
    }
    sql = CATALOG_SQL
    for original, replacement in replacement_by_expression.items():
        if sql.count(original) != 1:
            raise ValueError("plan_release_storage_catalog_contract_changed")
        sql = sql.replace(original, replacement)
    return sql


_API_CATALOG_SQL = _api_catalog_sql()


def _restore_catalog_columns(expression, alias_map):
    """Restore bare column tokens without rewriting literals, types or functions."""
    if expression is None:
        return None
    tokens = r"'(?:''|\\.|[^'\\])*'|\"(?:\"\"|[^\"])*\"|(?<![\w$:.])[a-z_][a-z_0-9$]*(?![\w$]|\s*[.(])"
    return re.sub(tokens, lambda token: alias_map.get(token[0], token[0]), expression)


def _api_catalog_family(rows):
    """Restore the existing verifier's expression vocabulary, keeping every guard."""
    family_rows = []
    for row in rows:
        relation_by_field = dict(row)
        alias_map = _document(relation_by_field.pop("expression_aliases"))
        relation_by_field["constraints"] = _document(relation_by_field["constraints"])
        for constraint in relation_by_field["constraints"]:
            if constraint["type"] == "c":
                constraint["definition"] = _restore_catalog_columns(constraint["definition"], alias_map)
        relation_by_field["indexes"] = _document(relation_by_field["indexes"])
        for index in relation_by_field["indexes"]:
            index["predicate"] = _restore_catalog_columns(index["predicate"], alias_map)
        family_rows.append(relation_by_field)
    return family_rows


_ACCESS_SQL = """SELECT c.relname,c.oid::bigint,c.relowner::bigint,r.oid::bigint AS reader_oid,
 has_table_privilege(r.oid,c.oid,'SELECT') AS readable,
 current_user=session_user AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole
   OR r.rolreplication OR r.rolbypassrls)
   AND NOT pg_has_role(r.oid,c.relowner,'SET')
   AND NOT has_schema_privilege(r.oid,c.relnamespace,'CREATE') AS ordinary_reader,
 NOT EXISTS(SELECT FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
   WHERE a.grantee<>c.relowner)
   AND NOT EXISTS(SELECT FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) grant_row
     WHERE a.attrelid=c.oid AND grant_row.grantee<>c.relowner) AS owner_only
 FROM pg_class c JOIN pg_roles r ON r.rolname=current_user
 WHERE c.oid=ANY(ARRAY(SELECT to_regclass(name)::oid FROM unnest($1::text[]) names(name)))
 ORDER BY c.relname"""

_RETAINED_READERS_SQL = """WITH retained_relations AS (
 SELECT to_regclass(name)::oid AS oid FROM unnest($1::text[]) names(name)
), readers(oid) AS (
 SELECT $3::oid
 UNION SELECT a.grantee FROM retained_relations retained JOIN pg_class c ON c.oid=retained.oid
 CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
 WHERE a.grantee<>$2::oid
 UNION SELECT grant_row.grantee FROM retained_relations retained
 JOIN pg_attribute field ON field.attrelid=retained.oid
 CROSS JOIN LATERAL aclexplode(field.attacl) grant_row WHERE grant_row.grantee<>$2::oid
)
SELECT r.oid::bigint,
 NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls)
 AND NOT pg_has_role(r.oid,$2::oid,'SET')
 AND NOT EXISTS(SELECT FROM retained_relations retained JOIN pg_class c ON c.oid=retained.oid
   WHERE NOT has_table_privilege(r.oid,c.oid,'SELECT')
   OR has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
   OR has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES')
   OR has_schema_privilege(r.oid,c.relnamespace,'CREATE')) AS ordinary_reader
 FROM readers JOIN pg_roles r ON r.oid=readers.oid ORDER BY r.oid"""

_LEGACY_AUTHORITY_SQL = """SELECT
 (SELECT count(*)=1 AND bool_and(k.convalidated AND k.conenforced AND NOT k.condeferrable
   AND k.confrelid=header.oid AND k.confdeltype='c' AND k.confupdtype='a' AND k.confmatchtype='s'
   AND k.conkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=legacy.oid
     AND attname='serving_revision_id')]::smallint[]
   AND k.confkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=header.oid
     AND attname='serving_revision_id')]::smallint[])
  FROM pg_constraint k WHERE k.conrelid=legacy.oid AND k.contype='f') AS original_fk,
 NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=legacy.oid) AS no_rewrite,
 pin.relkind='r' AND pin.relpersistence='p' AND pin.relowner=legacy.relowner
   AND NOT pin.relrowsecurity AND NOT pin.relforcerowsecurity
   AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=pin.oid OR inhparent=pin.oid)
   AND has_table_privilege(current_user,pin.oid,'SELECT')
   AND NOT has_table_privilege(current_user,pin.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
   AND NOT has_any_column_privilege(current_user,pin.oid,'INSERT,UPDATE,REFERENCES') AS old_pin_reader
 FROM pg_class legacy,pg_class header,pg_class pin
 WHERE legacy.oid=to_regclass($1) AND header.oid=to_regclass($2) AND pin.oid=to_regclass($3)"""


def _verify_retained_family(family, access):
    """The only admitted retained state has one unchanged, bounded legacy leaf."""
    by_name = {relation["relname"]: relation for relation in family}
    _require(len(family) == len(by_name) == 4, "retained_family_changed")
    _require(set(by_name) == {BINDING_TABLE, PIN_TABLE, REVISION_TABLE, LEGACY_BINDING_TABLE}, "retained_roots_changed")
    owner = access[0]["relowner"]
    for relation in family:
        _require(
            relation["relowner"] == owner
            and relation["relpersistence"] == "p"
            and not relation["relrowsecurity"]
            and not relation["relforcerowsecurity"]
            and relation["native_checks"]
            and relation["direct_acl_closed"],
            "retained_custody_changed",
        )
    for name in (BINDING_TABLE, PIN_TABLE):
        root = by_name[name]
        _require(
            root["relkind"] == "p"
            and not root["parents"]
            and root["inert"]
            and root["partition_key"] == "LIST (serving_revision_id)",
            "retained_parent_changed",
        )
        _verify_payload(root, pin=name == PIN_TABLE)
    header = by_name[REVISION_TABLE]
    _require(header["readers_closed"], "retained_header_reader_changed")
    _verify_header(header)
    legacy = by_name[LEGACY_BINDING_TABLE]
    _require(
        legacy["relkind"] == "r"
        and legacy["relispartition"]
        and legacy["parents"] == [by_name[BINDING_TABLE]["oid"]]
        and legacy["readers_closed"],
        "retained_attachment_changed",
    )
    constraints = _document(legacy["constraints"])
    _verify_payload(
        {**legacy, "constraints": [constraint for constraint in constraints if constraint["type"] != "f"]}, pin=False
    )
    literal = r"'hpserve_[0-9A-HJKMNP-TV-Z]{26}'"
    bound = legacy["bound"] or ""
    checks = [constraint for constraint in constraints if constraint["name"] == "plan_release_legacy_cohort_bound"]
    _require(
        re.fullmatch(rf"FOR VALUES IN \({literal}(?:, {literal})*\)", bound) is not None
        and len(checks) == 1
        and checks[0]["valid"]
        and set(re.findall(literal, checks[0]["definition"])) == set(re.findall(literal, bound)),
        "retained_bound_changed",
    )


async def is_retained_binding_read(fetch, schema):
    """Choose legacy storage only from positive catalog evidence, never a SQL error."""
    roots = [storage_relation(schema, name) for name in (BINDING_TABLE, PIN_TABLE)]
    access = await fetch(_ACCESS_SQL, roots)
    _require(len(access) == 2, "read_roots_missing")
    if all(root_access["readable"] is True for root_access in access):
        return False
    _require(
        all(
            root_access["readable"] is False and root_access["owner_only"] and root_access["ordinary_reader"]
            for root_access in access
        )
        and access[0]["relowner"] == access[1]["relowner"],
        "retained_read_not_admitted",
    )
    legacy = storage_relation(schema, LEGACY_BINDING_TABLE)
    header = storage_relation(schema, REVISION_TABLE)
    old_pin = storage_relation(schema, "ptg2_snapshot_pin")
    # Hold the original relations through encoding; administrator provisioning must wait.
    await fetch(f"SELECT 1 FROM ONLY {legacy}, ONLY {header}, ONLY {old_pin} LIMIT 0")
    access = await fetch(_ACCESS_SQL, roots)
    _require(
        all(
            root_access["owner_only"] and root_access["ordinary_reader"] and not root_access["readable"]
            for root_access in access
        ),
        "retained_read_changed",
    )
    readers = await fetch(_RETAINED_READERS_SQL, [legacy, header], access[0]["relowner"], access[0]["reader_oid"])
    _require(readers and all(reader["ordinary_reader"] for reader in readers), "retained_reader_cohort_changed")
    family = await fetch(
        CATALOG_SQL, [*roots, legacy, header], access[0]["relowner"], [reader["oid"] for reader in readers]
    )
    _verify_retained_family(family, access)
    authority = await fetch(_LEGACY_AUTHORITY_SQL, legacy, header, old_pin)
    _require(len(authority) == 1 and all(authority[0].values()), "retained_legacy_authority_changed")
    return True


async def is_retained_session_read(session, schema, *, api_read=False):
    """Use the same decision for the ORM request Reader and native bulk metadata."""
    _require(type(api_read) is bool, "api_read_mode_invalid")
    if api_read:
        return await _api_retained_session_read(session, schema)

    async def fetch(sql, *parameters):
        """Execute the fixed catalog queries through this exact ORM connection."""
        is_catalog = sql == CATALOG_SQL
        sql = re.sub(r"\$(\d+)", lambda match: ":p" + match[1], sql)
        # SQLAlchemy named binds need an explicit CAST rather than PostgreSQL's :: suffix.
        sql = re.sub(r"(:p\d+)::([a-z]+(?:\[\])?)", r"CAST(\1 AS \2)", sql)
        parameter_map = {"p" + str(index): value for index, value in enumerate(parameters, 1)}
        if is_catalog:
            return await _catalog_session_rows(session, text(sql), parameter_map)
        rows = await session.execute(text(sql), parameter_map)
        return [dict(row._mapping) for row in rows]

    return await is_retained_binding_read(fetch, schema)


async def _catalog_session_rows(session, statement, parameters):
    """Avoid catalog JIT while preserving the caller's settings and read locks."""
    # A connection savepoint does not flush pending ORM writes or replace an
    # existing session savepoint. Release on success retains catalog read locks.
    connection = await session.connection() if isinstance(session, AsyncSession) else session
    savepoint = await connection.begin_nested()
    try:
        previous_jit = await connection.scalar(text("SHOW jit"))
        setting = await connection.execute(text("SET LOCAL jit = off"))
        setting.close()
        catalog_result = await connection.execute(statement, parameters)
        try:
            catalog_rows = [dict(catalog_row._mapping) for catalog_row in catalog_result]
        finally:
            catalog_result.close()
        restored = await connection.scalar(
            text("SELECT set_config('jit', :previous_jit, true)"), {"previous_jit": previous_jit}
        )
        if restored != previous_jit:
            raise RuntimeError("Catalog JIT setting was not restored")
        await savepoint.commit()
        return catalog_rows
    except BaseException as failure:
        try:
            await savepoint.rollback()
        except BaseException as rollback_failure:
            failure.add_note("Catalog savepoint cleanup failed: " + type(rollback_failure).__name__)
            try:
                await connection.invalidate()
            except BaseException as invalidation_failure:
                failure.add_note("Catalog connection invalidation failed: " + type(invalidation_failure).__name__)
        raise


_API_SNAPSHOT_SQL = """SELECT pg_catalog.pg_backend_pid() AS backend_pid,
 pg_catalog.current_setting('transaction_isolation') AS isolation,
 pg_catalog.current_setting('transaction_read_only') AS read_only,
 pg_catalog.transaction_timestamp() AS started_at"""
_API_AUTHORITY_SQL = (
    "SELECT proof.*,pin.oid::bigint AS pin_oid FROM ("
    + _LEGACY_AUTHORITY_SQL
    + ") proof JOIN pg_catalog.pg_class pin ON pin.oid=pg_catalog.to_regclass($3)"
)


async def _api_native_snapshot(connection, expected=None):
    """Check caller-owned RRRO; the timestamp detects drift, not unique identity."""
    _require(connection.is_in_transaction() is True, "retained_snapshot_required")
    backend_pid = connection.get_server_pid()
    _require(type(backend_pid) is int and 0 < backend_pid <= 2147483647, "retained_backend_invalid")
    snapshot = await connection.fetchrow(_API_SNAPSHOT_SQL)
    _require(
        snapshot is not None
        and snapshot["backend_pid"] == backend_pid
        and (snapshot["isolation"], snapshot["read_only"]) in (("repeatable read", "on"), ("serializable", "on"))
        and type(snapshot["started_at"]) is datetime
        and snapshot["started_at"].utcoffset() is not None
        and connection.is_in_transaction() is True
        and connection.get_server_pid() == backend_pid,
        "retained_snapshot_changed",
    )
    identity = backend_pid, snapshot["started_at"]
    _require(expected is None or identity == expected, "retained_snapshot_changed")
    return identity


def _api_retained_pins(schema, family, authority):
    """Describe direct heaps; authenticated parents remain metadata, not routing pins."""
    by_name = {relation["relname"]: relation for relation in family}
    pins = []
    for name in (LEGACY_BINDING_TABLE, REVISION_TABLE):
        relation = by_name[name]
        _require(relation["nspname"] == schema, "retained_namespace_changed")
        _require(type(relation["oid"]) is int and 0 < relation["oid"] < 2**32, "retained_identity_changed")
        pins.append(CompactAddressRelation(schema, name, relation["oid"]))
    _require(type(authority["pin_oid"]) is int and 0 < authority["pin_oid"] < 2**32, "retained_identity_changed")
    pins.append(CompactAddressRelation(schema, "ptg2_snapshot_pin", authority["pin_oid"]))
    return tuple(pins)


async def read_retained_binding_metadata(connection, schema):
    """Return native metadata, preserving cancellation and normal read refusal."""
    try:
        return await _read_retained_binding_metadata(connection, schema)
    except ValueError:
        raise
    except Exception as error:
        raise ValueError("plan_release_storage_retained_metadata_unavailable") from error


async def _read_retained_binding_metadata(connection, schema):
    """Resolve metadata on the caller's exact native RRRO transaction, without locks.

    The caller must hold that original transaction through guarded payload reads.
    True pins cover ONLY/direct legacy, header and old-pin heaps; they establish
    no mutable routing or Source authority. False is access-only with no pins.
    """
    snapshot = await _api_native_snapshot(connection)
    roots = [storage_relation(schema, name) for name in (BINDING_TABLE, PIN_TABLE)]
    access = await connection.fetch(_ACCESS_SQL, roots)
    _require(len(access) == 2, "read_roots_missing")
    if all(root_access["readable"] is True for root_access in access):
        await _api_native_snapshot(connection, snapshot)
        return False, ()
    _require(
        all(
            root_access["readable"] is False and root_access["owner_only"] and root_access["ordinary_reader"]
            for root_access in access
        )
        and access[0]["relowner"] == access[1]["relowner"],
        "retained_read_not_admitted",
    )
    legacy = storage_relation(schema, LEGACY_BINDING_TABLE)
    header = storage_relation(schema, REVISION_TABLE)
    old_pin = storage_relation(schema, "ptg2_snapshot_pin")
    access = await connection.fetch(_ACCESS_SQL, roots)
    _require(
        len(access) == 2
        and all(root["owner_only"] and root["ordinary_reader"] and not root["readable"] for root in access),
        "retained_read_changed",
    )
    readers = await connection.fetch(
        _RETAINED_READERS_SQL, [legacy, header], access[0]["relowner"], access[0]["reader_oid"]
    )
    _require(readers and all(reader["ordinary_reader"] for reader in readers), "retained_reader_cohort_changed")
    family = _api_catalog_family(
        await connection.fetch(
            _API_CATALOG_SQL, [*roots, legacy, header], access[0]["relowner"], [reader["oid"] for reader in readers]
        )
    )
    _verify_retained_family(family, access)
    authority = await connection.fetch(_API_AUTHORITY_SQL, legacy, header, old_pin)
    _require(
        len(authority) == 1
        and all(authority[0][field] is True for field in ("original_fk", "no_rewrite", "old_pin_reader")),
        "retained_legacy_authority_changed",
    )
    pins = _api_retained_pins(schema, family, authority[0])
    try:
        await verify_bound_network_names(connection, pins, expected_backend_pid=snapshot[0])
    except NetworkReadIdentityError as error:
        raise ValueError("plan_release_storage_retained_identity_changed") from error
    await _api_native_snapshot(connection, snapshot)
    return True, pins


async def _api_session_continuity(session, connection, transaction, backend_pid):
    """Keep metadata and inventory inside the original ORM session and transaction."""
    try:
        actual = (await (await session.connection()).get_raw_connection()).driver_connection
        _require(
            session.in_transaction() is True
            and session.get_transaction() is transaction
            and actual is connection
            and connection.is_in_transaction() is True
            and connection.get_server_pid() == backend_pid,
            "retained_session_changed",
        )
    except Exception as error:
        raise ValueError("plan_release_storage_retained_session_changed") from error


async def _api_retained_session_read(session, schema):
    """Install metadata only for the original AsyncSession; native callers use the resolver."""
    try:
        _require(session.in_transaction() is True, "retained_snapshot_required")
        transaction = session.get_transaction()
        _require(transaction is not None, "retained_snapshot_required")
        connection = (await (await session.connection()).get_raw_connection()).driver_connection
        backend_pid = connection.get_server_pid()
        _require(type(backend_pid) is int and 0 < backend_pid <= 2147483647, "retained_backend_invalid")
        retained, pins = await read_retained_binding_metadata(connection, schema)
        await _api_session_continuity(session, connection, transaction, backend_pid)
        if pins:
            await install_network_read_inventory(session, pins)
            await _api_session_continuity(session, connection, transaction, backend_pid)
        return retained
    except ServiceUnavailable as error:
        raise ValueError("plan_release_storage_retained_identity_changed") from error
    except ValueError:
        raise
    except Exception as error:
        raise ValueError("plan_release_storage_retained_session_unavailable") from error
