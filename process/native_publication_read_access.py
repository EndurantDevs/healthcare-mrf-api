# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve reads during ordinary publication or an owner-bound validated heap swap."""

from db.plan_release_storage import storage_identifier, storage_relation
from process.entity_address_cutover_contract import lock_live_serving_relations

_RELATIONS_SQL = """SELECT requested.name,c.oid::bigint,c.relowner::bigint,
 c.relowner=current_user::regrole::oid AS owned,c.relkind::text,c.relpersistence::text,
 c.relrowsecurity,c.relforcerowsecurity
 FROM unnest($2::text[]) requested(name) LEFT JOIN pg_class c
 ON c.oid=to_regclass(format('%I.%I',$1::text,requested.name)) ORDER BY requested.name"""
_CROSS_RELATIONS_SQL = """SELECT requested.namespace,requested.name,c.oid::bigint,c.relowner::bigint,
 c.relowner=current_user::regrole::oid AS owned,c.relkind::text,c.relpersistence::text,
 pg_catalog.pg_has_role(current_user,c.relowner,'USAGE') AS owner_access,
 c.relrowsecurity,c.relforcerowsecurity,c.relispartition,
 EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited
 FROM unnest($1::text[],$2::text[]) requested(namespace,name) LEFT JOIN pg_class c
 ON c.oid=to_regclass(format('%I.%I',requested.namespace,requested.name))
 ORDER BY requested.namespace,requested.name"""
_READS_SQL = """SELECT NULL::text AS column_name,r.rolname,a.grantee,a.grantor,
 a.privilege_type,a.is_grantable FROM pg_class c,
 LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a
 LEFT JOIN pg_roles r ON r.oid=a.grantee
 WHERE c.oid=$1::oid AND a.grantee<>c.relowner AND a.privilege_type IN ('SELECT','MAINTAIN')
 UNION ALL SELECT col.attname,r.rolname,a.grantee,a.grantor,a.privilege_type,a.is_grantable
 FROM pg_attribute col,LATERAL aclexplode(col.attacl) a LEFT JOIN pg_roles r ON r.oid=a.grantee
 WHERE col.attrelid=$1::oid AND col.attnum>0 AND NOT col.attisdropped AND a.privilege_type='SELECT'"""


async def preserve_publication_reads(
    connection, *, schema, live_table, stage_table, stage_schema=None, expected_stage_owner_oid=None
):
    """Replay only SELECT/MAINTAIN while both exact heaps remain locked in this transaction."""
    storage_identifier(schema)
    storage_identifier(live_table)
    storage_identifier(stage_table)
    if expected_stage_owner_oid is not None and (
        type(expected_stage_owner_oid) is not int
        or not 0 < expected_stage_owner_oid < 2**32
        or stage_schema in (None, schema)
    ):
        raise RuntimeError("publication_read_access_custody_changed")
    if stage_schema is not None:
        storage_identifier(stage_schema)
        if stage_schema != schema:
            return await _preserve_cross_schema_reads(
                connection,
                schema=schema,
                live_table=live_table,
                stage_schema=stage_schema,
                stage_table=stage_table,
                expected_stage_owner_oid=expected_stage_owner_oid,
            )
    if not connection.is_in_transaction() or live_table == stage_table:
        raise RuntimeError("publication_read_access_transaction_required")
    names = [live_table, stage_table]
    before_relations = [dict(entry) for entry in await connection.fetch(_RELATIONS_SQL, schema, names)]
    by_name = {entry["name"]: entry for entry in before_relations}
    if len(before_relations) != 2 or set(by_name) != set(names) or by_name[stage_table]["oid"] is None:
        raise RuntimeError("publication_read_access_relations_changed")
    for entry in before_relations:
        if entry["oid"] is not None and (
            not entry["owned"]
            or entry["relkind"] != "r"
            or entry["relpersistence"] != "p"
            or entry["relrowsecurity"]
            or entry["relforcerowsecurity"]
        ):
            raise RuntimeError("publication_read_access_custody_changed")
    await lock_live_serving_relations(
        connection.execute, schema, [entry["name"] for entry in before_relations if entry["oid"]]
    )
    if before_relations != [dict(entry) for entry in await connection.fetch(_RELATIONS_SQL, schema, names)]:
        raise RuntimeError("publication_read_access_relations_changed")
    prior = by_name[live_table]
    if prior["oid"] is not None:
        grants = await connection.fetch(_READS_SQL, prior["oid"])
        await _replay_reads(connection, storage_relation(schema, stage_table), prior["relowner"], grants)


async def _preserve_cross_schema_reads(
    connection, *, schema, live_table, stage_schema, stage_table, expected_stage_owner_oid
):
    if not connection.is_in_transaction():
        raise RuntimeError("publication_read_access_transaction_required")
    namespaces, names = [schema, stage_schema], [live_table, stage_table]
    before_relations = [dict(entry) for entry in await connection.fetch(_CROSS_RELATIONS_SQL, namespaces, names)]
    by_identity = {(entry["namespace"], entry["name"]): entry for entry in before_relations}
    if len(before_relations) != 2 or set(by_identity) != {(schema, live_table), (stage_schema, stage_table)}:
        raise RuntimeError("publication_read_access_relations_changed")
    stage = by_identity[stage_schema, stage_table]
    prior = by_identity[schema, live_table]
    if stage["oid"] is None or stage["oid"] == prior["oid"]:
        raise RuntimeError("publication_read_access_relations_changed")
    if expected_stage_owner_oid is not None and stage["relowner"] != expected_stage_owner_oid:
        raise RuntimeError("publication_read_access_custody_changed")
    # Only a validated cross-schema cutover may use inherited ownership.
    owner_field = "owned" if expected_stage_owner_oid is None else "owner_access"
    for entry in before_relations:
        if entry["oid"] is not None and (
            not entry[owner_field]
            or entry["relkind"] != "r"
            or entry["relpersistence"] != "p"
            or entry["relrowsecurity"]
            or entry["relforcerowsecurity"]
            or entry["relispartition"]
            or entry["inherited"]
        ):
            raise RuntimeError("publication_read_access_custody_changed")
    await connection.execute(
        f"LOCK TABLE {storage_relation(stage_schema, stage_table)} IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    if prior["oid"] is not None:
        await lock_live_serving_relations(connection.execute, schema, [live_table])
    if before_relations != [dict(entry) for entry in await connection.fetch(_CROSS_RELATIONS_SQL, namespaces, names)]:
        raise RuntimeError("publication_read_access_relations_changed")
    if prior["oid"] is not None:
        grants = await connection.fetch(_READS_SQL, prior["oid"])
        await _replay_reads(connection, storage_relation(stage_schema, stage_table), prior["relowner"], grants)


async def _replay_reads(connection, target, owner, grants):
    for grant in grants:
        column = grant["column_name"]
        if (
            grant["grantor"] != owner
            or grant["privilege_type"] not in {"SELECT", "MAINTAIN"}
            or (column is not None and grant["privilege_type"] != "SELECT")
            or (grant["grantee"] != 0 and not grant["rolname"])
        ):
            raise RuntimeError("publication_read_access_grant_changed")
        principal = "PUBLIC" if grant["grantee"] == 0 else storage_identifier(grant["rolname"])
        selected = "" if column is None else f" ({storage_identifier(column)})"
        option = " WITH GRANT OPTION" if grant["is_grantable"] else ""
        await connection.execute(f"GRANT {grant['privilege_type']}{selected} ON TABLE {target} TO {principal}{option}")
