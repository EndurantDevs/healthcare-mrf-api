# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Apply native publication permissions to locked, schema-qualified address tables."""

from sqlalchemy import text

from db.plan_release_storage import storage_relation


def _transaction(session):
    if not session.in_transaction():
        raise RuntimeError("address publication requires a caller transaction")


async def retention_role(session):
    """Resolve the configured DDL destination, without admitting a role profile."""
    _transaction(session)
    role = await session.scalar(
        text(
            "SELECT quote_ident(pg_catalog.pg_get_userbyid(nspowner)) FROM pg_catalog.pg_namespace "
            "WHERE nspname='hp_snapshot_retention'"
        )
    )
    if not isinstance(role, str) or not role:
        raise RuntimeError("address retention namespace is unavailable")
    return role


async def preserve_reader_access(session, schema, live_table, stage_table):
    """Replay existing read grants; PostgreSQL decides whether the caller may grant."""
    _transaction(session)
    old, stage = storage_relation(schema, live_table), storage_relation(schema, stage_table)
    if old == stage:
        raise RuntimeError("address publication stage must differ from the live table")
    rows = await session.execute(
        text(
            "SELECT NULL::text AS column_name,CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(r.rolname) END AS principal,"
            "a.privilege_type,a.is_grantable FROM pg_class c,"
            "LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a "
            "LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE c.oid=to_regclass(:relation) "
            "AND a.privilege_type IN ('SELECT','MAINTAIN') "
            "UNION ALL SELECT quote_ident(col.attname),"
            "CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(r.rolname) END,a.privilege_type,a.is_grantable "
            "FROM pg_attribute col,LATERAL aclexplode(col.attacl) a LEFT JOIN pg_roles r ON r.oid=a.grantee "
            "WHERE col.attrelid=to_regclass(:relation) AND col.attnum>0 AND NOT col.attisdropped "
            "AND a.privilege_type='SELECT'"
        ),
        {"relation": old},
    )
    for grant in rows.mappings():
        column = "" if grant["column_name"] is None else " (" + grant["column_name"] + ")"
        option = " WITH GRANT OPTION" if grant["is_grantable"] else ""
        await session.execute(
            text(f"GRANT {grant['privilege_type']}{column} ON TABLE {stage} TO {grant['principal']}{option}")
        )


async def _revoke_mutation(session, relation, *, sequence=False, include_owner=False):
    privileges = (
        ["USAGE", "UPDATE"] if sequence else ["INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"]
    )
    owner_clause = "" if include_owner else "AND a.grantee<>c.relowner "
    query = (
        "SELECT a.grantee FROM pg_class c,LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a "
        "WHERE c.oid=to_regclass(:relation) " + owner_clause + "AND a.privilege_type=ANY(CAST(:privileges AS text[]))"
    )
    if not sequence:
        query += (
            " UNION SELECT a.grantee FROM pg_attribute col JOIN pg_class c ON c.oid=col.attrelid,LATERAL aclexplode(col.attacl) a "
            "WHERE col.attrelid=to_regclass(:relation) AND col.attnum>0 AND NOT col.attisdropped "
            + owner_clause
            + "AND a.privilege_type=ANY(CAST(:privileges AS text[]))"
        )
    rows = await session.execute(
        text(
            "SELECT DISTINCT CASE WHEN grants.grantee=0 THEN 'PUBLIC' ELSE quote_ident(r.rolname) END AS principal "
            "FROM (" + query + ") grants LEFT JOIN pg_roles r ON r.oid=grants.grantee"
        ),
        {"relation": relation, "privileges": privileges},
    )
    principals = {"PUBLIC", *rows.scalars().all()}
    kind = "SEQUENCE" if sequence else "TABLE"
    for principal in sorted(principals):
        await session.execute(text(f"REVOKE {','.join(privileges)} ON {kind} {relation} FROM {principal}"))


async def seal_relation(session, schema, table, *, role=None):
    """Enforce immutable publication without an owner or permission-closure admission check."""
    _transaction(session)
    relation = storage_relation(schema, table)
    role = await retention_role(session) if role is None else role
    previous = (
        (
            await session.execute(
                text(
                    "SELECT quote_ident(r.rolname) AS principal,"
                    "EXISTS(SELECT FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a "
                    "WHERE a.grantee=c.relowner AND a.privilege_type='SELECT') AS table_read,"
                    "ARRAY(SELECT quote_ident(col.attname) FROM pg_attribute col,LATERAL aclexplode(col.attacl) a "
                    "WHERE col.attrelid=c.oid AND col.attnum>0 AND NOT col.attisdropped AND a.grantee=c.relowner "
                    "AND a.privilege_type='SELECT' ORDER BY col.attnum) AS column_reads "
                    "FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner WHERE c.oid=to_regclass(:relation)"
                ),
                {"relation": relation},
            )
        )
        .mappings()
        .one()
    )
    await session.execute(text(f"ALTER TABLE {relation} OWNER TO {role}"))
    if previous["table_read"]:
        await session.execute(text(f"GRANT SELECT ON TABLE {relation} TO {previous['principal']}"))
    if previous["column_reads"]:
        await session.execute(
            text(f"GRANT SELECT ({','.join(previous['column_reads'])}) ON TABLE {relation} TO {previous['principal']}")
        )
    await _revoke_mutation(session, relation)
    for sequence in await _owned_sequences(session, relation):
        await session.execute(text(f"ALTER SEQUENCE {sequence} OWNER TO {role}"))
        await _revoke_mutation(session, sequence, sequence=True)


async def _owned_sequences(session, relation):
    """Resolve only the sequences belonging to the named publication table."""
    return (
        (
            await session.execute(
                text(
                    "SELECT quote_ident(n.nspname)||'.'||quote_ident(s.relname) FROM pg_depend d "
                    "JOIN pg_class s ON s.oid=d.objid JOIN pg_namespace n ON n.oid=s.relnamespace "
                    "WHERE d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass "
                    "AND d.refobjid=to_regclass(:relation) AND d.deptype IN ('a','i') AND s.relkind='S'"
                ),
                {"relation": relation},
            )
        )
        .scalars()
        .all()
    )


async def make_relation_read_only(session, schema, table):
    """Revoke ordinary writes without requiring transfer to another owning role."""
    _transaction(session)
    relation = storage_relation(schema, table)
    await _revoke_mutation(session, relation, include_owner=True)
    for sequence in await _owned_sequences(session, relation):
        await _revoke_mutation(session, sequence, sequence=True, include_owner=True)
