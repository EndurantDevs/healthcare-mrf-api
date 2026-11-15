# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Revoke ordinary publication writes using only native PostgreSQL permissions."""

from sqlalchemy import text

from db.plan_release_storage import storage_relation


def _transaction(session):
    if not session.in_transaction():
        raise RuntimeError("address publication requires a caller transaction")


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
        await session.execute(text(f"REVOKE {','.join(privileges)} ON {kind} {relation} FROM {principal} CASCADE"))


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
