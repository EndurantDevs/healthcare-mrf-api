# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Replay explicit reader grants when named publication tables are replaced."""

from dataclasses import dataclass

from db.plan_release_storage import storage_relation

_EXPLICIT_GRANTS_SQL = """WITH relation AS (
 SELECT c.oid,c.relacl,c.relowner FROM pg_catalog.pg_class c
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=$1 AND c.relname=$2), explicit_grants AS (
 SELECT NULL::text AS column_name,acl.* FROM relation
 CROSS JOIN LATERAL pg_catalog.aclexplode(relation.relacl) acl
 UNION ALL
 SELECT attribute.attname,acl.* FROM relation
 JOIN pg_catalog.pg_attribute attribute ON attribute.attrelid=relation.oid
 CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
 WHERE attribute.attnum>0 AND NOT attribute.attisdropped)
 SELECT pg_catalog.quote_ident(column_name) AS column_sql,
 CASE WHEN explicit_grants.grantee=0 THEN 'PUBLIC'
      ELSE pg_catalog.quote_ident(grantee.rolname) END AS principal_sql,
 explicit_grants.is_grantable FROM explicit_grants
 LEFT JOIN pg_catalog.pg_roles grantee ON grantee.oid=explicit_grants.grantee
 WHERE ($3::boolean AND explicit_grants.privilege_type='SELECT')
    OR (NOT $3::boolean AND explicit_grants.grantee<>(SELECT relowner FROM relation))
 ORDER BY column_name NULLS FIRST,principal_sql,is_grantable"""


async def _lock_read_tables(connection, schema, live_table, stage_table, *, exclusive=False):
    live, stage = storage_relation(schema, live_table), storage_relation(schema, stage_table)
    if live_table == stage_table:
        raise RuntimeError("ordinary_address_read_access_relations_changed")
    relations = [stage]
    if await connection.fetchval("SELECT pg_catalog.to_regclass($1) IS NOT NULL", live):
        relations.append(live)
    mode = "ACCESS EXCLUSIVE" if exclusive else "SHARE"
    await connection.execute(f"LOCK TABLE {','.join(sorted(relations))} IN {mode} MODE NOWAIT")


async def _replay_select(connection, schema, live_table, stage_table):
    target = storage_relation(schema, stage_table)
    stage_grants = await connection.fetch(_EXPLICIT_GRANTS_SQL, schema, stage_table, False)
    for column_sql, principal_sql in sorted(
        {(grant["column_sql"] or "", grant["principal_sql"]) for grant in stage_grants}
    ):
        column = "" if not column_sql else f" ({column_sql})"
        await connection.execute(f"REVOKE ALL PRIVILEGES{column} ON TABLE {target} FROM {principal_sql} CASCADE")
    for grant in await connection.fetch(_EXPLICIT_GRANTS_SQL, schema, live_table, True):
        column = "" if grant["column_sql"] is None else f" ({grant['column_sql']})"
        option = " WITH GRANT OPTION" if grant["is_grantable"] else ""
        await connection.execute(f"GRANT SELECT{column} ON TABLE {target} TO {grant['principal_sql']}{option}")


async def preserve_artifact_read_access(capability, stages):
    """Replay current explicit SELECT grants under named publication locks."""
    for stage in stages:
        if stage.schema != capability.policy.schema:
            raise RuntimeError("provider_directory_artifact_read_access_schema_changed")
        await _lock_read_tables(
            capability.connection, stage.schema, stage.target_relation, stage.stage_table, exclusive=True
        )
        await _replay_select(capability.connection, stage.schema, stage.target_relation, stage.stage_table)


@dataclass(frozen=True)
class _OrdinaryReadAccess:
    """Carry only the named tables whose current readers must survive the swap."""

    schema: str
    live_table: str
    stage_table: str


async def _ordinary_access(database, schema, live_table, stage_table, *, exclusive=False):
    from process.provider_directory_serving_recovery_receipts import ordinary

    storage_relation(schema, live_table)
    storage_relation(schema, stage_table)
    if live_table == stage_table:
        raise RuntimeError("ordinary_address_read_access_relations_changed")
    _session, driver, _actor = await ordinary(database)
    await _lock_read_tables(driver, schema, live_table, stage_table, exclusive=exclusive)
    return driver


async def prepare_ordinary_read_access(database, schema, swaps):
    """Prepare the named ordinary Address swap inventory while readers continue."""
    return await _prepare_read_relations(
        database, schema, ((swap.live_cls.__main_table__, swap.stage_cls.__tablename__) for swap in swaps)
    )


async def prepare_ordinary_artifact_reads(database, schema, stages):
    """Prepare ordinary artifact names in their original schema."""
    stages = tuple(stages)
    if any(stage.schema != schema for stage in stages):
        raise RuntimeError("ordinary_address_read_access_schema_changed")
    return await _prepare_read_relations(
        database, schema, ((stage.target_relation, stage.stage_table) for stage in stages)
    )


async def _prepare_read_relations(database, schema, relation_pairs):
    proofs = []
    for live, stage in relation_pairs:
        await _ordinary_access(database, schema, live, stage)
        proofs.append(_OrdinaryReadAccess(schema, live, stage))
    return tuple(proofs)


async def recheck_ordinary_read_access(database, proofs):
    """Replay the latest explicit SELECT grants under final named table locks."""
    for proof in proofs:
        if type(proof) is not _OrdinaryReadAccess:
            raise ValueError("ordinary_address_read_access_invalid")
        driver = await _ordinary_access(database, proof.schema, proof.live_table, proof.stage_table, exclusive=True)
        await _replay_select(driver, proof.schema, proof.live_table, proof.stage_table)
