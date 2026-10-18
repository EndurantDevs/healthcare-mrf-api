# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native authority closure for application-owned NPPES admission seals."""

from __future__ import annotations

import asyncio

from db import nppes_evidence_callable_contract as callable_contract
from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_source_policy as source_policy
from process.nppes_public_evidence_writer_contract import writer_error
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_custody import storage_roles
from process.plan_release_storage_provision import _GRANTEES_SQL

CONTROL_TABLES = (
    "public_evidence_nppes_registry_storage",
    "public_evidence_nppes_registry_admission",
    "public_evidence_nppes_registry_admission_seal",
    "public_evidence_nppes_registry_chain_admission",
    "public_evidence_nppes_registry_chain_admission_seal",
    "public_evidence_nppes_registry_chain_archive",
)
ARCHIVE_GUARD = "public_evidence_nppes_registry_admission_integrity_guard"
ARCHIVE_VALIDATOR = "validate_public_evidence_nppes_registry_admission_lifecycle_v2"

_ROLE_CLOSURE = """
SELECT r.rolname FROM pg_roles r JOIN pg_namespace n ON n.nspname=$1
WHERE (pg_has_role(r.oid,$2::name,'SET') OR pg_has_role(r.oid,$2::name,'USAGE')
 OR pg_has_role(r.oid,$3::name,'SET') OR pg_has_role(r.oid,$3::name,'USAGE'))
AND r.rolname<>ALL($4::text[]) AND NOT r.rolsuper
AND NOT (pg_has_role(r.oid,n.nspowner,'SET') OR pg_has_role(r.oid,n.nspowner,'USAGE'))
"""
_CALLABLE_CLOSURE = """
SELECT p.oid::bigint FROM pg_proc p CROSS JOIN pg_namespace n
WHERE n.nspname=$1 AND p.prosecdef AND p.oid<>ALL($4::oid[])
AND (pg_has_role(p.proowner,$2::name,'SET') OR pg_has_role(p.proowner,$2::name,'USAGE')
 OR pg_has_role(p.proowner,n.nspowner,'SET') OR pg_has_role(p.proowner,n.nspowner,'USAGE'))
AND EXISTS(SELECT FROM pg_roles r WHERE r.rolname<>ALL($3::text[]) AND NOT r.rolsuper
 AND NOT (pg_has_role(r.oid,n.nspowner,'SET') OR pg_has_role(r.oid,n.nspowner,'USAGE'))
 AND has_function_privilege(r.oid,p.oid,'EXECUTE'))
"""


def _require(condition):
    if not condition:
        raise writer_error()


def _require_control_tables(catalog_rows, schema, owner):
    _require(
        len(catalog_rows) == len(CONTROL_TABLES)
        and {catalog_row["relname"] for catalog_row in catalog_rows} == set(CONTROL_TABLES)
    )
    _require(
        all(
            catalog_row["relowner"] == owner
            and catalog_row["nspname"] == schema
            and catalog_row["relkind"] == "r"
            and catalog_row["relpersistence"] == "p"
            and not catalog_row["parents"]
            and not catalog_row["relispartition"]
            and not catalog_row["relrowsecurity"]
            and not catalog_row["relforcerowsecurity"]
            and catalog_row["direct_acl_closed"]
            and catalog_row["readers_closed"]
            for catalog_row in catalog_rows
        )
    )


async def _closed_catalog(connection, schema, policy, capability):
    owner, readers = await storage_roles(connection, schema, **policy.role_arguments())
    readonly, mutators = await callable_contract.capture_nppes_callable_contract(
        connection, schema, archive_guard_present=False
    )
    _require(
        all(
            catalog_record["owner"] == owner and catalog_record["owner_protected"] and catalog_record["execute_closed"]
            for catalog_record in mutators
        )
    )
    _require(
        all(catalog_record["owner"] == owner for catalog_record in await _append_definers(connection, schema, readonly))
    )
    catalog_rows = await connection.fetch(
        CATALOG_SQL, [storage_relation(schema, table) for table in CONTROL_TABLES], owner, readers
    )
    _require_control_tables(catalog_rows, schema, owner)
    _require(
        not await connection.fetch(
            _ROLE_CLOSURE, schema, policy.owner_role, policy.publisher_role, [policy.owner_role, policy.publisher_role]
        )
    )
    _require(
        not await connection.fetch(
            _CALLABLE_CLOSURE,
            schema,
            policy.owner_role,
            [policy.owner_role, policy.publisher_role],
            sorted(set(capability) | set(readonly)),
        )
    )
    _require(
        not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=ANY($1::oid[]))",
            [catalog_row["oid"] for catalog_row in catalog_rows],
        )
    )
    _require(
        not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=to_regclass($1) AND tgname=$2)",
            storage_relation(schema, CONTROL_TABLES[1]),
            ARCHIVE_GUARD,
        )
    )


async def assert_nppes_control_custody(connection, schema, policy):
    """Require the genuine enrolled publisher and closed metadata authority."""
    publisher = source_policy.bound_publisher()
    _require(publisher.connection is connection and publisher.policy is policy and policy.schema == schema)
    _require(connection.is_in_transaction() is True)
    actor = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
    _require(actor["session"] == policy.publisher_role and actor["current"] == policy.owner_role)
    capability = await source_policy.source_callable_capability(connection, policy)
    await _closed_catalog(connection, schema, policy, capability)


async def attest_nppes_control_custody(connection, schema, policy, *, migration_role, namespace_owner_role):
    """Read completed control custody without granting publication authority."""
    from process.provider_directory_source_callable_custody import (
        _IDENTITY_SQL,
        attest_source_callable_capability,
    )

    _require(policy.schema == schema and connection.is_in_transaction() is True)
    task, identity = asyncio.current_task(), dict(await connection.fetchrow(_IDENTITY_SQL))
    capability = await attest_source_callable_capability(
        connection, policy, migration_role=migration_role, namespace_owner_role=namespace_owner_role
    )
    await _closed_catalog(connection, schema, policy, capability)
    _require(asyncio.current_task() is task and dict(await connection.fetchrow(_IDENTITY_SQL)) == identity)


async def _revoke_table_grants(connection, schema, name):
    relation = storage_relation(schema, name)
    oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation)
    for grant in await connection.fetch(_GRANTEES_SQL, oid):
        principal = "PUBLIC" if grant["grantee"] == 0 else storage_identifier(grant["rolname"])
        privileges = (
            f"ALL PRIVILEGES ({storage_identifier(grant['column_name'])})" if grant["column_name"] else "ALL PRIVILEGES"
        )
        await connection.execute(f"REVOKE {privileges} ON TABLE {relation} FROM {principal} CASCADE")


async def _append_definers(connection, schema, readonly):
    names = set(callable_contract.expected_nppes_callable_bodies(schema)) - {
        callable_contract.ARCHIVE,
        callable_contract.CHAIN,
        callable_contract.IMMUTABLE,
    }
    records = await connection.fetch(
        "SELECT p.oid::bigint AS oid,p.proname AS name,p.proowner::bigint AS owner "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE n.nspname=$1 AND p.oid=ANY($2::oid[]) AND p.proname=ANY($3::text[])",
        schema,
        list(readonly),
        sorted(names),
    )
    _require(len(records) == len(names) and {record["name"] for record in records} == names)
    return tuple(dict(record, signature=storage_relation(schema, record["name"]) + "()") for record in records)


async def adopt_nppes_control_custody(connection, schema, policy, *, migration_role, namespace_owner_role):
    """Roll back every administrative change if any custody proof is rejected."""
    from process.provider_directory_cms_callable_custody import _admin_identity

    _require(connection.is_in_transaction() is True)
    task, actor = asyncio.current_task(), await _admin_identity(connection, policy)
    async with connection.transaction():
        await _adopt_nppes_control_custody(
            connection, schema, policy, migration_role=migration_role, namespace_owner_role=namespace_owner_role
        )
        _require(asyncio.current_task() is task and await _admin_identity(connection, policy) == actor)


async def _transfer_control_custody(connection, schema, policy, functions):
    for table in CONTROL_TABLES:
        await connection.execute(
            f"ALTER TABLE {storage_relation(schema, table)} OWNER TO {storage_identifier(policy.owner_role)}"
        )
        await _revoke_table_grants(connection, schema, table)
        await connection.execute(
            f"GRANT SELECT ON TABLE {storage_relation(schema, table)} TO "
            + ",".join(storage_identifier(role) for role in policy.runtime_roles)
        )
    # Append guards read the protected headers after their ownership changes.
    for function in functions:
        signature = function["signature"]
        await connection.execute(f"ALTER FUNCTION {signature} OWNER TO {storage_identifier(policy.owner_role)}")
        grants = await connection.fetch(
            "SELECT a.grantee,r.rolname FROM pg_proc p "
            "CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a "
            "LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=$1",
            function["oid"],
        )
        for grant in grants:
            principal = "PUBLIC" if grant["grantee"] == 0 else storage_identifier(grant["rolname"])
            await connection.execute(f"REVOKE EXECUTE ON FUNCTION {signature} FROM {principal} CASCADE")


async def _assert_sealed_controls(connection, schema):
    for table, seal, key in (
        (CONTROL_TABLES[1], CONTROL_TABLES[2], "admission_ref"),
        (CONTROL_TABLES[3], CONTROL_TABLES[4], "chain_ref"),
    ):
        _require(
            not await connection.fetchval(
                f"SELECT EXISTS(SELECT FROM {storage_relation(schema, table)} a "
                f"WHERE NOT EXISTS(SELECT FROM {storage_relation(schema, seal)} s "
                f"WHERE s.{storage_identifier(key)}=a.{storage_identifier(key)}))"
            )
        )


async def _adopt_nppes_control_custody(connection, schema, policy, *, migration_role, namespace_owner_role):
    """Close all NPPES metadata writers before retiring the old fact validator.

    The caller owns this administrator transaction. Existing shared fact guards,
    sealed historical rows and the low-volume chain validator remain in place.
    """
    from process.nppes_public_evidence_catalog import assert_nppes_admission_catalog
    from process.provider_directory_source_callable_custody import attest_source_callable_capability

    storage_identifier(migration_role)
    storage_identifier(namespace_owner_role)
    _require(policy.schema == schema and connection.is_in_transaction() is True)
    namespace = await connection.fetchval(
        "SELECT pg_get_userbyid(nspowner)::text FROM pg_namespace WHERE nspname=$1", schema
    )
    _require(namespace == namespace_owner_role)
    capability = await attest_source_callable_capability(
        connection, policy, migration_role=migration_role, namespace_owner_role=namespace_owner_role
    )
    await connection.execute(
        "LOCK TABLE "
        + ",".join(storage_relation(schema, table) for table in sorted(CONTROL_TABLES))
        + " IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    guard = await connection.fetchrow(
        "SELECT p.proname,t.tgenabled::text AS tgenabled,t.tgdeferrable,t.tginitdeferred "
        "FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid "
        "WHERE t.tgrelid=to_regclass($1) AND t.tgname=$2",
        storage_relation(schema, CONTROL_TABLES[1]),
        ARCHIVE_GUARD,
    )
    if guard is None:
        await _closed_catalog(connection, schema, policy, capability)
        return
    _require(
        guard["proname"] == ARCHIVE_VALIDATOR
        and guard["tgenabled"] == "A"
        and guard["tgdeferrable"]
        and guard["tginitdeferred"]
    )
    readonly, mutators = await callable_contract.capture_nppes_callable_contract(
        connection, schema, archive_guard_present=True
    )
    append_definers = await _append_definers(connection, schema, readonly)
    await assert_nppes_admission_catalog(connection, schema)
    await _assert_sealed_controls(connection, schema)
    await _transfer_control_custody(connection, schema, policy, (*mutators, *append_definers))
    await connection.execute(
        f"DROP TRIGGER {storage_identifier(ARCHIVE_GUARD)} ON {storage_relation(schema, CONTROL_TABLES[1])}"
    )
    await _closed_catalog(connection, schema, policy, capability)
