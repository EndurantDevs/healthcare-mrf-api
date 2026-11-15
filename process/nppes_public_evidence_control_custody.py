# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transactional PostgreSQL permissions for validated NPPES admission controls."""

from __future__ import annotations

from db import nppes_evidence_callable_contract as callable_contract
from db.plan_release_storage import storage_identifier, storage_relation
from process.nppes_public_evidence_writer_contract import writer_error
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


def _require(condition):
    if not condition:
        raise writer_error()


async def _revoke_table_grants(connection, schema, name):
    relation = storage_relation(schema, name)
    oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation)
    for grant in await connection.fetch(_GRANTEES_SQL, oid):
        principal = "PUBLIC" if grant["grantee"] == 0 else storage_identifier(grant["rolname"])
        privileges = (
            f"ALL PRIVILEGES ({storage_identifier(grant['column_name'])})" if grant["column_name"] else "ALL PRIVILEGES"
        )
        await connection.execute(f"REVOKE {privileges} ON TABLE {relation} FROM {principal} CASCADE")


async def adopt_nppes_control_custody(connection, schema, policy, *, migration_role, namespace_owner_role):
    """Apply configured DCL after data validation, rolling back a caller-caught failure.

    Legacy role arguments remain accepted for call compatibility. PostgreSQL
    authorizes the DDL; these arguments do not establish application authority.
    """
    _require(policy.schema == schema and connection.is_in_transaction() is True)
    async with connection.transaction():
        await _adopt_nppes_control_custody(connection, schema, policy)


async def _transfer_control_custody(connection, schema, policy, signatures):
    for table in CONTROL_TABLES:
        await connection.execute(
            f"ALTER TABLE {storage_relation(schema, table)} OWNER TO {storage_identifier(policy.owner_role)}"
        )
        await _revoke_table_grants(connection, schema, table)
        await connection.execute(
            f"GRANT SELECT ON TABLE {storage_relation(schema, table)} TO "
            + ",".join(storage_identifier(role) for role in policy.runtime_roles)
        )
    # Existing append guards read the protected headers after their ownership changes.
    for signature in signatures:
        await connection.execute(f"ALTER FUNCTION {signature} OWNER TO {storage_identifier(policy.owner_role)}")
        grants = await connection.fetch(
            "SELECT a.grantee,r.rolname FROM pg_proc p "
            "CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a "
            "LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE p.oid=to_regprocedure($1)",
            signature,
        )
        for grant in grants:
            principal = "PUBLIC" if grant["grantee"] == 0 else storage_identifier(grant["rolname"])
            await connection.execute(f"REVOKE EXECUTE ON FUNCTION {signature} FROM {principal} CASCADE")


async def _require_completed_data_receipts(connection, schema):
    """Require a completed data receipt for every retained admission and chain."""
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


async def _adopt_nppes_control_custody(connection, schema, policy):
    """Close control writers before retiring the replaced per-admission data hook."""
    from process.nppes_public_evidence_catalog import assert_nppes_admission_catalog, assert_nppes_candidate_catalog

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
        await assert_nppes_candidate_catalog(connection, schema, policy)
        return
    _require(
        guard["proname"] == ARCHIVE_VALIDATOR
        and guard["tgenabled"] == "A"
        and guard["tgdeferrable"]
        and guard["tginitdeferred"]
    )
    await assert_nppes_admission_catalog(connection, schema)
    await _require_completed_data_receipts(connection, schema)
    signatures = tuple(
        storage_relation(schema, name) + "()"
        for name in callable_contract.expected_nppes_callable_bodies(schema)
        if name != callable_contract.IMMUTABLE
    )
    await _transfer_control_custody(connection, schema, policy, signatures)
    await connection.execute(
        f"DROP TRIGGER {storage_identifier(ARCHIVE_GUARD)} ON {storage_relation(schema, CONTROL_TABLES[1])}"
    )
    await assert_nppes_candidate_catalog(connection, schema, policy)
