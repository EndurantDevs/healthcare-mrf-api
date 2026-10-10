# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Close the complete native address clone without changing its origin authority."""

import hashlib
import json

from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

from process.entity_address_snapshot_ownership import verify_entity_address_archive_stage_ownership
from process.entity_address_snapshot_receipt import capture_entity_address_archive_receipt
from process.network_address_projection import _identifier
from process.network_custom_address_source import _closed
from process.network_fhir_source_epoch import _catalog_relations
from process.network_membership_writer_closure import _check_roles


async def native_driver(session):
    """Use the caller's transaction for native custody checks and COPY."""
    if type(session) is NativeReceiptSession:
        return session.native_connection
    return (await (await session.connection()).get_raw_connection()).driver_connection


class _NativeMappings(list):
    def mappings(self):
        """Expose the existing receipt reader's mapping interface."""
        return self

    def one_or_none(self):
        """Require at most one native catalog row."""
        if len(self) > 1:
            raise ValueError("registry_cms_address_receipt_invalid")
        return self[0] if self else None


class NativeReceiptSession:
    """Run existing receipt SQL on exactly the publisher's asyncpg transaction."""

    def __init__(self, connection):
        self.native_connection = connection

    def in_transaction(self):
        """Report the caller's actual native transaction state."""
        return self.native_connection.is_in_transaction()

    async def execute(self, statement, parameters=None):
        """Compile named receipt binds and decode native JSON result columns."""
        compiled = statement.bindparams(**(parameters or {})).compile(dialect=PGDialect_asyncpg())
        prepared = await self.native_connection.prepare(str(compiled))
        json_names = {
            attribute.name for attribute in prepared.get_attributes() if attribute.type.name in {"json", "jsonb"}
        }
        records = await prepared.fetch(*(compiled.params[name] for name in compiled.positiontup or ()))
        return _NativeMappings(
            {
                name: json.loads(value) if name in json_names and isinstance(value, str) else value
                for name, value in record.items()
            }
            for record in records
        )

    async def scalar(self, statement, parameters=None):
        """Read the first native result value without changing transaction ownership."""
        records = await self.execute(statement, parameters)
        return next(iter(records[0].values())) if records else None


async def freeze_address_clone(session, ownership, owner_role, runtime_roles):
    """Transfer only the already captured family and revoke inherited write grants."""
    connection = await native_driver(session)
    await _check_roles(connection, {"owner_role": owner_role, "loader_roles": [], "reader_roles": list(runtime_roles)})
    await verify_entity_address_archive_stage_ownership(session, owner=ownership)
    namespace, owner = _identifier(ownership.schema_name), _identifier(owner_role)
    await connection.execute(f"ALTER SCHEMA {namespace} OWNER TO {owner}")
    for name, _oid in ownership.relation_oids:
        await connection.execute(f"ALTER TABLE {namespace}.{_identifier(name)} OWNER TO {owner}")
    sequences = await connection.fetch(
        "SELECT relname FROM pg_class WHERE relnamespace=$1::oid AND relkind='S'", ownership.schema_oid
    )
    for sequence in sequences:
        await connection.execute(f"ALTER SEQUENCE {namespace}.{_identifier(sequence['relname'])} OWNER TO {owner}")
    grantees = await connection.fetch(
        """SELECT DISTINCT CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE quote_ident(r.rolname) END AS name
      FROM pg_namespace n LEFT JOIN pg_class c ON c.relnamespace=n.oid
      CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))||coalesce(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 's'::"char" ELSE 'r'::"char" END,c.relowner))) a
      LEFT JOIN pg_roles r ON r.oid=a.grantee WHERE n.oid=$1::oid AND a.grantee<>n.nspowner""",
        ownership.schema_oid,
    )
    for grantee in grantees:
        for grant_target in (
            f"SCHEMA {namespace}",
            f"ALL TABLES IN SCHEMA {namespace}",
            f"ALL SEQUENCES IN SCHEMA {namespace}",
        ):
            await connection.execute(f"REVOKE ALL ON {grant_target} FROM {grantee['name']} CASCADE")
    for role in runtime_roles:
        await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {_identifier(role)}")
        await connection.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {namespace} TO {_identifier(role)}")
    return await address_clone_identity(session, ownership, owner_role, runtime_roles)


async def address_clone_identity(session, ownership, owner_role, runtime_roles, *, verify_content=True):
    """Verify full-model OIDs, indexes, owner and SELECT-only physical custody."""
    connection = await native_driver(session)
    await connection.execute(
        "LOCK TABLE "
        + ",".join(_identifier(ownership.schema_name) + "." + _identifier(name) for name, _ in ownership.relation_oids)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    await verify_entity_address_archive_stage_ownership(session, owner=ownership)
    await _check_roles(connection, {"owner_role": owner_role, "loader_roles": [], "reader_roles": list(runtime_roles)})
    owner_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", owner_role)
    if (
        await connection.fetchval("SELECT nspowner FROM pg_namespace WHERE oid=$1::oid", ownership.schema_oid)
        != owner_oid
    ):
        raise ValueError("registry_cms_address_custody_invalid")
    relations = await _catalog_relations(connection, ownership.schema_name)
    if any(
        relation_record["relowner"] != owner_oid
        or relation_record["relhastriggers"]
        or relation_record["relrowsecurity"]
        or relation_record["relforcerowsecurity"]
        or relation_record["inherited"]
        or relation_record["relpersistence"] != b"p"
        for relation_record in relations
    ):
        raise ValueError("registry_cms_address_custody_invalid")
    await _closed(
        connection, ownership.schema_oid, owner_oid, runtime_roles, [name for name, _ in ownership.relation_oids]
    )
    if await connection.fetchval(
        """SELECT EXISTS(SELECT 1 FROM pg_class c
      CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('s',c.relowner))) a
      WHERE c.relnamespace=$1::oid AND c.relkind='S' AND a.grantee<>$2::oid)
      OR EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=$1::oid)""",
        ownership.schema_oid,
        owner_oid,
    ):
        raise ValueError("registry_cms_address_custody_invalid")
    for relation_record in relations:
        for index in json.loads(relation_record["indexes"] or "[]"):
            if index["owner_oid"] != owner_oid or not all(index[field] for field in ("valid", "ready", "live")):
                raise ValueError("registry_cms_address_custody_invalid")
    catalog_sha256 = hashlib.sha256(
        json.dumps(
            [dict(relation_record) for relation_record in relations], sort_keys=True, separators=(",", ":"), default=str
        ).encode()
    ).hexdigest()
    receipt = (
        await capture_entity_address_archive_receipt(session, schema_name=ownership.schema_name)
        if verify_content
        else None
    )
    return receipt, catalog_sha256
