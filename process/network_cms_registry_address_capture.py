# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Close the complete native address clone without changing its origin authority."""

import asyncio
import hashlib
import json

from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

from process.entity_address_snapshot_ownership import verify_entity_address_archive_stage_ownership
from process.entity_address_snapshot_receipt import capture_entity_address_archive_receipt
from process.network_address_projection import _identifier
from process.network_custom_address_source import _closed
from process.network_fhir_source_epoch import _catalog_relations
from process.network_membership_writer_closure import _check_roles

_NATIVE_CONSTRAINT_TRIGGERS_SQL = """SELECT NOT EXISTS (
 SELECT 1 FROM pg_catalog.pg_trigger t
 JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid
 LEFT JOIN pg_catalog.pg_constraint k ON k.oid=t.tgconstraint
 LEFT JOIN pg_catalog.pg_index i ON i.indexrelid=k.conindid
 LEFT JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid
 LEFT JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid
 WHERE c.relnamespace=$1::oid AND NOT COALESCE(
   t.tgisinternal AND t.tgenabled='O' AND t.tgtype=21
   AND t.tgparentid=0 AND t.tgconstrrelid=0
   AND t.tgnargs=0 AND octet_length(t.tgargs)=0 AND t.tgattr=''::int2vector
   AND t.tgqual IS NULL AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL
   AND k.conrelid=t.tgrelid AND k.connamespace=c.relnamespace
   AND k.contype IN ('u','p') AND k.convalidated
   AND k.conparentid=0 AND k.condeferrable AND t.tgdeferrable=k.condeferrable
   AND t.tginitdeferred=k.condeferred AND k.conindid=t.tgconstrindid
   AND i.indrelid=t.tgrelid AND i.indisunique AND NOT i.indimmediate
   AND i.indisvalid AND i.indisready AND i.indislive
   AND i.indisprimary=(k.contype='p') AND i.indpred IS NULL AND i.indexprs IS NULL
   AND cardinality(k.conkey)>0 AND cardinality(k.conkey)=i.indnkeyatts
   AND k.conkey=ARRAY(SELECT key.attnum FROM unnest(i.indkey) WITH ORDINALITY key(attnum,pos)
                     WHERE key.pos<=i.indnkeyatts ORDER BY key.pos)
   AND idx.relkind='i' AND idx.relowner=c.relowner AND idx.relnamespace=c.relnamespace
   AND idx.relpersistence='p' AND idx.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='btree')
   AND p.oid='pg_catalog.unique_key_recheck()'::regprocedure
   AND p.pronamespace='pg_catalog'::regnamespace
   AND p.prolang=(SELECT oid FROM pg_catalog.pg_language WHERE lanname='internal')
   AND p.prosrc='unique_key_recheck' AND p.prokind='f' AND p.provolatile='v'
   AND NOT p.prosecdef AND NOT p.proretset AND p.pronargs=0
   AND p.proargtypes=''::oidvector AND p.prorettype='trigger'::regtype
   AND p.proconfig IS NULL AND p.probin IS NULL,
 false))"""


async def _require_native_constraint_triggers(connection, schema_oid):
    """Admit only native deferred uniqueness checks tied to their own exact index."""
    if await connection.fetchval(_NATIVE_CONSTRAINT_TRIGGERS_SQL, schema_oid) is not True:
        raise ValueError("registry_cms_address_custody_invalid")


async def native_driver(session):
    """Expose the caller's physical transaction without taking its first data snapshot."""
    if type(session) is NativeReceiptSession:
        return session.native_connection
    if not session.in_transaction():
        raise ValueError("registry_cms_address_transaction_required")
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    if not driver.is_in_transaction():
        await connection.exec_driver_sql("SHOW transaction_isolation")
        if not driver.is_in_transaction():
            raise ValueError("registry_cms_address_transaction_required")
    return driver


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
        rows = _NativeMappings()
        for row_number, record in enumerate(records, 1):
            rows.append(
                {
                    name: json.loads(value) if name in json_names and isinstance(value, str) else value
                    for name, value in record.items()
                }
            )
            # Large proof pages must not starve concurrent lease renewals.
            if row_number % 256 == 0:
                await asyncio.sleep(0)
        return rows

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
    owner_oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", owner_role)
    if (
        await connection.fetchval("SELECT nspowner FROM pg_namespace WHERE oid=$1::oid", ownership.schema_oid)
        != owner_oid
    ):
        raise ValueError("registry_cms_address_custody_invalid")
    relations = await _catalog_relations(connection, ownership.schema_name)
    await _require_native_constraint_triggers(connection, ownership.schema_oid)
    if any(
        relation_record["relowner"] != owner_oid
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
