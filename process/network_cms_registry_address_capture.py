# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Close the complete native address clone without changing its origin authority."""

import asyncio
import json

from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

from process.entity_address_snapshot_ownership import verify_entity_address_archive_stage_ownership
from process.entity_address_snapshot_receipt import capture_entity_address_archive_receipt
from process.network_address_projection import _identifier

_NATIVE_CONSTRAINT_TRIGGERS_SQL = """SELECT NOT EXISTS (
 SELECT 1 FROM pg_catalog.pg_trigger t
 JOIN pg_catalog.pg_class c ON c.oid=t.tgrelid
 LEFT JOIN pg_catalog.pg_constraint k ON k.oid=t.tgconstraint
 LEFT JOIN pg_catalog.pg_index i ON i.indexrelid=k.conindid
 LEFT JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid
 LEFT JOIN pg_catalog.pg_proc p ON p.oid=t.tgfoid
 WHERE c.relnamespace=pg_catalog.to_regnamespace($1::text) AND c.relname=ANY($2::text[]) AND NOT COALESCE(
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
   AND idx.relkind='i' AND idx.relnamespace=c.relnamespace
   AND idx.relpersistence='p' AND idx.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='btree')
   AND p.oid='pg_catalog.unique_key_recheck()'::regprocedure
   AND p.pronamespace='pg_catalog'::regnamespace
   AND p.prolang=(SELECT oid FROM pg_catalog.pg_language WHERE lanname='internal')
   AND p.prosrc='unique_key_recheck' AND p.prokind='f' AND p.provolatile='v'
   AND NOT p.prosecdef AND NOT p.proretset AND p.pronargs=0
   AND p.proargtypes=''::oidvector AND p.prorettype='trigger'::regtype
   AND p.proconfig IS NULL AND p.probin IS NULL,
 false))"""


async def _require_native_constraint_triggers(connection, schema_name, relation_names):
    """Admit only native deferred uniqueness checks tied to their own exact index."""
    names = tuple(relation_names)
    if not names or len(set(names)) != len(names):
        raise ValueError("registry_cms_address_selected_relations_invalid")
    for name in names:
        _identifier(name)
    if await connection.fetchval(_NATIVE_CONSTRAINT_TRIGGERS_SQL, schema_name, list(names)) is not True:
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
            # A timed yield lets pending renewal I/O drain between payload batches.
            if row_number % 256 == 0:
                await asyncio.sleep(0.001)
        return rows

    async def scalar(self, statement, parameters=None):
        """Read the first native result value without changing transaction ownership."""
        records = await self.execute(statement, parameters)
        return next(iter(records[0].values())) if records else None


async def freeze_address_clone(session, ownership, owner_role, runtime_roles):
    """Close the complete named clone through the common native permission transition."""
    from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot

    connection = await native_driver(session)
    ownership = await verify_entity_address_archive_stage_ownership(session, owner=ownership)
    await freeze_fhir_source_snapshot(
        connection,
        ownership.schema_name,
        namespace_owner=owner_role,
        reader_roles=tuple(dict.fromkeys((owner_role, *runtime_roles))),
        application_roles=runtime_roles,
        relation_names=ownership.relation_names,
    )
    return await address_clone_identity(session, ownership, owner_role, runtime_roles)


async def address_clone_identity(session, ownership, owner_role=None, runtime_roles=(), *, verify_content=True):
    """Validate complete named content/layout using actual native read access."""
    from process.network_cms_registry_address_equivalence import _native_capture

    connection = await native_driver(session)
    ownership = await verify_entity_address_archive_stage_ownership(session, owner=ownership)
    await connection.execute(
        "LOCK TABLE "
        + ",".join(_identifier(ownership.schema_name) + "." + _identifier(name) for name in ownership.relation_names)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    await _require_native_constraint_triggers(connection, ownership.schema_name, ownership.relation_names)
    captured, catalog_json, _logical, _ordinals, _sequence = await _native_capture(
        session,
        ownership.schema_name,
        ownership.relation_names,
        verify_content=verify_content,
    )
    from process.entity_address_snapshot_receipt import _canonical_digest

    return captured, _canonical_digest(json.loads(catalog_json))
