# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Close the complete native address clone without changing its origin authority."""

import asyncio
import json

from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

from process.entity_address_snapshot_ownership import verify_entity_address_archive_stage_ownership
from process.network_address_projection import _identifier


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
    """Transfer only the already captured family and revoke inherited write grants."""
    from process.entity_address_snapshot_source import _close_clone_grants

    connection = await native_driver(session)
    await verify_entity_address_archive_stage_ownership(session, owner=ownership)
    namespace, owner = _identifier(ownership.schema_name), _identifier(owner_role)
    await connection.execute(f"ALTER SCHEMA {namespace} OWNER TO {owner}")
    for name, _oid in ownership.relation_oids:
        await connection.execute(f"ALTER TABLE {namespace}.{_identifier(name)} OWNER TO {owner}")
    sequences = await connection.fetch(
        "SELECT relname FROM pg_class WHERE relnamespace=to_regnamespace($1) AND relkind='S'", ownership.schema_name
    )
    for sequence in sequences:
        await connection.execute(f"ALTER SEQUENCE {namespace}.{_identifier(sequence['relname'])} OWNER TO {owner}")
    await _close_clone_grants(connection, ownership.schema_name, (name for name, _ in ownership.relation_oids))
    for role in runtime_roles:
        await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {_identifier(role)}")
        await connection.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {namespace} TO {_identifier(role)}")
    return await address_clone_identity(session, ownership, owner_role, runtime_roles)


async def address_clone_identity(
    session,
    ownership,
    owner_role,
    runtime_roles,
    *,
    verify_content=True,
    expected_receipt=None,
    expected_logical_sha256=None,
):
    """Compare the named snapshot's data and normalized layout, never local identities."""
    from process.network_cms_registry_address_equivalence import _native_capture

    connection = await native_driver(session)
    await connection.execute(
        "LOCK TABLE "
        + ",".join(_identifier(ownership.schema_name) + "." + _identifier(name) for name, _ in ownership.relation_oids)
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    captured, catalog_json, logical_hash, _columns, _sequence = await _native_capture(
        session, ownership.schema_name, None, verify_content=verify_content
    )
    if expected_logical_sha256 is not None:
        if logical_hash != expected_logical_sha256:
            raise ValueError("registry_cms_address_layout_changed")
        if verify_content and expected_receipt is not None:
            captured = _accepted_receipt(captured, expected_receipt)
    elif not verify_content and expected_receipt is not None:
        schema_sha256_by_table = json.loads(catalog_json)["schema_sha256_by_table"]
        for table in expected_receipt.tables:
            current = schema_sha256_by_table[table.table_name]
            if current != table.schema_sha256:
                raise ValueError("registry_cms_address_layout_changed")
    return captured, logical_hash


def _accepted_receipt(captured, expected):
    """Preserve accepted receipt bytes after normalized layout and full content checks."""
    from process.entity_address_snapshot_receipt import validate_entity_address_archive_receipt

    expected = validate_entity_address_archive_receipt(expected)
    if (
        captured.contract != expected.contract
        or captured.main_input_sha256 != expected.main_input_sha256
        or [(table.model_name, table.table_name, table.row_count, table.row_sha256) for table in captured.tables]
        != [(table.model_name, table.table_name, table.row_count, table.row_sha256) for table in expected.tables]
    ):
        raise ValueError("registry_cms_address_content_changed")
    return expected
