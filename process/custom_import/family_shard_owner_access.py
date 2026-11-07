# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read-only connection admission; native writer windows remain independent."""

from sqlalchemy import text

from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import.build_source import _set_timeout
from process.custom_import.family_shard_handoff import unavailable


async def require_owner_connection(session, expected_role):
    """Check the same actual procedure/table owners without entering a writer page."""
    await session.execute(text("SET TRANSACTION READ ONLY"))
    await _set_timeout(session, 5000)
    if await session.scalar(text("SELECT CURRENT_USER")) != expected_role:
        unavailable()
    connection = await session.connection()
    schema = CustomImportSnapshotFamily.__table__.schema
    translations = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = translations.get(schema, schema)
    if not isinstance(schema, str) or not schema or len(schema.encode()) > 63:
        unavailable()
    quoted = connection.dialect.identifier_preparer.quote_schema(schema)
    procedures = await session.scalar(
        text(
            "SELECT pg_catalog.count(*)=2 AND pg_catalog.bool_and(pg_catalog.pg_get_userbyid(p.proowner)=CURRENT_USER) "
            "FROM pg_catalog.pg_proc p WHERE p.oid IN "
            "(pg_catalog.to_regprocedure(:authorize),pg_catalog.to_regprocedure(:finalize))"
        ),
        {
            "authorize": quoted
            + ".source_bulk_authorize(pg_catalog.int8,pg_catalog.int2,pg_catalog.int8,pg_catalog.bytea,pg_catalog.int4,pg_catalog.int8)",
            "finalize": quoted + ".source_set_finalize(pg_catalog.uuid,pg_catalog.int4[])",
        },
    )
    tables = await session.scalar(
        text(
            "SELECT count(*)=3 AND bool_and(pg_catalog.pg_get_userbyid(relowner)=CURRENT_USER) "
            "FROM pg_catalog.pg_class WHERE oid=ANY(CAST(:names AS regclass[]))"
        ),
        {
            "names": [
                quoted + "." + name
                for name in (
                    "custom_import_snapshot_family",
                    "custom_import_family_block_layout",
                    "custom_import_family_block_relation",
                )
            ]
        },
    )
    if procedures is not True or tables is not True:
        unavailable()
