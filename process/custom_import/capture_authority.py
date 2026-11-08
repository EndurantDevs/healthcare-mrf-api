# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in trusted capture writes without changing legacy writer privileges."""

from sqlalchemy import text

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportCaptureUsage,
)
from db.models.custom_import_raw_capture import CustomImportCaptureRawObject
from process.custom_import.capture_store import CaptureStoreError, _require_clean_session, _require_transaction

_CAPTURE_MODELS = (
    CustomImportCaptureBundle,
    CustomImportCapture,
    CustomImportCaptureParquetPart,
    CustomImportCaptureUsage,
    CustomImportCaptureRawObject,
)
_AUTHORITY_SQL = """
WITH targets AS (
    SELECT relation.oid, relation.relowner, relation.relkind, relation.relacl
    FROM pg_catalog.unnest(CAST(:relations AS text[])) AS expected(name)
    LEFT JOIN pg_catalog.pg_class relation ON relation.oid = pg_catalog.to_regclass(expected.name)
), write_grants AS (
    SELECT target.relowner, acl.grantee
    FROM targets target
    CROSS JOIN LATERAL pg_catalog.aclexplode(
        coalesce(target.relacl, pg_catalog.acldefault('r', target.relowner))) acl
    WHERE acl.privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER', 'MAINTAIN')
    UNION ALL
    SELECT target.relowner, acl.grantee
    FROM targets target JOIN pg_catalog.pg_attribute attribute ON attribute.attrelid = target.oid
    CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) acl
    WHERE attribute.attnum > 0 AND NOT attribute.attisdropped
      AND acl.privilege_type IN ('INSERT', 'UPDATE', 'REFERENCES')
)
SELECT count(*) = :relation_count
    AND coalesce(bool_and(oid IS NOT NULL AND relkind = 'r'
        AND pg_catalog.pg_has_role(relowner, 'USAGE')), false)
    AND NOT EXISTS (
        SELECT 1 FROM write_grants grant_entry
        WHERE grant_entry.grantee <> grant_entry.relowner
    )
FROM targets
"""


async def _relation_names(session):
    connection = await session.connection()
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    dialect = connection.sync_connection.dialect
    quote = dialect.identifier_preparer.quote
    relations = []
    for model in _CAPTURE_MODELS:
        table = model.__table__
        schema = schema_map.get(table.schema, table.schema) or dialect.default_schema_name
        if not isinstance(schema, str) or not schema:
            raise CaptureStoreError("raw capture authority requires an explicit resolved schema")
        relations.append(f"{quote(schema)}.{quote(table.name)}")
    return relations


async def require_raw_capture_authority(session) -> None:
    """Require owner-equivalent authority and closed non-owner write grant roots.

    This check never grants or assumes a role. Administrators must drain earlier
    writers before closing ACLs; an already-authorized statement can outlive REVOKE.
    Owner/admin role changes remain the trusted provisioning boundary.
    """

    _require_transaction(session)
    _require_clean_session(session)
    allowed = await session.scalar(
        text(_AUTHORITY_SQL), {"relations": await _relation_names(session), "relation_count": len(_CAPTURE_MODELS)}
    )
    if allowed is not True:
        raise CaptureStoreError("raw capture requires trusted registrar authority and closed capture write grants")
