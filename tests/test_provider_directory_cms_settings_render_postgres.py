# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native SET rendering parity only; these synthetic functions issue no CMS authority."""

import asyncio
import json
import os
from uuid import uuid4

import asyncpg
import pytest

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_routine_settings as settings


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "schema,identifier,timezone",
    [
        ("settings_lower", "settings_lower", False),
        ("MixedSettings", '"MixedSettings"', False),
        ("select", '"select"', False),
        ("settings_utc", "settings_utc", True),
    ],
)
async def test_native_settings_declaration_preserves_quoted_schema_and_timezone(schema, identifier, timezone):
    dsn = os.getenv("HLTHPRT_CMS_CUSTODY_TEST_DSN")
    if not dsn:
        pytest.skip("set HLTHPRT_CMS_CUSTODY_TEST_DSN for native settings rendering")
    connection = await asyncpg.connect(dsn, command_timeout=3)
    signature = storage_relation(schema, "settings_" + uuid4().hex[:12]) + "()"
    legacy_settings = ["search_path=pg_catalog, " + identifier]
    transaction = connection.transaction()
    try:
        assert await connection.fetchval("SELECT pg_catalog.to_regnamespace($1)", schema) is None
        async with asyncio.timeout(3):
            await transaction.start()
            await connection.execute("CREATE SCHEMA " + storage_identifier(schema))
            if timezone:
                legacy_settings = ["search_path=pg_catalog", "TimeZone=UTC"]
            old_settings, target_settings = settings.approved_cms_routine_settings(legacy_settings)
            await connection.execute(
                "CREATE FUNCTION "
                + signature
                + " RETURNS pg_catalog.text LANGUAGE sql SET "
                + old_settings[0]
                + (" SET TimeZone='UTC'" if timezone else "")
                + " AS $function$SELECT 'unchanged'::pg_catalog.text$function$"
            )
            query = (
                "SELECT pg_catalog.to_jsonb(p) AS definition,pg_catalog.pg_get_functiondef(p.oid) AS body "
                "FROM pg_catalog.pg_proc p WHERE p.oid=pg_catalog.to_regprocedure($1)"
            )
            before = await connection.fetchrow(query, signature)
            assert json.loads(before["definition"])["proconfig"] == old_settings
            expected = settings._declaration_after(before["body"], old_settings, target_settings)
            await connection.execute("ALTER FUNCTION " + signature + " SET " + target_settings[0])
            after = await connection.fetchrow(query, signature)
            original_definition = json.loads(before["definition"])
            original_definition["proconfig"] = target_settings
            assert json.loads(after["definition"]) == original_definition
            assert after["body"] == expected
    finally:
        if connection.is_in_transaction():
            await transaction.rollback()
        assert await connection.fetchval("SELECT pg_catalog.to_regnamespace($1)", schema) is None
        await connection.close()
