# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared snapshot pin guards survive the legacy binding retirement."""

import pytest

from tests.plan_release_storage_postgres_support import install_legacy_guards, storage_case


@pytest.mark.asyncio
async def test_shared_pin_guards_survive_binding_retirement():
    async with storage_case(migrated=False) as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(lambda native: install_legacy_guards(native, case.schema))
        admin = await case.connect()
        shared = case.relation("ptg2_snapshot_pin")
        query = "SELECT oid,pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid=to_regclass($1) ORDER BY oid"
        original = await admin.fetch(query, shared)
        assert len(original) == 6  # Four unchanged custom guards and two native snapshot FK hooks.
        await case.migrate()
        await case.provision()
        assert await admin.fetch(query, shared) == original
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=to_regclass($1))",
            case.relation("plan_release_snapshot_binding_legacy"),
        )
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_constraint WHERE conrelid=to_regclass($1) AND contype='f')",
            case.relation("plan_release_snapshot_binding_legacy"),
        )
