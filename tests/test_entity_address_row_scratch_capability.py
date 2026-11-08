# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
from contextlib import asynccontextmanager

import pytest

from process import entity_address_preparation_admission as admitted
from tests.entity_address_rows_concurrency_support import parallel_preparation


@asynccontextmanager
async def registered_scratch(schema, name):
    capability = admitted.issue_row_scratch()
    await admitted.create_stage_sql(schema, name, f"CREATE TABLE {schema}.{name}(marker text)", row_scratch=capability)
    try:
        yield capability
    finally:
        await admitted.drop_stage(schema, name)
        admitted.finish_row_scratch(capability)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["forged", "foreign_scope", "unregistered", "retired"])
async def test_capability_rejects_unissued_foreign_empty_or_retired_identity(monkeypatch, damage):
    async with parallel_preparation(monkeypatch) as (_sql, _database, _schema, _authority, _scope):
        capability = admitted.issue_row_scratch()
        supplied = admitted._RowScratchCapability() if damage == "forged" else capability
        token = None
        if damage == "foreign_scope":
            token = admitted._ADMISSION.set(None)
        elif damage == "retired":
            admitted.finish_row_scratch(capability)
        try:
            with pytest.raises(RuntimeError, match="scratch capability"):
                async with admitted.native_transaction(row_scratch=supplied):
                    pytest.fail("invalid capability reached payload work")
        finally:
            if token is not None:
                admitted._ADMISSION.reset(token)
            if damage != "retired":
                admitted.finish_row_scratch(capability)


@pytest.mark.asyncio
async def test_capability_cannot_be_swapped_inherited_or_enter_existing_broad_transaction(monkeypatch):
    async with (
        parallel_preparation(monkeypatch) as (_sql, _database, schema, _authority, _scope),
        registered_scratch(schema, "first_private") as first,
        registered_scratch(schema, "second_private") as second,
    ):
        async with admitted.native_transaction():
            with pytest.raises(RuntimeError, match="replace transaction locks"):
                async with admitted.native_transaction(row_scratch=first):
                    pytest.fail("selective mode replaced broad locks")
        async with admitted.native_transaction(row_scratch=first):
            with pytest.raises(RuntimeError, match="replace transaction locks"):
                async with admitted.native_transaction(row_scratch=second):
                    pytest.fail("another runner replaced the selected capability")
            with pytest.raises(RuntimeError, match="scratch capability"):
                await asyncio.create_task(admitted.tuned_status("SELECT 1"))
            with pytest.raises(RuntimeError, match="scratch capability"):
                await asyncio.create_task(admitted.drop_stage(schema, "second_private"))
            with pytest.raises(RuntimeError, match="scratch capability"):
                await asyncio.create_task(
                    admitted.create_stage_sql(
                        schema,
                        "inherited_private",
                        f"CREATE TABLE {schema}.inherited_private(marker text)",
                        row_scratch=first,
                    )
                )


@pytest.mark.asyncio
async def test_replaced_physical_oid_is_rejected_and_preserved_before_payload(monkeypatch):
    async with (
        parallel_preparation(monkeypatch) as (_sql, database, schema, _authority, scope),
        registered_scratch(schema, "replaced_private") as capability,
    ):
        original_oid = scope.owned_oids["replaced_private"]
        await database.status(f"ALTER TABLE {schema}.replaced_private RENAME TO preserved_private")
        await database.status(f"CREATE TABLE {schema}.replaced_private(marker text)")
        replacement_oid = await database.scalar(f"SELECT '{schema}.replaced_private'::regclass::oid::bigint")
        try:
            with pytest.raises(RuntimeError, match="relation_changed"):
                async with admitted.native_transaction(row_scratch=capability):
                    pytest.fail("replacement reached payload work")
            assert (
                await database.scalar(f"SELECT '{schema}.replaced_private'::regclass::oid::bigint") == replacement_oid
            )
            assert scope.row_scratch_owners["replaced_private"][0] == original_oid
        finally:
            await database.status(f"DROP TABLE {schema}.replaced_private")
            await database.status(f"ALTER TABLE {schema}.preserved_private RENAME TO replaced_private")
