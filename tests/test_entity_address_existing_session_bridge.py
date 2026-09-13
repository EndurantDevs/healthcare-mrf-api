# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for borrowing a coordinator transaction during address cutover."""

from __future__ import annotations

import importlib
import os

import pytest

from db.connection import Database
from tests.test_entity_address_unified_publication_db import (
    _prepare_live_and_stage,
    _StageTable,
    _temporary_schema,
)

entity_address_unified = importlib.import_module("process.entity_address_unified")


def _cutover_plan(schema: str):
    return entity_address_unified._entity_address_cutover_plan(
        schema,
        _StageTable,
        {},
        partial_support_patch=False,
        affected_group_table="",
        context={},
    )


async def _assert_callback_connection(
    database: Database,
    caller_session,
    caller_driver: object,
    caller_xid: str,
    observed_drivers: list[object],
) -> None:
    binding = database._transaction_binding()
    assert binding is not None and binding.session is caller_session
    raw_connection = await (await binding.session.connection()).get_raw_connection()
    observed_drivers.append(raw_connection.driver_connection)
    assert raw_connection.driver_connection is caller_driver
    assert await database.scalar("SELECT pg_current_xact_id()::text") == caller_xid


def _callbacks(
    database: Database,
    schema: str,
    caller_session,
    caller_driver: object,
    caller_xid: str,
    observed_drivers: list[object],
):
    async def verify_local_state() -> None:
        await _assert_callback_connection(database, caller_session, caller_driver, caller_xid, observed_drivers)

    async def record_address_receipt() -> None:
        await _assert_callback_connection(database, caller_session, caller_driver, caller_xid, observed_drivers)
        await database.status(f"INSERT INTO {schema}.adoption_receipt (marker) VALUES ('written');")

    return entity_address_unified.EntityAddressCutoverCallbacks(
        before_cutover=verify_local_state,
        after_publish=record_address_receipt,
    )


async def _run_borrowed_cutover_then_rollback(database: Database, schema: str) -> tuple[object, list[object]]:
    assert database.session_factory is not None
    caller_session = database.session_factory()
    observed_drivers: list[object] = []
    try:
        with pytest.raises(RuntimeError, match="caller rollback"):
            async with caller_session.begin():
                caller_connection = await caller_session.connection()
                caller_raw_connection = await caller_connection.get_raw_connection()
                caller_driver = caller_raw_connection.driver_connection
                caller_xid = await caller_session.scalar(database.text("SELECT pg_current_xact_id()::text"))
                swaps, patches, relation_names, required_names = _cutover_plan(schema)
                async with database.bind_existing_session(caller_session):
                    await entity_address_unified._run_entity_address_cutover(
                        schema,
                        swaps,
                        patches,
                        relation_names,
                        required_names,
                        {"address_alias_generation": 0},
                        callbacks=_callbacks(
                            database,
                            schema,
                            caller_session,
                            caller_driver,
                            caller_xid,
                            observed_drivers,
                        ),
                        require_caller_owned_transaction=True,
                    )
                    assert await database.scalar(f"SELECT count(*) FROM {schema}.adoption_receipt;") == 1
                    assert caller_session.in_transaction()
                    raise RuntimeError("caller rollback")
    finally:
        await caller_session.close()
    return caller_driver, observed_drivers


async def _expect_bridge_rejected(database: Database, session, match: str) -> None:
    bridge = database.bind_existing_session(session)
    with pytest.raises(RuntimeError, match=match):
        await bridge.__aenter__()


async def _assert_inactive_and_identity_rejections(database: Database) -> None:
    assert database.session_factory is not None
    inactive_session = database.session_factory()
    try:
        await _expect_bridge_rejected(database, inactive_session, "active caller transaction")
    finally:
        await inactive_session.close()

    caller_session = database.session_factory()
    try:
        async with caller_session.begin():
            unconnected_database = Database()
            async with unconnected_database.bind_existing_session(caller_session):
                assert unconnected_database.engine is None
                assert (
                    await unconnected_database.scalar("SELECT current_database()") == os.environ["HLTHPRT_DB_DATABASE"]
                )
            assert unconnected_database.engine is None

            wrong_database = Database()
            wrong_database._database_override = "different_test_database"
            await _expect_bridge_rejected(wrong_database, caller_session, "database identity")
            assert wrong_database.engine is None
    finally:
        await caller_session.close()


async def _assert_existing_binding_rejections(database: Database) -> None:
    assert database.session_factory is not None
    caller_session = database.session_factory()
    other_database = Database()
    await other_database.connect()
    try:
        async with caller_session.begin():
            async with database.bind_existing_session(caller_session):
                binding = database._transaction_binding()
                assert binding is not None and binding.session is caller_session
                await _expect_bridge_rejected(database, caller_session, "transaction is already bound")
                assert database._transaction_binding() is binding
            assert database._transaction_binding() is None

        async with caller_session.begin(), other_database.transaction():
            other_binding = other_database._transaction_binding()
            assert other_binding is not None
            await _expect_bridge_rejected(database, caller_session, "transaction is already bound")
            assert other_database._transaction_binding() is other_binding
    finally:
        await caller_session.close()
        await other_database.disconnect()


@pytest.mark.asyncio
async def test_borrowed_session_keeps_native_cutover_and_outer_rollback_atomic(monkeypatch):
    async with _temporary_schema() as (database, schema):
        monkeypatch.setattr(entity_address_unified, "db", database)
        await _prepare_live_and_stage(database, schema)
        await database.status(f"CREATE TABLE {schema}.adoption_receipt (marker text NOT NULL);")

        caller_driver, observed_drivers = await _run_borrowed_cutover_then_rollback(database, schema)

        assert observed_drivers == [caller_driver, caller_driver]
        assert database._transaction_binding() is None
        assert await database.scalar(f"SELECT marker FROM {schema}.entity_address_unified;") == "old"
        assert await database.scalar(f"SELECT count(*) FROM {schema}.adoption_receipt;") == 0


@pytest.mark.asyncio
async def test_existing_session_bridge_rejects_ambiguous_or_unowned_contexts():
    async with _temporary_schema() as (database, _schema):
        await _assert_inactive_and_identity_rejections(database)
        await _assert_existing_binding_rejections(database)
