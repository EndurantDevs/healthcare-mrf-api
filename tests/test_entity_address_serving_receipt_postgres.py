# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual ordinary address swaps retain the common receipt's deferred publication gates."""

import importlib
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from db.connection import Database
from process import entity_address_serving_receipt as continuity
from process import provider_directory_cms_serving_receipt as receipts
from tests.test_provider_directory_cms_serving_receipt_postgres import _database, _publish_initial

native = importlib.import_module("process.entity_address_unified")


class _Stage:
    __tablename__ = "entity_address_unified_stage"
    __my_additional_indexes__ = []


@asynccontextmanager
async def _ordinary_publication(monkeypatch, *, history=True):
    async with _database(monkeypatch) as (engine, schema):
        initial = await _publish_initial(engine, schema) if history else None
        async with engine.begin() as connection:
            await connection.execute(
                text(f"ALTER TABLE {schema}.address_alias_state_v1 ADD COLUMN schema_version int DEFAULT 2")
            )
            await connection.execute(
                text(f"ALTER TABLE {schema}.address_alias_state_v1 ADD COLUMN active_ruleset_version int DEFAULT 1")
            )
            if not history:
                await connection.execute(
                    text(f"INSERT INTO {schema}.address_alias_state_v1 (singleton,generation) VALUES (true,0)")
                )
            await connection.execute(text(f"INSERT INTO {schema}.entity_address_unified VALUES (1)"))
            await connection.execute(text(f"CREATE UNLOGGED TABLE {schema}.{_Stage.__tablename__} (synthetic_id int)"))
            await connection.execute(text(f"INSERT INTO {schema}.{_Stage.__tablename__} VALUES (2)"))
        database = Database()
        await database.connect()
        monkeypatch.setattr(native, "db", database)
        # Geo-assurance has independent native tests; retain the actual swap, authority and receipt SQL here.
        monkeypatch.setattr(
            native,
            "_activate_geo_assurance_candidate_sql",
            lambda name: f"SELECT to_regclass('{name}.entity_address_unified')::oid::bigint",
        )
        try:
            yield database, engine, schema, initial
        finally:
            await database.disconnect()


async def _publish(database, schema):
    context_by_field = {"address_alias_generation": 0}
    await native._publish_staged_entity_address_tables(
        schema, _Stage, {}, partial_support_patch=False, affected_group_table="", context=context_by_field
    )
    return context_by_field


async def _assert_incumbent(engine, schema, initial):
    async with engine.connect() as connection:
        assert await receipts.read_current_receipt(connection, schema) == initial
        assert await connection.scalar(text(f"SELECT synthetic_id FROM {schema}.entity_address_unified")) == 1
        assert await connection.scalar(text(f"SELECT synthetic_id FROM {schema}.{_Stage.__tablename__}")) == 2
        assert (
            await connection.scalar(text(f"SELECT count(*) FROM {schema}.provider_directory_cms_serving_receipt")) == 1
        )


@pytest.mark.asyncio
async def test_ordinary_swap_advances_address_and_common_receipt_in_one_transaction(monkeypatch):
    async with _ordinary_publication(monkeypatch) as (database, engine, schema, initial):
        context = await _publish(database, schema)
        async with engine.connect() as connection:
            current = await receipts.read_current_receipt(connection, schema)
            assert current["payload"]["predecessor_receipt_id"] == initial["receipt_id"]
            assert current["payload"]["expected_incumbent"] == initial["payload"]["desired_datasets"][0]
            assert current["payload"]["address"]["local_generation"] == 2
            assert context["result_generation"]["local_generation"] == 2
            assert (
                current["payload"]["address"]["relation_oids"][0] != initial["payload"]["address"]["relation_oids"][0]
            )
            for key in (
                "cms",
                "selection",
                "profile",
                "doctors",
                "desired_datasets",
                "alias_generation",
                "overlay_oid",
            ):
                assert current["payload"][key] == initial["payload"][key]
            assert await connection.scalar(text(f"SELECT synthetic_id FROM {schema}.entity_address_unified")) == 2
            assert await connection.scalar(text(f"SELECT synthetic_id FROM {schema}.entity_address_unified_old")) == 1
            assert await receipts.verify_historical_receipt(
                connection, schema, initial["receipt_id"], initial["payload"]
            )


@pytest.mark.asyncio
async def test_empty_common_history_keeps_ordinary_publication(monkeypatch):
    async with _ordinary_publication(monkeypatch, history=False) as (database, engine, schema, _initial):
        context = await _publish(database, schema)
        assert context["result_generation"]["local_generation"] == 1
        async with engine.connect() as connection:
            assert (
                await connection.scalar(text(f"SELECT count(*) FROM {schema}.provider_directory_cms_serving_receipt"))
                == 0
            )
            assert await connection.scalar(text(f"SELECT synthetic_id FROM {schema}.entity_address_unified")) == 2


@pytest.mark.asyncio
async def test_receipt_failure_rolls_back_actual_swap_and_address_authority(monkeypatch):
    async with _ordinary_publication(monkeypatch) as (database, engine, schema, initial):
        append = receipts.append_serving_receipt

        async def fail_after_append(session, name, payload):
            await append(session, name, payload)
            raise RuntimeError("synthetic receipt write failure")

        monkeypatch.setattr(continuity.receipts, "append_serving_receipt", fail_after_append)
        with pytest.raises(RuntimeError, match="synthetic receipt write failure"):
            await _publish(database, schema)
        await _assert_incumbent(engine, schema, initial)


@pytest.mark.asyncio
async def test_borrowed_transaction_does_not_commit_common_successor(monkeypatch):
    async with _ordinary_publication(monkeypatch) as (database, engine, schema, initial):
        with pytest.raises(RuntimeError, match="synthetic outer rollback"):
            async with database.transaction():
                await database.status("SET LOCAL lock_timeout='7s'")
                await _publish(database, schema)
                assert await database.scalar("SELECT current_setting('lock_timeout')") == "7s"
                assert (
                    await database.scalar(text(f"SELECT count(*) FROM {schema}.provider_directory_cms_serving_receipt"))
                    == 2
                )
                async with engine.connect() as observer:
                    assert (
                        await observer.scalar(
                            text(f"SELECT count(*) FROM {schema}.provider_directory_cms_serving_receipt")
                        )
                        == 1
                    )
                raise RuntimeError("synthetic outer rollback")
        await _assert_incumbent(engine, schema, initial)


@pytest.mark.asyncio
@pytest.mark.parametrize("lock", ["receipt", "doctors"])
async def test_competing_common_or_native_writer_fails_before_swap(monkeypatch, lock):
    async with _ordinary_publication(monkeypatch) as (database, engine, schema, initial):
        async with engine.begin() as other:
            statement = (
                f"LOCK TABLE {schema}.provider_directory_cms_serving_receipt IN ROW EXCLUSIVE MODE"
                if lock == "receipt"
                else f"SELECT 1 FROM {schema}.reference_family_result_generation WHERE importer_id='cms-doctors' FOR UPDATE"
            )
            await other.execute(text(statement))
            with pytest.raises(DBAPIError) as error:
                await _publish(database, schema)
            assert native.postgres_sqlstate(error.value) == "55P03"
        await _assert_incumbent(engine, schema, initial)


@pytest.mark.asyncio
async def test_inconsistent_existing_native_history_fails_before_address_swap(monkeypatch):
    async with _ordinary_publication(monkeypatch) as (database, engine, schema, initial):
        async with engine.begin() as connection:
            await connection.execute(
                text(f"ALTER TABLE {schema}.cms_doctor_education RENAME TO cms_doctor_education_retired")
            )
            await connection.execute(text(f"CREATE TABLE {schema}.cms_doctor_education (synthetic_id int)"))
        with pytest.raises(RuntimeError, match="native_authority_drift"):
            await _publish(database, schema)
        async with engine.connect() as connection:
            assert await connection.scalar(text(f"SELECT synthetic_id FROM {schema}.entity_address_unified")) == 1
            assert await connection.scalar(text(f"SELECT synthetic_id FROM {schema}.{_Stage.__tablename__}")) == 2
            assert (
                await connection.scalar(text(f"SELECT count(*) FROM {schema}.provider_directory_cms_serving_receipt"))
                == 1
            )
            assert await receipts.verify_historical_receipt(
                connection, schema, initial["receipt_id"], initial["payload"]
            )
