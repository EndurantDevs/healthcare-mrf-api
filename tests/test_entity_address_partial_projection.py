# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Partial overlay caching preserves endpoint fences and source completeness."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_partial_projection as projection
from tests.entity_address_partial_projection_support import AVAILABLE, native, projection_database


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared", [False, True])
async def test_projection_load_keeps_absent_or_completed_context_untouched(prepared):
    """Repeated preparation must not replace the context already used by readers."""
    active = projection._Projection('"data"."inputs"', '"data"."scope"', '"data"."context"', ("selected",), "run")
    active.context_relation = active.context_target
    active.created_relations.append(active.context_target)
    status = AsyncMock()
    sql = SimpleNamespace(db=SimpleNamespace(status=status))
    token = projection._ACTIVE.set(active if prepared else None)
    try:
        await projection.prepare_load(sql, "data", AVAILABLE, "affected_groups")
        status.assert_not_called()
        assert projection._ACTIVE.get() is (active if prepared else None)
        assert active.context_relation == active.context_target
        assert active.created_relations == [active.context_target]
    finally:
        projection._ACTIVE.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("desired", [False, True])
async def test_partial_projection_preserves_scoped_and_all_source_rows(monkeypatch, desired):
    from tests.entity_address_partial_projection_support import assert_projection_parity

    await assert_projection_parity(monkeypatch, desired)


@pytest.mark.asyncio
async def test_partial_projection_rolls_back_create_and_cleans_cancelled_consumer(monkeypatch):
    async with projection_database(monkeypatch) as (database, schema):
        with pytest.raises(asyncio.CancelledError):
            async with projection.scoped_projection(
                native, schema, "cancel_run", AVAILABLE, source_ids=["selected"], run_id="run-a"
            ) as relation:
                raise asyncio.CancelledError()
        assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=relation)
        async with database.transaction():
            async with projection.scoped_projection(
                native, schema, "bound_run", AVAILABLE, source_ids=["selected"], run_id="run-a"
            ) as declined:
                assert declined is None


def test_projection_plan_drives_existing_scope_and_membership_indexes():
    statement = projection._raw_sql(native, "sample", source_ids=["selected"], run_id="run-a")
    assert "overlay.source_id=scope.source_id AND overlay.last_seen_run_id=scope.run_id" in statement
    assert "OFFSET 0" in statement and "LIMIT 1" in statement
    assert "SELECT overlay.*" not in statement and "npi_address" not in statement
    assert "HAVING count(*)=1" in statement


@pytest.mark.asyncio
async def test_projection_keeps_membership_guard_on_cached_reads(monkeypatch):
    async with projection_database(monkeypatch, desired=True) as (database, schema):
        async with projection.scoped_projection(
            native, schema, "membership_run", AVAILABLE, source_ids=["selected"], run_id="run-a"
        ):
            await database.status(
                f"DELETE FROM \"{schema}\".provider_directory_dataset_resource WHERE resource_id='one'"
            )
            groups = projection.group_input_sql(native, schema)
            assert await database.scalar(f"SELECT count(*) FROM ({groups}) grouped") == 1
            await projection.prepare_load(native, schema, AVAILABLE, "affected_groups")
            selected = projection.cached_overlay_source(
                native, schema, AVAILABLE, affected_group_table="affected_groups"
            )
            assert await database.scalar(f"SELECT count(*) FROM ({selected}) selected") == 2


@pytest.mark.asyncio
async def test_projection_context_failure_preserves_preexisting_relation(monkeypatch):
    monkeypatch.setattr(
        native.address_batches, "scratch_name", lambda sql, table, purpose: sql._archived_identifier(table, purpose)
    )
    async with projection_database(monkeypatch) as (database, schema):
        await database.status(f'CREATE TABLE "{schema}".collision_run_pd_npi(sentinel int)')
        await database.status(f'INSERT INTO "{schema}".collision_run_pd_npi VALUES(7)')
        with pytest.raises(Exception, match="already exists"):
            async with projection.scoped_projection(
                native, schema, "collision_run", AVAILABLE, source_ids=["selected"], run_id="run-a"
            ):
                await projection.prepare_load(native, schema, AVAILABLE, "affected_groups")
        assert await database.scalar(f'SELECT sentinel FROM "{schema}".collision_run_pd_npi') == 7
        assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=f"{schema}.collision_run_pd_projection")
        assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=f"{schema}.collision_run_pd_scope")


@pytest.mark.asyncio
async def test_partial_projection_plan_prunes_history_and_estimates_pin_scope(monkeypatch):
    from tests.entity_address_partial_projection_support import assert_projection_plan

    await assert_projection_plan(monkeypatch)


@pytest.mark.asyncio
async def test_partial_projection_without_npi_index_keeps_original_sql(monkeypatch):
    async with projection_database(monkeypatch, desired=True) as (database, schema):
        await database.status(f'DROP INDEX "{schema}".desired_overlay_npi_idx')
        async with projection.scoped_projection(
            native, schema, "no_index_run", AVAILABLE, source_ids=["selected"], run_id="run-a"
        ) as declined:
            assert declined is None
            assert (
                projection.cached_overlay_source(native, schema, AVAILABLE, source_ids=["selected"], run_id="run-a")
                is None
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("commit_number", [1, 2])
async def test_partial_projection_commit_cancellation_retires_created_tables(monkeypatch, commit_number):
    from contextlib import asynccontextmanager

    async with projection_database(monkeypatch, desired=True) as (database, schema):
        transaction = database.transaction
        status = database.status
        commits = []
        created_relations = []

        async def record_created(statement, **params):
            result = await status(statement, **params)
            if statement.startswith("CREATE UNLOGGED TABLE "):
                created_relations.append(statement.split()[3])
            return result

        @asynccontextmanager
        async def cancel_after_commit():
            async with transaction() as session:
                yield session
            commits.append(None)
            if len(commits) == commit_number:
                raise asyncio.CancelledError()

        monkeypatch.setattr(database, "transaction", cancel_after_commit)
        monkeypatch.setattr(database, "status", record_created)
        with pytest.raises(asyncio.CancelledError):
            async with projection.scoped_projection(
                native, schema, "commit_run", AVAILABLE, source_ids=["selected"], run_id="run-a"
            ):
                await projection.prepare_load(native, schema, AVAILABLE, "affected_groups")
        assert created_relations
        for relation in created_relations:
            assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=relation)


@pytest.mark.asyncio
async def test_partial_scratch_names_follow_control_context_and_reset():
    import importlib

    directory = importlib.import_module("process.provider_directory_fhir")

    @native.address_batches.scope
    async def names(ctx, task=None):
        return [
            native.address_batches.scratch_name(native, "local_stage", purpose)
            for purpose in ("_pd_scope", "_pd_projection", "_pd_npi")
        ]

    named = await names(
        {"context": {"control_run_id": "scope-owner"}, "control_run_id": "outer-owner"}, task={"run_id": "task-owner"}
    )
    expected_token = directory._address_overlay_stage_owner_token("scope-owner")
    assert len(set(named)) == 3
    assert all(
        directory._is_address_overlay_stage_table_name(name) and name.rsplit("_", 1)[-1].startswith(expected_token)
        for name in named
    )
    unowned = await names({"context": {}}, task={"run_id": "task-owner"})
    assert all(name.startswith("local_stage_pd_") for name in unowned)
    assert len(set(unowned)) == 3
    assert not set(unowned).intersection(await names({"context": {}}))
    assert native.address_batches._OWNER.get() is None


@pytest.mark.asyncio
async def test_unowned_live_inputs_preserve_stale_and_concurrent_tables(monkeypatch):
    async with projection_database(monkeypatch) as (database, schema):
        fields = ",".join(f"{name} varchar" for name in native.address_batches.stage.ADDRESS_FIELDS)
        await database.status(
            f'CREATE TABLE "{schema}".entity_address_unified(entity_type varchar,entity_id varchar,address_key uuid,'
            f"{fields},address_sources varchar[],location_key uuid)"
        )
        await database.status(f'CREATE TABLE "{schema}".shared_stage_pd_live_inputs(sentinel int)')
        await database.status(f'INSERT INTO "{schema}".shared_stage_pd_live_inputs VALUES(7)')
        monkeypatch.setattr(native, "_provider_directory_live_source_filter_sql", lambda *args, **kwargs: "")
        async with native.address_batches._prepared_live(native, schema, "shared_stage", ["selected"]) as first:
            async with native.address_batches._prepared_live(native, schema, "shared_stage", ["selected"]) as second:
                assert first != second
                assert await database.scalar("SELECT to_regclass(:name) IS NOT NULL", name=first)
            assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=second)
            assert await database.scalar("SELECT to_regclass(:name) IS NOT NULL", name=first)
        assert await database.scalar("SELECT to_regclass(:name) IS NULL", name=first)
        assert await database.scalar(f'SELECT sentinel FROM "{schema}".shared_stage_pd_live_inputs') == 7


@pytest.mark.asyncio
async def test_control_named_projection_collision_preserves_existing_table(monkeypatch):
    import importlib

    from sqlalchemy.exc import ProgrammingError

    directory = importlib.import_module("process.provider_directory_fhir")
    existing = directory._address_overlay_stage_table_name("scope-owner")
    monkeypatch.setattr(directory, "_address_overlay_stage_table_name", lambda owner: existing)
    async with projection_database(monkeypatch, desired=True) as (database, schema):
        await database.status(f'CREATE TABLE "{schema}"."{existing}"(sentinel int)')
        await database.status(f'INSERT INTO "{schema}"."{existing}" VALUES(7)')

        @native.address_batches.scope
        async def create(ctx):
            async with projection.scoped_projection(
                native, schema, "local_stage", AVAILABLE, source_ids=["selected"], run_id="run-a"
            ):
                pytest.fail("preexisting table was accepted")

        with pytest.raises(ProgrammingError, match="already exists"):
            await create({"context": {"control_run_id": "scope-owner"}})
        assert await database.scalar(f'SELECT sentinel FROM "{schema}"."{existing}"') == 7
        assert projection.group_input_sql(native, schema) is None
