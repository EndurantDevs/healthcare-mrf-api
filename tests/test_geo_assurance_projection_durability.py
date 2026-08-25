# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transaction and re-import contracts for provider geo assurance."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_geo_projection as projection
from api import ptg2_serving as serving
from api.ptg2_geo_policy import (
    provider_address_identity_coherence_sql,
    provider_address_point_coherence_sql,
)
from db.connection import Database
from tests.ptg2_serving_address_evidence_postgres_support import (
    _schema_sql,
    _temporary_schema,
)
from tests.test_geo_assurance_projection import (
    _insert_projection_addresses,
    _insert_projection_evidence,
    _insert_projection_references,
    entity_address_unified,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("current_projection_available", "expected_force"),
    ((True, False), (False, True)),
)
async def test_projection_receipt_locks_sources_and_forces_stale_reimports(
    monkeypatch,
    current_projection_available,
    expected_force,
):
    """Require locking before projection and full work for stale imports."""

    events: list[str] = []

    class FakeDB:
        @asynccontextmanager
        async def transaction(self):
            yield

        async def status(self, statement):
            events.append(str(statement))
            return 7 if "UPDATE fixture.stage AS target" in str(statement) else 0

        async def scalar(self, statement):
            statement = str(statement)
            events.append(statement)
            if statement.lstrip().startswith("SELECT EXISTS"):
                return current_projection_available
            if "SELECT COUNT(*)" in statement:
                return 0
            if "INSERT INTO fixture.entity_address_geo_assurance_state" in statement:
                return 42
            raise AssertionError(f"unexpected scalar SQL: {statement}")

    monkeypatch.setattr(entity_address_unified, "db", FakeDB())
    monkeypatch.setattr(
        entity_address_unified,
        "_entity_address_sql_settings",
        lambda: [("lock_timeout", "1s")],
    )
    progress = Mock()
    monkeypatch.setattr(entity_address_unified, "enqueue_live_progress", progress)
    projection_context_by_field: dict = {}
    assert await entity_address_unified._materialize_geo_assurance(
        "fixture",
        "stage",
        force=False,
        context=projection_context_by_field,
        run_id="run-1",
        stage_rows=7,
    ) == 7

    settings_at = next(i for i, sql in enumerate(events) if "SET LOCAL lock_timeout" in sql)
    lock_at = next(i for i, sql in enumerate(events) if sql.startswith("LOCK TABLE"))
    update_sql = next(sql for sql in events if "UPDATE fixture.stage AS target" in sql)
    assert settings_at < lock_at
    assert "fixture.npi_address" in events[lock_at]
    assert "tiger.zcta5" in events[lock_at]
    assert ("WHERE TRUE" in update_sql) is expected_force
    assert (
        projection_context_by_field["geo_assurance_forced_full_projection"]
        is expected_force
    )
    assert projection_context_by_field["invalid_geo_assurance_rows"] == 0
    assert [call.kwargs["done"] for call in progress.call_args_list] == [0, 7]
    assert "row(s)" not in progress.call_args_list[0].kwargs["message"]
    assert "7 row(s)" in progress.call_args_list[1].kwargs["message"]


@pytest.mark.asyncio
async def test_projection_transaction_settings_preserve_permission_boundary(
    monkeypatch,
):
    class FakeDB:
        status = AsyncMock(
            side_effect=(
                RuntimeError("permission denied to set parameter lock_timeout"),
                RuntimeError("database unavailable"),
            )
        )

        @asynccontextmanager
        async def transaction(self):
            yield

    monkeypatch.setattr(entity_address_unified, "db", FakeDB())
    monkeypatch.setattr(
        entity_address_unified,
        "_entity_address_sql_settings",
        lambda: [("lock_timeout", "1s"), ("work_mem", "1MB")],
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        await entity_address_unified._apply_entity_address_transaction_settings()


@pytest.mark.asyncio
async def test_projection_stage_housekeeping_is_fail_closed(monkeypatch):
    class FakeStage:
        __tablename__ = "entity_address_unified_stage"
        __my_additional_indexes__ = [
            {"name": "npi", "index_elements": ("npi",)},
        ]

    status = AsyncMock()
    execute_ddl = AsyncMock()
    monkeypatch.setattr(
        entity_address_unified,
        "db",
        SimpleNamespace(status=status, execute_ddl=execute_ddl),
    )
    assert await entity_address_unified._drop_stage_secondary_indexes(
        FakeStage,
        "fixture",
    ) == 1
    assert "DROP INDEX IF EXISTS fixture." in status.await_args.args[0]

    persistence = AsyncMock(side_effect=(None, "u", "p"))
    promote = AsyncMock()
    monkeypatch.setattr(
        entity_address_unified,
        "_stage_table_persistence",
        persistence,
    )
    monkeypatch.setattr(
        entity_address_unified,
        "_ensure_promoted_stage_logged",
        promote,
    )
    with pytest.raises(RuntimeError, match="does not exist"):
        await entity_address_unified._compact_geo_assurance_stage(
            "fixture",
            "entity_address_unified_stage",
        )
    assert (
        await entity_address_unified._compact_geo_assurance_stage(
            "fixture",
            "entity_address_unified_stage",
        )
        == "set_logged"
    )
    assert (
        await entity_address_unified._compact_geo_assurance_stage(
            "fixture",
            "entity_address_unified_stage",
        )
        == "vacuum_full"
    )
    promote.assert_awaited_once_with("fixture", "entity_address_unified_stage")
    execute_ddl.assert_awaited_once_with(
        "VACUUM (FULL, ANALYZE) fixture.entity_address_unified_stage;"
    )


async def _activate_projection_state(database, schema: str, projected_rows: int) -> None:
    """Record and activate one projection candidate."""

    candidate_oid = await database.scalar(
        _schema_sql(
            entity_address_unified._record_geo_assurance_candidate_sql(
                schema,
                "entity_address_unified",
                projected_rows,
            ),
            schema,
        )
    )
    assert candidate_oid is not None
    active_oid = await database.scalar(
        _schema_sql(
            entity_address_unified._activate_geo_assurance_candidate_sql(schema),
            schema,
        )
    )
    assert active_oid == candidate_oid
    assert await database.scalar(
        f"SELECT candidate_table_oid IS NULL "
        f"FROM {schema}.entity_address_geo_assurance_state"
    ) is True


def _runtime_assurance_sql(schema: str) -> tuple[str, str, str]:
    return (
        _schema_sql(serving._ptg2_geo_evidence_level_sql("addr"), schema),
        _schema_sql(
            provider_address_identity_coherence_sql("addr", schema_name=schema),
            schema,
        ),
        _schema_sql(
            provider_address_point_coherence_sql("addr", schema_name=schema),
            schema,
        ),
    )


async def _runtime_assurance(database, schema: str, sql: tuple[str, str, str]):
    return tuple(
        await database.first(
            f"SELECT {sql[0]}, {sql[1]}, {sql[2]} "
            f"FROM {schema}.entity_address_unified AS addr "
            "WHERE location_key = 'nppes'"
        )
    )


async def _materialize_active_projection(database, schema: str) -> None:
    await database.status(
        f"UPDATE {schema}.entity_address_unified "
        "SET lat = 42.0, long = -83.0 WHERE location_key = 'nppes'"
    )
    materialize_sql = _schema_sql(
        entity_address_unified._materialize_geo_assurance_sql(
            schema,
            "entity_address_unified",
            force=True,
        ),
        schema,
    )
    assert await database.status(materialize_sql) == 5
    await _activate_projection_state(database, schema, 5)


async def _replace_npi_source_table(database, schema: str) -> None:
    await database.status(f"ALTER TABLE {schema}.npi_address RENAME TO npi_address_old")
    await database.status(
        f"CREATE TABLE {schema}.npi_address "
        f"(LIKE {schema}.npi_address_old INCLUDING ALL)"
    )
    await database.status(
        f"INSERT INTO {schema}.npi_address SELECT * FROM {schema}.npi_address_old"
    )


@pytest.mark.asyncio
async def test_reimport_invalidates_and_recovers_projection():
    """Prove a source-table swap cannot leave stale projected assurance live."""

    async with _temporary_schema() as (database, schema):
        await _insert_projection_references(database, schema)
        await _insert_projection_addresses(database, schema)
        await _insert_projection_evidence(database, schema)
        await _materialize_active_projection(database, schema)
        runtime_sql = _runtime_assurance_sql(schema)
        await database.status(
            f"UPDATE {schema}.entity_address_unified "
            "SET geo_evidence_source_id = 2, geo_identity_coherent = false, "
            "geo_point_coherent = false WHERE location_key = 'nppes'"
        )
        assert await _runtime_assurance(database, schema, runtime_sql) == (
            "multi_issuer_marketplace_address",
            False,
            False,
        )
        assert await database.scalar(
            _schema_sql(
                entity_address_unified._record_geo_assurance_candidate_sql(
                    schema,
                    "entity_address_unified",
                    5,
                ),
                schema,
            )
        ) is not None

        await _replace_npi_source_table(database, schema)
        assert await _runtime_assurance(database, schema, runtime_sql) == (
            "nppes_registry_address",
            True,
            True,
        )
        activation_sql = entity_address_unified._activate_geo_assurance_candidate_sql(
            schema
        )
        assert await database.scalar(_schema_sql(activation_sql, schema)) is None
        await _materialize_active_projection(database, schema)
        assert await _runtime_assurance(database, schema, runtime_sql) == (
            "nppes_registry_address",
            True,
            True,
        )


async def _fail_projection_validation(database, schema, _requested_schema, _table):
    await database.status(
        f"UPDATE {schema}.entity_address_geo_assurance_state "
        "SET candidate_projected_rows = 777"
    )
    raise RuntimeError("induced validation failure")


def _patch_failed_projection(monkeypatch, database, schema: str) -> None:
    original_lock_sql = projection.projection_dependency_lock_sql
    original_signature_sql = projection.projection_relation_signature_sql
    monkeypatch.setattr(entity_address_unified, "db", database)
    monkeypatch.setattr(
        projection,
        "projection_dependency_lock_sql",
        lambda _schema: _schema_sql(original_lock_sql(schema), schema),
    )
    monkeypatch.setattr(
        projection,
        "projection_relation_signature_sql",
        lambda _schema: _schema_sql(original_signature_sql(schema), schema),
    )
    monkeypatch.setattr(
        entity_address_unified,
        "_materialize_geo_assurance_sql",
        lambda *_args, **_kwargs: (
            f"UPDATE {schema}.entity_address_unified "
            "SET geo_evidence_source_id = 0, geo_identity_coherent = false, "
            "geo_point_coherent = false, geo_assurance_version = 1 "
            "WHERE location_key = 'rollback'"
        ),
    )
    monkeypatch.setattr(
        entity_address_unified,
        "_validate_geo_assurance_projection",
        partial(_fail_projection_validation, database, schema),
    )


@pytest.mark.asyncio
async def test_projection_validation_failure_rolls_back_cells_and_state(monkeypatch):
    """Require cell and receipt rollback when projection validation fails."""

    async with _temporary_schema() as (database, schema):
        await database.status(
            f"INSERT INTO {schema}.entity_address_unified "
            "(location_key, type, checksum) VALUES ('rollback', 'practice', 1)"
        )
        await database.status(
            f"UPDATE {schema}.entity_address_geo_assurance_state "
            "SET candidate_projected_rows = 99"
        )
        _patch_failed_projection(monkeypatch, database, schema)
        with pytest.raises(RuntimeError, match="induced validation failure"):
            await entity_address_unified._materialize_geo_assurance(
                schema,
                "entity_address_unified",
                force=True,
                context={},
                run_id="",
                stage_rows=1,
            )

        assert await database.scalar(
            f"SELECT geo_assurance_version IS NULL "
            f"FROM {schema}.entity_address_unified WHERE location_key = 'rollback'"
        ) is True
        assert await database.scalar(
            f"SELECT candidate_projected_rows "
            f"FROM {schema}.entity_address_geo_assurance_state"
        ) == 99


async def _hold_projection_receipt_lock(
    database,
    schema: str,
    candidate_sql: str,
    lock_acquired: asyncio.Event,
    release_lock: asyncio.Event,
) -> None:
    async with database.transaction():
        await database.status(
            _schema_sql(projection.projection_dependency_lock_sql(schema), schema)
        )
        assert await database.scalar(candidate_sql) is not None
        lock_acquired.set()
        await release_lock.wait()


@pytest.mark.asyncio
async def test_projection_receipt_lock_blocks_source_swap_and_stale_activation():
    """Require a source swap to wait and invalidate its stale candidate."""

    async with _temporary_schema() as (database, schema):
        lock_acquired = asyncio.Event()
        release_lock = asyncio.Event()
        candidate_sql = _schema_sql(
            entity_address_unified._record_geo_assurance_candidate_sql(
                schema,
                "entity_address_unified",
                0,
            ),
            schema,
        )
        lock_task = asyncio.create_task(
            _hold_projection_receipt_lock(
                database,
                schema,
                candidate_sql,
                lock_acquired,
                release_lock,
            )
        )
        await asyncio.wait_for(lock_acquired.wait(), timeout=2)
        publisher = Database()
        await publisher.connect()
        swap_task = asyncio.create_task(
            publisher.status(
                f"ALTER TABLE {schema}.npi_address RENAME TO npi_address_reimported"
            )
        )
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(swap_task), timeout=0.15)
            release_lock.set()
            await asyncio.wait_for(lock_task, timeout=2)
            await asyncio.wait_for(swap_task, timeout=2)
            activation_sql = (
                entity_address_unified._activate_geo_assurance_candidate_sql(schema)
            )
            assert await database.scalar(_schema_sql(activation_sql, schema)) is None
        finally:
            release_lock.set()
            if not lock_task.done():
                lock_task.cancel()
            if not swap_task.done():
                swap_task.cancel()
            await publisher.disconnect()
