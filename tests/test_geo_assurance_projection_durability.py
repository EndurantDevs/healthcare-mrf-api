# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transaction and re-import contracts for provider geo assurance."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from uuid import uuid4

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
)
from tests.ptg2_serving_address_evidence_postgres_support import (
    _temporary_schema as _temporary_evidence_schema,
)
from tests.test_geo_assurance_projection import (
    _insert_projection_addresses,
    _insert_projection_evidence,
    _insert_projection_references,
    entity_address_unified,
)


def _assert_projection_receipt_order(events, projection_context_by_field, expected_force, progress, projected_rows):
    """Keep lock ordering, complete work and row accounting assertions together."""
    settings_at = next(i for i, sql in enumerate(events) if "SET LOCAL lock_timeout" in sql)
    lock_at = next(i for i, sql in enumerate(events) if sql.startswith("LOCK TABLE"))
    update_sql = next(sql for sql in events if "UPDATE fixture.stage AS target" in sql)
    assert settings_at < lock_at
    assert "fixture.npi_address" in events[lock_at]
    assert "tiger.zcta5" in events[lock_at]
    assert ("WHERE TRUE" in update_sql) is expected_force
    forced = projection_context_by_field["geo_assurance_forced_full_projection"]
    assert forced is expected_force
    assert projection_context_by_field["invalid_geo_assurance_rows"] == 0
    assert [call.kwargs["done"] for call in progress.call_args_list] == [0, 1]
    assert all(call.kwargs["total"] == 1 and call.kwargs["unit"] == "operations" for call in progress.call_args_list)
    assert (
        projection_context_by_field["phase_timings"]["entity-address-unified projecting geo assurance"]["rows"]
        == projected_rows
    )
    assert "row(s)" not in progress.call_args_list[0].kwargs["message"]
    assert f"{projected_rows} row(s)" in progress.call_args_list[1].kwargs["message"]


def _receipt_database(current_projection_available, events, projected_rows):
    """Keep transaction custody observable in the provider projection fixture."""

    class FakeDB:
        transaction_depth = 0

        def _transaction_binding(self):
            return self if self.transaction_depth else None

        @asynccontextmanager
        async def transaction(self):
            self.transaction_depth += 1
            try:
                yield
            finally:
                self.transaction_depth -= 1

        async def status(self, statement):
            assert self._transaction_binding() is not None
            events.append(str(statement))
            return projected_rows if "UPDATE fixture.stage AS target" in str(statement) else 0

        async def scalar(self, statement):
            assert self._transaction_binding() is not None
            statement = str(statement)
            events.append(statement)
            if statement.lstrip().startswith("SELECT EXISTS"):
                return current_projection_available
            if "SELECT COUNT(*)" in statement:
                return 0
            if "INSERT INTO fixture.entity_address_geo_assurance_state" in statement:
                return 42
            raise AssertionError(f"unexpected scalar SQL: {statement}")

    return FakeDB()


@pytest.mark.asyncio
@pytest.mark.parametrize("projected_rows", (0, 2, 7))
@pytest.mark.parametrize(
    ("current_projection_available", "expected_force"),
    ((True, True), (False, True)),
)
async def test_projection_receipt_locks_sources_and_forces_stale_reimports(
    monkeypatch,
    current_projection_available,
    expected_force,
    projected_rows,
):
    """Require locking before projection and full work for stale imports."""
    events: list[str] = []

    database = _receipt_database(current_projection_available, events, projected_rows)
    monkeypatch.setattr(entity_address_unified, "db", database)
    monkeypatch.setattr(
        entity_address_unified,
        "_entity_address_sql_settings",
        lambda: [("lock_timeout", "1s")],
    )
    progress = AsyncMock()
    monkeypatch.setattr(entity_address_unified, "_emit_phase_progress", progress)
    projection_context_by_field: dict = {}
    assert (
        await entity_address_unified._materialize_geo_assurance(
            "fixture",
            "stage",
            force=False,
            context=projection_context_by_field,
            run_id="run-1",
            stage_rows=7,
        )
        == projected_rows
    )
    assert database._transaction_binding() is None
    _assert_projection_receipt_order(events, projection_context_by_field, expected_force, progress, projected_rows)


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
    assert (
        await entity_address_unified._drop_stage_secondary_indexes(
            FakeStage,
            "fixture",
        )
        == 1
    )
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
    execute_ddl.assert_awaited_once_with("VACUUM (FULL, ANALYZE) fixture.entity_address_unified_stage;")


@asynccontextmanager
async def _temporary_schema():
    """Publish the genuine NPI generation used by each projection signature."""
    from tests.test_entity_address_snapshot_destination import _publish_npi_fixture_generation

    async with _temporary_evidence_schema() as (database, schema):
        await _publish_npi_fixture_generation(database, schema)
        yield database, schema


async def _activate_projection_state(database, schema: str, projected_rows: int) -> None:
    """Record and activate one projection candidate."""

    candidate_result = await database.scalar(
        _schema_sql(
            entity_address_unified._record_geo_assurance_candidate_sql(
                schema,
                "entity_address_unified",
                projected_rows,
            ),
            schema,
        )
    )
    assert candidate_result is not None
    candidate_version = await database.scalar(
        f"SELECT candidate_geo_assurance_version FROM {schema}.entity_address_geo_assurance_state"
    )
    active_version = await database.scalar(
        _schema_sql(
            entity_address_unified._activate_geo_assurance_candidate_sql(schema),
            schema,
        )
    )
    assert active_version == candidate_version
    assert (
        await database.scalar(f"SELECT candidate_table_oid IS NULL FROM {schema}.entity_address_geo_assurance_state")
        is True
    )


def _runtime_assurance_sql(schema: str) -> tuple[str, str, str]:
    with patch.object(serving, "PTG2_SCHEMA", schema):
        evidence = serving._ptg2_geo_evidence_level_sql("addr")
    return (
        _schema_sql(evidence, schema),
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
        f"UPDATE {schema}.entity_address_unified SET lat = 42.0, long = -83.0 WHERE location_key = 'nppes'"
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
    await database.status(f"CREATE TABLE {schema}.npi_address (LIKE {schema}.npi_address_old INCLUDING ALL)")
    await database.status(f"INSERT INTO {schema}.npi_address SELECT * FROM {schema}.npi_address_old")


@pytest.mark.asyncio
async def test_identical_named_reimport_preserves_and_rematerializes_projection():
    """Identical copies preserve semantic results; cached cells cannot override current data."""

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
            "nppes_registry_address",
            True,
            True,
        )
        assert (
            await database.scalar(
                _schema_sql(
                    entity_address_unified._record_geo_assurance_candidate_sql(
                        schema,
                        "entity_address_unified",
                        5,
                    ),
                    schema,
                )
            )
            is not None
        )

        await _replace_npi_source_table(database, schema)
        assert await _runtime_assurance(database, schema, runtime_sql) == (
            "nppes_registry_address",
            True,
            True,
        )
        activation_sql = entity_address_unified._activate_geo_assurance_candidate_sql(schema)
        assert await database.scalar(_schema_sql(activation_sql, schema)) is not None
        await _materialize_active_projection(database, schema)
        assert await _runtime_assurance(database, schema, runtime_sql) == (
            "nppes_registry_address",
            True,
            True,
        )


async def _fail_projection_validation(database, schema, _requested_schema, _table):
    await database.status(f"UPDATE {schema}.entity_address_geo_assurance_state SET candidate_projected_rows = 777")
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

    progress = AsyncMock()
    monkeypatch.setattr(entity_address_unified, "_emit_phase_progress", progress)
    async with _temporary_schema() as (database, schema):
        await database.status(
            f"INSERT INTO {schema}.entity_address_unified "
            "(location_key, type, checksum) VALUES ('rollback', 'practice', 1)"
        )
        await database.status(f"UPDATE {schema}.entity_address_geo_assurance_state SET candidate_projected_rows = 99")
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

        assert (
            await database.scalar(
                f"SELECT geo_assurance_version IS NULL "
                f"FROM {schema}.entity_address_unified WHERE location_key = 'rollback'"
            )
            is True
        )
        assert (
            await database.scalar(f"SELECT candidate_projected_rows FROM {schema}.entity_address_geo_assurance_state")
            == 99
        )
        assert progress.await_count == 1
        assert progress.call_args.kwargs["done"] == 0 and progress.call_args.kwargs["total"] == 1


async def _hold_projection_receipt_lock(
    database,
    schema: str,
    candidate_sql: str,
    lock_acquired: asyncio.Event,
    release_lock: asyncio.Event,
) -> None:
    async with database.transaction():
        await database.status(_schema_sql(projection.projection_dependency_lock_sql(schema), schema))
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
            publisher.status(f"ALTER TABLE {schema}.npi_address RENAME TO npi_address_reimported")
        )
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(swap_task), timeout=0.15)
            release_lock.set()
            await asyncio.wait_for(lock_task, timeout=2)
            await asyncio.wait_for(swap_task, timeout=2)
            activation_sql = entity_address_unified._activate_geo_assurance_candidate_sql(schema)
            assert await database.scalar(_schema_sql(activation_sql, schema)) is None
        finally:
            release_lock.set()
            if not lock_task.done():
                lock_task.cancel()
            if not swap_task.done():
                swap_task.cancel()
            await publisher.disconnect()


async def test_late_publication_progress_reaches_guarded_run_row(monkeypatch):
    from tests.entity_address_progress_support import assert_late_progress_persists

    await assert_late_progress_persists(monkeypatch, entity_address_unified)


async def test_parallel_stage_index_progress_reports_measured_counts(monkeypatch):
    from tests.entity_address_progress_support import assert_index_progress_counts

    await assert_index_progress_counts(monkeypatch, entity_address_unified)


async def test_failed_late_publication_work_never_reports_completion(monkeypatch):
    from tests.entity_address_progress_support import assert_failed_phase_stays_incomplete

    await assert_failed_phase_stays_incomplete(monkeypatch, entity_address_unified)


@pytest.mark.parametrize("is_failed", (False, True))
async def test_snapshot_publication_progress_tracks_operation_completion(monkeypatch, is_failed):
    from tests.entity_address_progress_support import assert_snapshot_publication_progress

    await assert_snapshot_publication_progress(monkeypatch, entity_address_unified, is_failed=is_failed)


@pytest.mark.asyncio
async def test_logical_source_replacement_at_same_names_cannot_keep_old_projection():
    """The ordinary NPI rotation and changed reference data must govern public predicates."""
    from process.npi import _rotate_npi_canonical_table

    async with _temporary_schema() as (database, schema):
        await _insert_projection_references(database, schema)
        await _insert_projection_addresses(database, schema)
        await _insert_projection_evidence(database, schema)
        await _materialize_active_projection(database, schema)
        runtime_sql = _runtime_assurance_sql(schema)
        assert await _runtime_assurance(database, schema, runtime_sql) == ("nppes_registry_address", True, True)
        await database.status(f"CREATE TABLE {schema}.npi_address_reimported (LIKE {schema}.npi_address INCLUDING ALL)")
        await database.status(
            f"INSERT INTO {schema}.npi_address_reimported SELECT * FROM {schema}.npi_address WHERE npi<>7001"
        )
        async with database.transaction() as session:
            connection = await session.connection()
            await connection.exec_driver_sql("SELECT 1")
            native = (await connection.get_raw_connection()).driver_connection
            await _rotate_npi_canonical_table(
                native, schema=schema, live_table="npi_address", stage_table="npi_address_reimported", index_suffixes=()
            )
        assert await database.scalar(f"SELECT count(*) FROM {schema}.npi_address WHERE npi=7001") == 0
        assert await _runtime_assurance(database, schema, runtime_sql) == (None, True, True)
        assert (
            await database.scalar(
                f"SELECT geo_evidence_source_id FROM {schema}.entity_address_unified WHERE location_key='nppes'"
            )
            == 1
        )
        for table, column in (("geo_zip_lookup", "zip_code"), ("zip_state", "zip"), ("zcta5", "zcta5ce")):
            await database.status(f"UPDATE {schema}.{table} SET {column}='99999'")
        assert await _runtime_assurance(database, schema, runtime_sql) == (None, False, False)
        await _materialize_active_projection(database, schema)
        assert await _runtime_assurance(database, schema, runtime_sql) == (None, False, False)
        assert tuple(
            await database.first(
                f"SELECT geo_evidence_source_id,geo_identity_coherent,geo_point_coherent "
                f"FROM {schema}.entity_address_unified WHERE location_key='nppes'"
            )
        ) == (0, False, False)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["arbitrary-schema", "wrong-point-table", "split-family"])
async def test_cached_point_rejects_unsupported_held_snapshot_names(damage):
    """A complete named map cannot turn an arbitrary relation into the supported held family."""
    async with _temporary_schema() as (database, schema):
        held = ("arbitrary_geo_" if damage == "arbitrary-schema" else "reference_family_archive_") + uuid4().hex
        point_table = "other_zcta" if damage == "wrong-point-table" else "zcta5"
        await database.status(f"CREATE SCHEMA {held}")
        try:
            for source_table, target_table in (("zcta5", point_table), ("zip_state", "zip_state")):
                await database.status(
                    f"CREATE TABLE {held}.{target_table} (LIKE {schema}.{source_table} INCLUDING ALL)"
                )
            binding_by_name = {
                f"{namespace or schema}.{name}": {"schema_name": schema, "table_name": name}
                for namespace, name in projection._PROJECTION_DEPENDENCIES
            }
            binding_by_name["tiger.zcta5"] = {"schema_name": held, "table_name": point_table}
            binding_by_name["tiger.zip_state"] = {
                "schema_name": schema if damage == "split-family" else held,
                "table_name": "zip_state",
            }
            await database.status(
                f"UPDATE {schema}.entity_address_geo_assurance_state SET active_geo_assurance_version=1,"
                f"active_relation_signature={projection.projection_relation_signature_sql(schema, dependency_bindings=binding_by_name)},"
                "active_dependency_bindings=CAST(:bindings AS jsonb)",
                bindings=json.dumps(binding_by_name),
            )
            assert await database.scalar("SELECT " + projection.projection_state_available_sql(schema)) is True
            await database.status(
                f"INSERT INTO {schema}.entity_address_unified "
                "(location_key,geo_point_coherent,geo_assurance_version) VALUES ('point',true,1)"
            )
            point = projection.projected_boolean_sql(
                "address", "geo_point_coherent", schema_name=schema, legacy_sql="FALSE"
            )
            assert await database.scalar(f"SELECT {point} FROM {schema}.entity_address_unified address") is False
        finally:
            await database.status(f"DROP SCHEMA {held} CASCADE")
