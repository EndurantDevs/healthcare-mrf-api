# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from collections.abc import Callable
from unittest.mock import AsyncMock

import pytest

importer = importlib.import_module("process.provider_directory_fhir")


async def _keep_stage_indexes(_schema: str, _stage_table: str) -> None:
    """Leave index names unchanged for simple disposable test tables."""
    return None


async def _fail_stage_indexes(_schema: str, _stage_table: str) -> None:
    """Inject a late failure after every bundle relation has been renamed."""
    raise RuntimeError("second target failed")


async def _create_artifact_bundle_relations(database, schema: str) -> None:
    """Create two old targets and two replacement stages."""
    values_by_relation = {
        "artifact_target_a": "old-a",
        "artifact_target_b": "old-b",
        "artifact_stage_a": "new-a",
        "artifact_stage_b": "new-b",
    }
    for relation_name, value in values_by_relation.items():
        await database.status(
            f"CREATE TABLE {schema}.{relation_name} (value text NOT NULL);"
        )
        await database.status(
            f"INSERT INTO {schema}.{relation_name} VALUES (:value);",
            value=value,
        )


async def _prepared_bundle_stage(
    schema: str,
    stage_table: str,
    target_relation: str,
    rename_stage_indexes,
):
    """Build a prepared-stage descriptor fenced to the current target OID."""
    target_oid = await importer._provider_directory_relation_oid(
        schema,
        target_relation,
    )
    return importer.ProviderDirectoryPreparedArtifactStage(
        schema,
        stage_table,
        target_relation,
        rename_stage_indexes,
        importer.ProviderDirectoryArtifactBuildFence(target_oid=target_oid),
    )


async def _artifact_relation_value(database, schema: str, relation: str) -> str:
    """Read the marker value stored in a disposable artifact relation."""
    return str(
        await database.scalar(f"SELECT value FROM {schema}.{relation};")
    )


async def _assert_artifact_relation_values(
    database,
    schema: str,
    expected_values_by_relation: dict[str, str],
) -> None:
    """Assert marker values for a set of disposable artifact relations."""
    for relation, expected_value in expected_values_by_relation.items():
        assert await _artifact_relation_value(
            database,
            schema,
            relation,
        ) == expected_value


class _FakeTransaction:
    def __init__(self, events: list[str]):
        self.events = events

    async def __aenter__(self):
        self.events.append("transaction-enter")
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.events.append("transaction-rollback" if exc_type else "transaction-commit")
        self.events.append("transaction-exit")
        return False


class _LockUnavailableError(RuntimeError):
    pgcode = "55P03"


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _AdvisoryConnection:
    def __init__(self, engine):
        self.engine = engine

    async def execute(self, statement, params):
        sql_text = str(statement)
        self.engine.events.append(sql_text)
        if "pg_try_advisory_lock" in sql_text:
            if self.engine.locked:
                return _ScalarResult(False)
            self.engine.locked = True
            return _ScalarResult(True)
        if "pg_advisory_unlock" in sql_text:
            was_locked = self.engine.locked
            self.engine.locked = False
            return _ScalarResult(was_locked)
        raise AssertionError(sql_text)

    async def commit(self):
        self.engine.events.append("connection-commit")

    async def invalidate(self):
        self.engine.events.append("connection-invalidate")

    async def close(self):
        self.engine.events.append("connection-close")


class _AdvisoryEngine:
    def __init__(self):
        self.events: list[str] = []
        self.locked = False

    async def connect(self):
        return _AdvisoryConnection(self)


def _install_cutover_db(
    monkeypatch,
    *,
    persistence: str = "p",
    advisory_results: list[bool] | None = None,
    status_hook: Callable[[str], None] | None = None,
    target_kind: str = "r",
) -> list[str]:
    events: list[str] = []
    advisory_values = iter(advisory_results or [True])

    async def status(sql, **_params):
        sql_text = str(sql)
        events.append(sql_text)
        if status_hook:
            status_hook(sql_text)

    async def scalar(sql, **_params):
        sql_text = str(sql)
        events.append(sql_text)
        if "pg_try_advisory_xact_lock" in sql_text:
            return next(advisory_values)
        if "cls.relpersistence" in sql_text:
            return persistence
        if "cls.relkind" in sql_text:
            relation = _params["relation_name"]
            if relation.endswith("_old"):
                return None
            if relation.endswith("_stage"):
                return "r"
            return target_kind
        return None

    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer.db, "scalar", scalar)
    monkeypatch.setattr(importer.db, "transaction", lambda: _FakeTransaction(events))
    return events


@pytest.mark.asyncio
async def test_artifact_cutover_logs_stage_before_transaction_and_renames_indexes_inside_swap(monkeypatch):
    events = _install_cutover_db(monkeypatch)

    async def rename_indexes(schema, stage_table):
        await importer.db.status(
            f'ALTER INDEX "{schema}"."{stage_table}_lookup_idx" RENAME TO "artifact_lookup_idx";'
        )

    await importer._cutover_provider_directory_artifact_stage(
        schema="mrf",
        stage_table="overlay_stage",
        target_relation="address_overlay",
        rename_stage_indexes=rename_indexes,
    )

    logged_index = next(index for index, event in enumerate(events) if "SET LOGGED" in event)
    transaction_index = events.index("transaction-enter")
    commit_index = events.index("transaction-commit")
    assert logged_index < transaction_index
    assert any("ACCESS EXCLUSIVE MODE NOWAIT" in event for event in events)
    assert any("statement_timeout" in event for event in events)
    assert next(index for index, event in enumerate(events) if "ALTER INDEX" in event) < commit_index
    assert sum("cls.relpersistence" in event for event in events) == 3


@pytest.mark.asyncio
async def test_artifact_cutover_rejects_unlogged_stage_and_cleans_it_up(monkeypatch):
    events = _install_cutover_db(monkeypatch, persistence="u")

    with pytest.raises(RuntimeError, match="not LOGGED"):
        await importer._cutover_provider_directory_artifact_stage(
            schema="mrf",
            stage_table="overlay_stage",
            target_relation="address_overlay",
            rename_stage_indexes=AsyncMock(),
        )

    assert not any(event == "transaction-enter" for event in events)
    assert any('DROP TABLE IF EXISTS "mrf"."overlay_stage"' in event for event in events)


@pytest.mark.asyncio
async def test_artifact_cutover_retries_nowait_lock_conflicts(monkeypatch):
    attempt_count_by_lock = {"target_lock": 0}

    def fail_first_target_lock(sql: str) -> None:
        if 'LOCK TABLE "mrf"."address_overlay"' in sql:
            attempt_count_by_lock["target_lock"] += 1
            if attempt_count_by_lock["target_lock"] == 1:
                raise _LockUnavailableError()

    events = _install_cutover_db(
        monkeypatch,
        advisory_results=[True, True],
        status_hook=fail_first_target_lock,
    )
    sleep = AsyncMock()
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)

    await importer._cutover_provider_directory_artifact_stage(
        schema="mrf",
        stage_table="overlay_stage",
        target_relation="address_overlay",
        rename_stage_indexes=AsyncMock(),
    )

    assert attempt_count_by_lock["target_lock"] == 2
    assert events.count("transaction-rollback") == 1
    assert events.count("transaction-commit") == 1
    assert sleep.await_count == 1
    assert sum("pg_try_advisory_xact_lock" in event for event in events) == 2


@pytest.mark.asyncio
async def test_artifact_cutover_retries_advisory_conflicts_then_cleans_stage(monkeypatch):
    events = _install_cutover_db(monkeypatch, advisory_results=[False, False, False])
    sleep = AsyncMock()
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)

    with pytest.raises(importer.ProviderDirectoryArtifactCutoverConflict):
        await importer._cutover_provider_directory_artifact_stage(
            schema="mrf",
            stage_table="overlay_stage",
            target_relation="address_overlay",
            rename_stage_indexes=AsyncMock(),
        )

    assert sleep.await_count == 2
    assert events.count("transaction-rollback") == 3
    assert events.count("transaction-commit") == 0
    assert sum("pg_try_advisory_xact_lock" in event for event in events) == 3
    assert not any("ACCESS EXCLUSIVE MODE NOWAIT" in event for event in events)
    assert any('DROP TABLE IF EXISTS "mrf"."overlay_stage"' in event for event in events)


@pytest.mark.asyncio
async def test_artifact_cutover_does_not_retry_non_lock_failure(monkeypatch):
    attempt_count_by_lock = {"target_lock": 0}

    def fail_target_lock(sql: str) -> None:
        if 'LOCK TABLE "mrf"."address_overlay"' in sql:
            attempt_count_by_lock["target_lock"] += 1
            raise RuntimeError("catalog lookup failed")

    events = _install_cutover_db(monkeypatch, status_hook=fail_target_lock)
    sleep = AsyncMock()
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)

    with pytest.raises(RuntimeError, match="catalog lookup failed"):
        await importer._cutover_provider_directory_artifact_stage(
            schema="mrf",
            stage_table="overlay_stage",
            target_relation="address_overlay",
            rename_stage_indexes=AsyncMock(),
        )

    assert attempt_count_by_lock["target_lock"] == 1
    assert events.count("transaction-rollback") == 1
    assert events.count("transaction-commit") == 0
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_artifact_cutover_replaces_materialized_corroboration_without_table_lock(monkeypatch):
    events = _install_cutover_db(monkeypatch, target_kind="m")

    await importer._cutover_provider_directory_artifact_stage(
        schema="mrf",
        stage_table="corroboration_stage",
        target_relation=importer.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW,
        rename_stage_indexes=AsyncMock(),
    )

    target_lock = 'LOCK TABLE "mrf"."provider_directory_address_corroboration"'
    assert not any(target_lock in event for event in events)
    assert any("DROP MATERIALIZED VIEW" in event for event in events)


@pytest.mark.asyncio
async def test_all_artifact_publishers_route_stage_swaps_through_shared_helper(monkeypatch):
    cutover = AsyncMock()
    monkeypatch.setattr(importer, "_cutover_provider_directory_artifact_stage", cutover)

    await importer._swap_address_overlay_stage(
        "mrf",
        "overlay_stage",
        '"mrf"."overlay_stage"',
        '"mrf"."address_overlay"',
    )
    await importer._swap_network_catalog_stage(
        "mrf",
        "catalog_stage",
        '"mrf"."catalog_stage"',
        '"mrf"."network_catalog"',
    )
    await importer._swap_address_corroboration_stage(
        "mrf",
        "corroboration_stage",
        '"mrf"."corroboration_stage"',
        importer.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW,
    )

    assert [call.kwargs["target_relation"] for call in cutover.await_args_list] == [
        importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE,
        importer.PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE,
        importer.PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW,
    ]


@pytest.mark.asyncio
async def test_artifact_build_guard_serializes_complete_build_cycle(monkeypatch):
    engine = _AdvisoryEngine()
    monkeypatch.setattr(importer.db, "engine", engine)
    monkeypatch.setattr(importer.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(importer, "_provider_directory_relation_oid", AsyncMock(return_value=42))
    first_entered = importer.asyncio.Event()
    release_first = importer.asyncio.Event()

    async def hold_first_guard():
        async with importer._provider_directory_artifact_build_guard("mrf", "address_overlay") as build_fence:
            assert build_fence.target_oid == 42
            first_entered.set()
            await release_first.wait()

    first_task = importer.asyncio.create_task(hold_first_guard())
    await first_entered.wait()
    with pytest.raises(importer.ProviderDirectoryArtifactCutoverConflict):
        async with importer._provider_directory_artifact_build_guard("mrf", "address_overlay"):
            raise AssertionError("the second publisher entered the guarded build")
    release_first.set()
    await first_task

    assert sum("pg_try_advisory_lock" in event for event in engine.events) == 4
    assert sum("pg_advisory_unlock" in event for event in engine.events) == 1
    assert engine.locked is False


@pytest.mark.asyncio
async def test_artifact_cutover_rejects_stage_built_from_superseded_target(monkeypatch):
    events = _install_cutover_db(monkeypatch)
    monkeypatch.setattr(importer, "_provider_directory_relation_oid", AsyncMock(return_value=22))

    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        await importer._promote_provider_directory_artifact_stage(
            "mrf",
            "overlay_stage",
            "address_overlay",
            AsyncMock(),
            importer.ProviderDirectoryArtifactBuildFence(target_oid=11),
        )

    assert events.count("transaction-rollback") == 1
    assert events.count("transaction-commit") == 0
    assert not any('RENAME TO "address_overlay_old"' in event for event in events)


@pytest.mark.asyncio
async def test_artifact_cutover_transaction_deadline_rolls_back(monkeypatch):
    events = _install_cutover_db(monkeypatch)
    base_status = importer.db.status
    never = importer.asyncio.Event()

    async def hanging_status(sql, **params):
        if 'LOCK TABLE "mrf"."address_overlay"' in str(sql):
            await never.wait()
        return await base_status(sql, **params)

    monkeypatch.setattr(importer.db, "status", hanging_status)
    monkeypatch.setattr(importer, "PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_TRANSACTION_TIMEOUT_SECONDS", 0.01)

    with pytest.raises(TimeoutError):
        await importer._promote_provider_directory_artifact_stage(
            "mrf",
            "overlay_stage",
            "address_overlay",
            AsyncMock(),
        )

    assert events.count("transaction-rollback") == 1
    assert events.count("transaction-commit") == 0


def test_artifact_stage_names_are_unique_for_duplicate_runs():
    network_names = {importer._network_catalog_stage_table_name("run_1") for _index in range(10)}
    overlay_names = {
        importer._address_overlay_stage_table_name("run_1")
        for _index in range(1_000)
    }

    assert len(network_names) == 10
    assert len(overlay_names) == 1_000
    assert all(
        importer._is_address_overlay_stage_table_name(name)
        for name in overlay_names
    )
    legacy_name = (
        f"{importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_STAGE_PREFIX}_"
        + "a"
        * importer.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_STAGE_LEGACY_IDENTIFIER_HEX_LENGTH
    )
    assert importer._is_address_overlay_stage_table_name(legacy_name)
