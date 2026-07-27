# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os
import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.connection import Database
from process.ptg_parts import source_snapshot_control
from tests.ptg_source_snapshot_removal_postgres_support import (
    count_rows as _count,
    create_production_shaped_schema as _create_production_shaped_schema,
    insert_release_binding as _insert_release_binding,
    insert_shared_snapshots as _insert_shared_snapshots,
)


class _RecordingTransaction:
    def __init__(self):
        self.active = False
        self.statements = []

    async def __aenter__(self):
        self.active = True
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.active = False
        return False

    async def execute(self, statement, params):
        assert self.active
        self.statements.append((str(statement), params))


def _remove_plan_fake(transaction):
    async def fake_plan(**_kwargs):
        assert transaction.active
        return {
            "snapshot_id": "shared-a",
            "source_key": "source_a",
            "exists": True,
            "removable": True,
            "tables": [],
            "artifact_manifest_ids": [],
            "current_references": {},
            "storage_generation": "shared_blocks_v3",
            "shared_snapshot_key": 11,
        }

    return fake_plan


def _remove_status_fake(transaction, events):
    async def fake_status(statement, **params):
        assert transaction.active
        assert params == {"snapshot_id": "shared-a"}
        if "ptg2_v3_snapshot_scope" in statement:
            events.append("scope-delete")
            return 1
        if "ptg2_v3_snapshot_binding" in statement:
            events.append("binding-delete")
            return 1
        if "ptg2_artifact_manifest" in statement:
            events.append("artifact-delete")
            return 0
        if 'DELETE FROM "mrf".ptg2_snapshot WHERE' in statement:
            events.append("snapshot-delete")
            return 1
        raise AssertionError(statement)

    return fake_status


def _layout_release_fake(transaction, events):
    async def fake_release(
        *,
        schema_name,
        executor,
        require_shared,
        layout_keys,
    ):
        assert transaction.active
        assert schema_name == "mrf"
        assert require_shared is True
        assert executor._session is transaction
        assert layout_keys == (11,)
        events.append("layout-release")
        return SimpleNamespace(
            logical_layout_count=1,
            candidate_hash_count=2,
            stored_bytes=4096,
        )

    return fake_release


def _install_remove_transaction_fakes(monkeypatch, transaction, events):
    """Install transaction-bound fakes for the removal ordering assertion."""

    monkeypatch.setattr(source_snapshot_control.db, "transaction", lambda: transaction)
    monkeypatch.setattr(
        source_snapshot_control.db,
        "status",
        _remove_status_fake(transaction, events),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "build_source_snapshot_remove_plan",
        _remove_plan_fake(transaction),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "bound_shared_layout_keys",
        AsyncMock(return_value=(11,)),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "release_unbound_ptg2_shared_layouts",
        _layout_release_fake(transaction, events),
    )


@pytest.mark.asyncio
async def test_remove_v3_snapshot_releases_layout_in_the_removal_transaction(monkeypatch):
    """Verify remove v3 snapshot releases layout in the removal transaction."""
    transaction = _RecordingTransaction()
    events = []
    _install_remove_transaction_fakes(monkeypatch, transaction, events)

    removal_result = await source_snapshot_control.remove_ptg2_source_snapshot(
        snapshot_id="shared-a",
        source_key="source_a",
    )

    assert events == [
        "scope-delete",
        "binding-delete",
        "artifact-delete",
        "snapshot-delete",
        "layout-release",
    ]
    assert removal_result["deleted_v3_snapshot_scopes"] == 1
    assert removal_result["deleted_v3_snapshot_bindings"] == 1
    assert removal_result["deleted_snapshots"] == 1
    assert removal_result["released_shared_layouts"] == 1
    assert removal_result["queued_shared_block_candidates"] == 2
    assert removal_result["queued_shared_block_bytes"] == 4096
    assert removal_result["layout_cleanup"] == "released"
    assert removal_result["physical_cleanup"] == "pending_sweep"


@pytest.mark.asyncio
async def test_real_postgres_remove_v3_snapshot_matches_production_fk_ddl(monkeypatch):
    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 for the isolated PostgreSQL test")

    database = Database()
    schema_name = f"ptg2_snapshot_removal_{uuid.uuid4().hex}"
    schema = f'"{schema_name}"'
    await database.connect()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setattr(source_snapshot_control, "db", database)
    try:
        await _create_production_shaped_schema(database, schema_name)
        await _insert_shared_snapshots(database, schema_name)

        first = await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id="shared-a",
            source_key="source_a",
        )

        assert first["deleted_v3_snapshot_scopes"] == 1
        assert first["deleted_v3_snapshot_bindings"] == 1
        assert first["released_shared_layouts"] == 0
        assert first["layout_cleanup"] == "retained_shared"
        assert first["physical_cleanup"] == "deferred"
        assert await _count(database, schema_name, "ptg2_snapshot", snapshot_id="shared-a") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_scope", snapshot_id="shared-a") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_plan_scope", snapshot_id="shared-a") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_binding", snapshot_id="shared-a") == 0
        assert await _count(database, schema_name, "ptg2_v3_candidate_audit_attestation", snapshot_id="shared-a") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_layout") == 1
        assert await _count(database, schema_name, "ptg2_v3_snapshot_binding", snapshot_id="shared-b") == 1
        assert await _count(database, schema_name, "ptg2_v3_snapshot_plan_scope", snapshot_id="shared-b") == 1
        assert await _count(database, schema_name, "ptg2_v3_candidate_audit_attestation", snapshot_id="shared-b") == 1

        second = await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id="shared-b",
            source_key="source_b",
        )

        assert second["deleted_v3_snapshot_scopes"] == 1
        assert second["deleted_v3_snapshot_bindings"] == 1
        assert second["released_shared_layouts"] == 1
        assert second["layout_cleanup"] == "released"
        assert second["physical_cleanup"] == "not_applicable"
        assert await _count(database, schema_name, "ptg2_snapshot") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_scope") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_plan_scope") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_binding") == 0
        assert await _count(database, schema_name, "ptg2_v3_candidate_audit_attestation") == 0
        assert await _count(database, schema_name, "ptg2_v3_snapshot_layout") == 0
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


async def _assert_release_binding_blocks_removal() -> None:
    plan = await source_snapshot_control.build_source_snapshot_remove_plan(
        snapshot_id="shared-a",
        source_key="source_a",
    )
    assert plan["removable"] is False
    assert plan["current_references"]["plan_release_pins"] == []
    assert plan["current_references"]["plan_release_bindings"] == [
        "hpserve-fixture:in_network:0"
    ]
    assert "plan release binding pointer" in plan["reason"]
    with pytest.raises(ValueError, match="plan release binding pointer"):
        await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id="shared-a",
            source_key="source_a",
        )


async def _assert_blocked_snapshot_is_untouched(
    database: Database,
    schema_name: str,
) -> None:
    assert (
        await _count(
            database,
            schema_name,
            "ptg2_snapshot",
            snapshot_id="shared-a",
        )
        == 1
    )
    assert (
        await _count(
            database,
            schema_name,
            "plan_release_snapshot_binding",
            snapshot_id="shared-a",
        )
        == 1
    )
    assert await _count(database, schema_name, "ptg2_v3_snapshot_layout") == 1


async def _delete_release_binding(
    database: Database,
    schema: str,
) -> None:
    async with database.acquire() as connection:
        await connection.status(
            f"""
            DELETE FROM {schema}.plan_release_snapshot_binding
             WHERE snapshot_id = 'shared-a'
            """
        )


@pytest.mark.asyncio
async def test_real_postgres_unpinned_release_binding_blocks_snapshot_removal(
    monkeypatch,
) -> None:
    """A partial release projection remains a hard retention reference."""

    if os.getenv("HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_GC_POSTGRES_TEST=1 for the isolated "
            "PostgreSQL test"
        )

    database = Database()
    schema_name = f"ptg2_snapshot_removal_{uuid.uuid4().hex}"
    schema = f'"{schema_name}"'
    await database.connect()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setattr(source_snapshot_control, "db", database)
    try:
        await _create_production_shaped_schema(database, schema_name)
        await _insert_shared_snapshots(database, schema_name)
        await _insert_release_binding(
            database,
            schema_name,
            snapshot_id="shared-a",
            source_key="source_a",
        )
        await _assert_release_binding_blocks_removal()
        await _assert_blocked_snapshot_is_untouched(database, schema_name)
        await _delete_release_binding(database, schema)
        removed = await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id="shared-a",
            source_key="source_a",
        )
        assert removed["deleted_snapshots"] == 1
        assert removed["layout_cleanup"] == "retained_shared"
    finally:
        try:
            async with database.acquire() as connection:
                await connection.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()
