"""Pure fixed-phase LOCAL custody contracts; installed native authority is not mocked into acceptance."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import registry_pricing_local_rows as local
from process import registry_pricing_snapshot_rows as rows
from process.ptg_parts import ptg2_physical_binding as physical
from tests.ptg2_manifest_tables_support import strict_snapshot_row, strict_v4_serving_index
from tests.test_ptg2_local_physical_read_view import _installed_fixture, _legacy_view_fixture, _view_fixture
from tests.test_registry_pricing_snapshot_rows import SnapshotDriver, metadata


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        physical.PTG2PhysicalBindingError("identity"),
        asyncpg.UndefinedTableError("missing"),
        asyncio.CancelledError(),
        asyncpg.InsufficientPrivilegeError("role"),
        ValueError("caller"),
    ],
)
async def test_only_acknowledged_optional_custody_failure_recovers(monkeypatch, failure):
    lifecycle_events = []
    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value={"isolation": "repeatable read", "read_only": "on", "search_path": "public"}),
        execute=AsyncMock(),
    )
    savepoint = SimpleNamespace(
        start=AsyncMock(side_effect=lambda: lifecycle_events.append("start")),
        rollback=AsyncMock(side_effect=lambda: lifecycle_events.append("rollback")),
        commit=AsyncMock(),
    )
    driver.transaction = lambda: savepoint
    monkeypatch.setattr(local, "_installed_local_rows", AsyncMock(side_effect=failure))
    if isinstance(failure, (*local._STORAGE_ERRORS, physical.PTG2PhysicalBindingError)):
        assert await local.read_pricing_local_rows(driver, [{"snapshot_id": "one"}], max_report_bytes=4096) == {}
    else:
        with pytest.raises(type(failure)) as caught:
            await local.read_pricing_local_rows(driver, [{"snapshot_id": "one"}], max_report_bytes=4096)
        assert caught.value is failure
    assert lifecycle_events == ["start", "rollback"]
    savepoint.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_optional_rollback_keeps_primary_error(monkeypatch):
    original = physical.PTG2PhysicalBindingError("native identity")
    savepoint = SimpleNamespace(
        start=AsyncMock(), rollback=AsyncMock(side_effect=asyncio.CancelledError()), commit=AsyncMock()
    )
    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value={"isolation": "repeatable read", "read_only": "on", "search_path": "public"}),
        execute=AsyncMock(),
        transaction=lambda: savepoint,
    )
    monkeypatch.setattr(local, "_installed_local_rows", AsyncMock(side_effect=original))
    with pytest.raises(physical.PTG2PhysicalBindingError) as caught:
        await local.read_pricing_local_rows(driver, [{"snapshot_id": "one"}], max_report_bytes=4096)
    assert caught.value is original and "rollback" in original.__notes__[-1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [None, physical.PTG2PhysicalBindingError("catalog"), asyncpg.InsufficientPrivilegeError("role")]
)
async def test_entire_local_authority_phase_uses_canonical_path_and_restores_parent(monkeypatch, failure):
    parent_path = "pg_temp, untrusted, pg_catalog"
    settings_by_name = {"path": parent_path}
    events = []

    async def settings(query):
        assert query.count("pg_catalog.current_setting(") == 3
        return {"isolation": "repeatable read", "read_only": "on", "search_path": settings_by_name["path"]}

    async def execute(query, *parameters):
        assert query.startswith("SELECT pg_catalog.set_config('search_path',")
        settings_by_name["path"] = parameters[0] if parameters else "pg_catalog, pg_temp"

    async def rollback():
        events.append("rollback")
        settings_by_name["path"] = parent_path

    async def validate(connection, selected, maximum):
        assert settings_by_name["path"] == "pg_catalog, pg_temp"
        events.append("validate")
        if failure is not None:
            raise failure
        return {"one": object()}

    savepoint = SimpleNamespace(
        start=AsyncMock(side_effect=lambda: events.append("start")),
        rollback=rollback,
        commit=AsyncMock(side_effect=lambda: events.append("commit")),
    )
    driver = SimpleNamespace(
        is_in_transaction=lambda: True, fetchrow=settings, execute=execute, transaction=lambda: savepoint
    )
    monkeypatch.setattr(local, "_installed_local_rows", validate)
    if isinstance(failure, asyncpg.InsufficientPrivilegeError):
        with pytest.raises(asyncpg.InsufficientPrivilegeError) as caught:
            await local.read_pricing_local_rows(driver, [{"snapshot_id": "one"}], max_report_bytes=4096)
        assert caught.value is failure
    else:
        local_rows_by_snapshot = await local.read_pricing_local_rows(
            driver, [{"snapshot_id": "one"}], max_report_bytes=4096
        )
        assert set(local_rows_by_snapshot) == ({"one"} if failure is None else set())
    assert settings_by_name["path"] == parent_path
    assert events == ["start", "validate", "commit" if failure is None else "rollback"]


@pytest.mark.asyncio
async def test_consumed_local_row_preserves_destination_and_payload_keys(monkeypatch):
    binding, _ = _view_fixture()
    snapshot = strict_snapshot_row(
        strict_v4_serving_index(binding.payload_snapshot_key), has_local_physical_binding=True
    )
    snapshot.update(snapshot_id=binding.snapshot_id, bound_snapshot_key=binding.destination_layout_key)
    driver = SnapshotDriver([snapshot])
    resolver = AsyncMock(return_value={binding.snapshot_id: (snapshot, binding)})
    monkeypatch.setattr(local, "read_pricing_local_rows", resolver)
    checks = await rows.read_pricing_snapshot_row_checks(
        driver, metadata([binding.snapshot_id]), max_report_bytes=16 * 1024 * 1024
    )
    check = checks[binding.snapshot_id]
    assert check["status"] == "local_published_row_validated" and check["local_custody_status"] == "validated"
    assert check["shared_snapshot_key"] == binding.payload_snapshot_key
    assert check["local_descriptor_status"] == check["binding_selector_status"] == "not_assessed"
    assert check["full_readiness"] == "not_assessed" and "shared_descriptor_status" not in check
    assert resolver.await_args.args[0] is driver


@pytest.mark.asyncio
@pytest.mark.parametrize("requested_count", [1, 8, 64])
@pytest.mark.parametrize("legacy", [False, True])
async def test_complete_named_payload_uses_bounded_set_queries(requested_count, legacy):
    binding, authority, control = _installed_fixture()
    if legacy:
        binding, authority = _legacy_view_fixture(False)
    control.update(
        attested_source_key="source_a",
        attested_coverage_scope_id="a" * 64,
        attested_source_set_digest="b" * 64,
        attested_audit_sample_digest="c" * 64,
    )
    calls = []
    original_task = asyncio.current_task()

    async def fetch(query, *parameters):
        assert asyncio.current_task() is original_task
        assert "WITH selected AS MATERIALIZED" in query and "byte_count<=" in query
        assert "pg_class" not in query and "pg_roles" not in query
        calls.append((query, parameters))
        if "SELECT * FROM" in query:
            return [deepcopy(authority)]
        if "AS request_no,snapshot.*" in query:
            return [dict(deepcopy(control), request_no=1)]
        if "SELECT snapshot_id,plan_id" in query:
            return [dict(snapshot_id=binding.snapshot_id, plan_id="12-3456789", plan_market_type="group")]
        if "LIMIT 257" in query:
            return [dict(request_no=1, source_key=0)]
        raise AssertionError(query)

    driver = SimpleNamespace(is_in_transaction=lambda: True, fetch=fetch, execute=AsyncMock())
    selected = [{"snapshot_id": binding.snapshot_id}] + [
        {"snapshot_id": f"missing-{index}"} for index in range(1, requested_count)
    ]
    resolved = await local._installed_local_rows(driver, selected, 16 * 1024 * 1024)
    assert set(resolved) == {binding.snapshot_id}
    assert resolved[binding.snapshot_id][1] == binding
    assert resolved[binding.snapshot_id][0]["bound_snapshot_key"] == binding.destination_layout_key
    assert len(calls) == 4
    assert calls[0][1][0] == [selected_by_field["snapshot_id"] for selected_by_field in selected]
    assert driver.execute.await_count == 1
    sql = driver.execute.await_args.args[0]
    assert sql.startswith("LOCK TABLE ONLY ") and "ACCESS SHARE MODE NOWAIT" in sql
    assert all(binding.relation(name) in sql for _schema, name in binding.relations)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["unknown", "duplicate"])
async def test_installed_projection_refuses_wrong_or_duplicate_snapshot(fault):
    binding, authority, _control = _installed_fixture()
    selected = [authority, authority] if fault == "duplicate" else [{**authority, "destination_snapshot_id": "other"}]
    driver = SimpleNamespace(is_in_transaction=lambda: True, fetch=AsyncMock(return_value=selected))
    with pytest.raises(physical.PTG2PhysicalBindingError, match="installed identity"):
        await local._installed_bindings(driver, (binding.snapshot_id,), 16 * 1024 * 1024)
