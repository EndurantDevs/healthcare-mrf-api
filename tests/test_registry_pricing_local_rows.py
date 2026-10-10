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
from tests.test_ptg2_local_physical_read_view import _view_fixture
from tests.test_ptg2_local_preparation_authority import _ownership, _published_control_fixture
from tests.test_registry_pricing_snapshot_rows import SnapshotDriver, metadata


def _catalog_fixture():
    binding, _, _, _ = _published_control_fixture()
    ownership = _ownership(binding)
    relations = [
        {
            "schema_name": ownership.schema_name,
            "oid": oid,
            "relname": name,
            "relkind": "r",
            "ordinary": True,
            "index_table_oid": None,
        }
        for name, oid in ownership.relation_oids
    ]
    relations += [
        {
            "schema_name": ownership.schema_name,
            "oid": oid,
            "relname": name,
            "relkind": "S",
            "ordinary": True,
            "index_table_oid": None,
        }
        for name, oid, _, _ in ownership.sequence_oids
    ]
    sequences = [
        {"sequence_name": name, "sequence_oid": oid, "table_name": table, "column_name": column}
        for name, oid, table, column in ownership.sequence_oids
    ]
    return ownership, relations, sequences


@pytest.mark.parametrize("change", [None, "missing", "oid", "schema", "rls", "unknown", "duplicate", "sequence"])
def test_complete_native_inventory_requires_every_exact_object(change):
    ownership, relations, sequences = _catalog_fixture()
    if change == "missing":
        relations.pop(0)
    if change == "oid":
        relations[0]["oid"] += 10000
    if change == "schema":
        relations[0]["schema_name"] = "other"
    if change == "rls":
        relations[0]["ordinary"] = False
    if change == "unknown":
        relations.append({**relations[0], "relname": "unexpected", "oid": 999999})
    if change == "duplicate":
        relations.append(dict(relations[0]))
    if change == "sequence":
        sequences[0]["column_name"] = "other"
    if change is None:
        local._family_inventory(ownership, relations, sequences)
    else:
        with pytest.raises(physical.PTG2PhysicalBindingError):
            local._family_inventory(ownership, relations, sequences)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, ValueError("view"), asyncio.CancelledError()])
async def test_deparser_restores_without_replacing_original_failure(failure):
    driver = SimpleNamespace(
        fetchval=AsyncMock(return_value="original"), fetchrow=AsyncMock(return_value={"oid": 1}), execute=AsyncMock()
    )
    if failure is not None:
        driver.fetchrow.side_effect = failure
        driver.execute.side_effect = [None, RuntimeError("restore")]
        with pytest.raises(type(failure)) as caught:
            await local._view_proof(driver, "trusted", 1, "mrf", 4096)
        assert caught.value is failure and "restoration" in failure.__notes__[-1]
    else:
        assert await local._view_proof(driver, "trusted", 1, "mrf", 4096) == {"oid": 1}
    assert driver.execute.await_count == 2
    assert driver.execute.await_args.args == ("SELECT pg_catalog.set_config('search_path',$1,true)", "original")


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
async def test_native_role_and_fixed_view_refuse_before_header_reads():
    driver = SimpleNamespace(is_in_transaction=lambda: True, fetch=AsyncMock(return_value=[]))
    with pytest.raises(physical.PTG2PhysicalBindingError):
        await local._qualify_installed_view(driver, 4096)
    assert driver.fetch.await_count == 1
    query, names, _, columns, cap = driver.fetch.await_args.args
    assert "requested.table_name" in query and names == [
        physical._PREPARATION + suffix for suffix in ("", "_relation", "_sequence")
    ]
    assert columns == list(physical._INITIAL_PREPARATION_COLUMNS) and cap == 4096


def _installed_fixture():
    binding, authority_by_field = _view_fixture()
    _, control, evidence, publication = _published_control_fixture()
    control.update(
        attested_source_key="source_a",
        attested_coverage_scope_id="a" * 64,
        attested_source_set_digest="b" * 64,
        attested_audit_sample_digest="c" * 64,
    )
    for field in ("initialization", "data", "native_audit", "activation_evidence"):
        authority_by_field["native_validation"][field].update(evidence[field])
    authority_by_field["native_validation"]["control_sha256"] = evidence["control_sha256"]
    authority_by_field["native_validation"]["activation_evidence"]["control_sha256"] = evidence["control_sha256"]
    authority_by_field = {**dict.fromkeys(physical._local_read_view_columns(False)), **authority_by_field}
    authority_by_field.update(contract="ptg.installed-physical-binding-read.v1", native_publication=publication)
    ownership, relations, sequences = _catalog_fixture()
    columns = [
        dict(table_name=name, column_name="key", data_type="bigint", attnotnull=True, attidentity="", attgenerated="")
        for name, _ in ownership.relation_oids
    ]
    objects = [
        dict(kind="relation", oid=oid, name=name, owner=binding.owner_oid, definition="r:p:0:0")
        for name, oid in ownership.relation_oids
    ]
    digest = physical._local_catalog_digest(ownership, objects, columns)
    authority_by_field["native_validation"]["catalog_sha256"] = digest
    authority_by_field["native_validation"]["native_audit"]["catalog_sha256"] = digest
    return binding, authority_by_field, control, ownership, relations, sequences, objects, columns


@pytest.mark.asyncio
@pytest.mark.parametrize("requested_count", [1, 8, 64])
async def test_complete_payload_phases_use_fixed_set_queries(monkeypatch, requested_count):
    """Pure payload proof only: authentic view/role qualification is explicitly stubbed here."""
    binding, authority_by_field, control, ownership, relations, sequences, objects, columns = _installed_fixture()
    calls = []
    task = asyncio.current_task()

    async def fetch(query, *parameters):
        assert asyncio.current_task() is task
        assert "WITH selected AS MATERIALIZED" in query and "byte_count<=" in query
        calls.append((query, parameters))
        if "SELECT * FROM" in query:
            return [deepcopy(authority_by_field)]
        if "AS schema_name" in query:
            return [dict(selected_by_field, schema_oid=binding.schema_oid) for selected_by_field in relations]
        if "d.deptype IN ('a','i')" in query:
            return [dict(selected_by_field, schema_oid=binding.schema_oid) for selected_by_field in sequences]
        if "custody.*" in query:
            return [dict(schema_oid=binding.schema_oid, object_count=len(relations), closed=True)]
        if "AS schema_oid,'relation'" in query:
            return [dict(selected_by_field, schema_oid=binding.schema_oid) for selected_by_field in objects]
        if "AS schema_oid,c.relname" in query:
            return [dict(selected_by_field, schema_oid=binding.schema_oid) for selected_by_field in columns]
        if "AS request_no,snapshot.*" in query:
            return [dict(deepcopy(control), request_no=1)]
        if "SELECT snapshot_id,plan_id" in query:
            return [dict(snapshot_id=binding.snapshot_id, plan_id="12-3456789", plan_market_type="group")]
        if "LIMIT 257" in query:
            return [dict(request_no=1, source_key=0)]
        raise AssertionError(query)

    driver = SimpleNamespace(is_in_transaction=lambda: True, fetch=fetch, execute=AsyncMock())
    monkeypatch.setattr(local, "_qualify_installed_view", AsyncMock(return_value=binding.owner_oid))
    selected = [{"snapshot_id": binding.snapshot_id}] + [
        {"snapshot_id": f"missing-{index}"} for index in range(1, requested_count)
    ]
    resolved = await local._installed_local_rows(driver, selected, 16 * 1024 * 1024)
    assert set(resolved) == {binding.snapshot_id}
    assert resolved[binding.snapshot_id][1] == binding
    assert resolved[binding.snapshot_id][0]["bound_snapshot_key"] == binding.destination_layout_key
    assert len(calls) == 9
    assert calls[0][1][0] == [selected_by_field["snapshot_id"] for selected_by_field in selected]
    assert driver.execute.await_count == 1
    assert str(driver.execute.await_args.args[0]).startswith("LOCK TABLE ONLY ")
