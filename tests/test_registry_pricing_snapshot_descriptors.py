# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual shared construction/native set SQL, with fake driver and no PG."""

import asyncio
import json
from copy import deepcopy

import asyncpg
import pytest

from api import ptg2_tables
from process import registry_pricing_release_binding_lookup as lookup
from process import registry_pricing_snapshot_descriptors as descriptors
from process import registry_pricing_snapshot_rows as rows
from tests.ptg2_manifest_tables_support import (
    FakeSession,
    strict_direct_v4_serving_index,
    strict_serving_index,
    strict_snapshot_row,
    strict_v4_root_row,
    strict_v4_serving_index,
)
from tests.test_ptg2_v4_finalizer_maps import _root_row
from tests.test_registry_pricing_release_binding_lookup import REFERENCE, REPORT
from tests.test_registry_pricing_release_binding_lookup import retained_export as retained_export
from tests.test_registry_pricing_snapshot_rows import SnapshotDriver, metadata


class NativeSetDriver(SnapshotDriver):
    def __init__(self, snapshots, graph_roots=(), finalizer_roots=()):
        super().__init__(snapshots)
        self.graph_roots = list(graph_roots)
        self.finalizer_roots = list(finalizer_roots)
        self.finalizer_tables = 3
        self.root_failure = None
        self.root_limit = None

    async def fetch(self, sql, *parameters):
        if any("ptg2_v3_snapshot_plan_scope" in name for name in parameters[0]):
            return await super().fetch(sql, *parameters)
        if any("ptg2_v4_" in name for name in parameters[0]):
            self.calls.append((sql, parameters))
            self.tasks.append(asyncio.current_task())
            is_finalizer = "finalizer" in parameters[0][0]
            return [
                {"relation_name": name, "available": not is_finalizer or index < self.finalizer_tables}
                for index, name in enumerate(parameters[0])
            ]
        return await super().fetch(sql, *parameters)

    async def fetchrow(self, sql, *parameters):
        if "WITH roots" not in sql:
            return await super().fetchrow(sql, *parameters)
        self.calls.append((sql, parameters))
        self.tasks.append(asyncio.current_task())
        if self.root_failure is not None:
            raise self.root_failure
        root_rows = self.finalizer_roots if "finalizer_map_root" in sql else self.graph_roots
        encoded = json.dumps(root_rows, default=lambda value: value.hex())
        is_bounded = len(encoded.encode()) <= parameters[2] and self.root_limit is not False
        return {
            "isolation": self.isolation,
            "read_only": self.read_only,
            "bounded": is_bounded,
            "rows_json": encoded if is_bounded else None,
        }


def v4_rows(count):
    snapshots = []
    graph_roots = []
    finalizer_roots = []
    for index in range(count):
        key = index + 100
        serving_index = strict_v4_serving_index(key)
        snapshots.append({**strict_snapshot_row(serving_index), "snapshot_id": f"v4-{index}"})
        graph_roots.append({**strict_v4_root_row(serving_index), "snapshot_key": key})
        finalizer_roots.append(_root_row(snapshot_key=key))
    return snapshots, graph_roots, finalizer_roots


@pytest.mark.asyncio
@pytest.mark.parametrize("count", (1, 8, 64))
async def test_consumed_native_sets_have_fixed_phases(count):
    snapshots, graph_roots, finalizer_roots = v4_rows(count)
    driver = NativeSetDriver(snapshots, graph_roots, finalizer_roots)
    checks = await rows.read_pricing_snapshot_row_checks(
        driver, metadata([row["snapshot_id"] for row in snapshots]), max_report_bytes=16 * 1024 * 1024
    )
    assert len(driver.calls) == 8 and driver.lifecycle == ["start", "release", "start", "release", "start", "release"]
    assert all(task is asyncio.current_task() for task in driver.tasks)
    assert all(
        check["shared_descriptor_status"] == "validated" and check["full_readiness"] == "not_assessed"
        for check in checks.values()
    )
    assert driver.calls[3][1][0] == driver.calls[5][1][0]
    assert sorted(driver.calls[3][1][0]) == list(range(100, 100 + count))
    for call_index in (3, 5):
        sql = driver.calls[call_index][0]
        assert "$1::bigint[]" in sql and ":snapshot_key" not in sql
        assert "CASE WHEN byte_count<=$3::bigint THEN" in sql
        assert sql.index("byte_count<=$3::bigint") < sql.index("jsonb_agg")
    assert driver.is_in_transaction()


@pytest.mark.asyncio
@pytest.mark.parametrize("factory", (strict_serving_index, strict_v4_serving_index, strict_direct_v4_serving_index))
async def test_descriptors_equal_consumed_singleton(factory):
    serving_index = factory()
    row_by_field = {**strict_snapshot_row(serving_index), "snapshot_id": "same-snapshot"}
    graph_row = (
        {**strict_v4_root_row(serving_index), "snapshot_key": serving_index["shared_snapshot_key"]}
        if "provider_graph_v4" in serving_index.get("serving_binary", {})
        else None
    )
    driver = NativeSetDriver([row_by_field], [graph_row] if graph_row else [], [])
    driver.finalizer_tables = 0
    actual = await descriptors.read_pricing_shared_descriptors(
        driver, [row_by_field], max_report_bytes=16 * 1024 * 1024
    )
    singleton = await ptg2_tables.snapshot_serving_tables(
        FakeSession([deepcopy(row_by_field), deepcopy(graph_row)] if graph_row else [deepcopy(row_by_field)]),
        "same-snapshot",
    )
    assert actual["same-snapshot"] == singleton
    assert len(singleton.__dataclass_fields__) == 31


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ("missing", "identity", "diagnostic", "resource", "partial", "malformed_finalizer", "mixed_finalizer")
)
async def test_bad_graph_or_finalizer_never_yields_descriptor(change):
    snapshots, graph_roots, finalizer_roots = v4_rows(2)
    driver = NativeSetDriver(snapshots, graph_roots, finalizer_roots)
    if change == "missing":
        driver.graph_roots.pop(0)
    if change == "identity":
        driver.graph_roots[0]["map_digest"] = "f" * 64
    if change == "diagnostic":
        driver.graph_roots[0]["worst_member_count"] = 999
    if change == "resource":
        driver.graph_roots[0]["factor_edge_count"] = 999
    if change == "partial":
        driver.finalizer_tables = 1
    if change == "malformed_finalizer":
        driver.finalizer_roots[0]["root_completed_at"] = None
    if change == "mixed_finalizer":
        driver.finalizer_roots[0]["relational_mapping_present"] = True
    actual = await descriptors.read_pricing_shared_descriptors(driver, snapshots, max_report_bytes=16 * 1024 * 1024)
    assert actual["v4-0"] is None
    if change in ("missing", "identity", "diagnostic", "resource"):
        assert actual["v4-1"] is not None
    else:
        assert actual["v4-1"] is None
    assert driver.is_in_transaction()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    (
        asyncpg.UndefinedColumnError("synthetic"),
        asyncpg.InsufficientPrivilegeError("synthetic"),
        asyncio.CancelledError(),
    ),
)
async def test_root_failure_preserves_caller_and_parent(failure):
    snapshots, graph_roots, finalizer_roots = v4_rows(1)
    driver = NativeSetDriver(snapshots, graph_roots, finalizer_roots)
    driver.root_failure = failure
    if isinstance(failure, asyncpg.UndefinedColumnError):
        assert (
            await descriptors.read_pricing_shared_descriptors(driver, snapshots, max_report_bytes=16 * 1024 * 1024)
        )["v4-0"] is None
    else:
        with pytest.raises(type(failure)) as caught:
            await descriptors.read_pricing_shared_descriptors(driver, snapshots, max_report_bytes=16 * 1024 * 1024)
        assert caught.value is failure
    assert driver.lifecycle == ["start", "rollback"] and driver.is_in_transaction()


@pytest.mark.asyncio
async def test_root_limit_and_invalid_identity_remain_distinct():
    snapshots, graph_roots, finalizer_roots = v4_rows(1)
    driver = NativeSetDriver(snapshots, graph_roots, finalizer_roots)
    driver.root_limit = False
    assert (await descriptors.read_pricing_shared_descriptors(driver, snapshots, max_report_bytes=16 * 1024 * 1024))[
        "v4-0"
    ] is None
    driver.root_limit = None
    driver.graph_roots.append(deepcopy(driver.graph_roots[0]))
    with pytest.raises(ValueError, match="root_identity_invalid"):
        await descriptors.read_pricing_shared_descriptors(driver, snapshots, max_report_bytes=16 * 1024 * 1024)
    driver.graph_roots = graph_roots
    first_error = asyncpg.UndefinedColumnError("synthetic")
    driver.root_failure = first_error
    driver.rollback_error = asyncio.CancelledError()
    with pytest.raises(asyncpg.UndefinedColumnError) as caught:
        await descriptors.read_pricing_shared_descriptors(driver, snapshots, max_report_bytes=16 * 1024 * 1024)
    assert caught.value is first_error


@pytest.mark.asyncio
async def test_report_consumes_descriptor_phase(monkeypatch, retained_export):
    snapshots, graph_roots, finalizer_roots = v4_rows(1)
    snapshots[0]["snapshot_id"] = REFERENCE["snapshot_id"]
    driver = NativeSetDriver(snapshots, graph_roots, finalizer_roots)
    monkeypatch.setattr(lookup, "read_pricing_snapshot_row_checks", rows.read_pricing_snapshot_row_checks)
    result = await lookup.append_pricing_binding_metadata(
        driver, deepcopy(REPORT), object(), control_schema="sample_control", max_report_bytes=16 * 1024 * 1024
    )
    provenance = result["provenance"]["pricing_binding_metadata"]
    assert provenance["snapshot_row_checks"][REFERENCE["snapshot_id"]]["shared_descriptor_status"] == "validated"
    assert provenance["full_readiness"] == "not_assessed"
    assert provenance["networks"][0]["pricing_refs"][0]["resolved"] is False
    assert result["targets"] == REPORT["targets"] and result["totals"] == REPORT["totals"]
    assert len(driver.calls) == 10 and driver.lifecycle == ["start", "release"] * 4
    assert all(task is asyncio.current_task() for task in driver.tasks)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ("graph", "finalizer"))
async def test_unknown_root_identity_is_trust_failure(phase):
    snapshots, graph_roots, finalizer_roots = v4_rows(1)
    driver = NativeSetDriver(snapshots, graph_roots, finalizer_roots)
    selected = driver.graph_roots if phase == "graph" else driver.finalizer_roots
    selected[0]["snapshot_key"] = 999
    with pytest.raises(ValueError, match="root_identity_invalid"):
        await descriptors.read_pricing_shared_descriptors(driver, snapshots, max_report_bytes=16 * 1024 * 1024)
    assert driver.lifecycle == ["start", "rollback"] and driver.is_in_transaction()


@pytest.mark.asyncio
async def test_local_and_zero_code_rows_never_fallback():
    row_by_field = {**strict_snapshot_row(), "snapshot_id": "local", "has_local_physical_binding": True}
    driver = NativeSetDriver([row_by_field])
    assert (
        await descriptors.read_pricing_shared_descriptors(driver, [row_by_field], max_report_bytes=16 * 1024 * 1024)
    )["local"] is None
    row_by_field["has_local_physical_binding"] = False
    row_by_field["layout_serving_index"]["code_count"] = 0
    assert (
        await descriptors.read_pricing_shared_descriptors(driver, [row_by_field], max_report_bytes=16 * 1024 * 1024)
    )["local"] is None
    assert not driver.calls
