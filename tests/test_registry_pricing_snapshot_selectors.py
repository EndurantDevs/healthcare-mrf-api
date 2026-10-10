# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consumed native selector sets and original scope authority; no PostgreSQL."""

import asyncio
import json

import asyncpg
import pytest

from api.ptg2_serving_utils import ein_plan_id_variants
from api.ptg2_snapshot import (
    _explicit_snapshot_plan_sql,
    _explicit_snapshot_source_sql,
    _serving_relation_available_sql,
)
from process import registry_pricing_snapshot_rows as rows
from process import registry_pricing_snapshot_selectors as selectors
from tests.ptg2_manifest_tables_support import strict_snapshot_row
from tests.test_registry_pricing_snapshot_rows import SnapshotDriver, metadata


class SelectorDriver(SnapshotDriver):
    def __init__(self, count=1):
        super().__init__([{**strict_snapshot_row(), "snapshot_id": f"snapshot-{index}"} for index in range(count)])
        self.matches = {}
        self.selector_failure = None
        self.selector_storage = True
        self.mutate_page = None

    async def fetch(self, sql, *arguments):
        if any("ptg2_v3_snapshot_plan_scope" in name for name in arguments[0]) and not self.selector_storage:
            self.calls.append((sql, arguments))
            return [{"relation_name": name, "available": False} for name in arguments[0]]
        return await super().fetch(sql, *arguments)

    async def fetchrow(self, sql, *arguments):
        if "request.request_no" not in sql:
            return await super().fetchrow(sql, *arguments)
        self.calls.append((sql, arguments))
        self.tasks.append(asyncio.current_task())
        if self.selector_failure is not None:
            raise self.selector_failure
        selected_rows = [
            {"request_no": index, "snapshot_id": identity, "matches": self.matches.get(index, True)}
            for index, identity in enumerate(arguments[0], 1)
        ]
        if self.mutate_page:
            self.mutate_page(selected_rows)
        encoded = json.dumps(selected_rows)
        return {
            "isolation": self.isolation,
            "read_only": self.read_only,
            "bounded": len(encoded.encode()) <= arguments[-1],
            "rows_json": encoded if len(encoded.encode()) <= arguments[-1] else None,
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("count", (1, 8, 64))
async def test_real_row_consumer_uses_fixed_native_selector_phases(count):
    driver = SelectorDriver(count)
    requested = metadata([f"snapshot-{index}" for index in range(count)])
    checks = await rows.read_pricing_snapshot_row_checks(driver, requested, max_report_bytes=16 * 1024 * 1024)
    assert len(driver.calls) == 4 and all(check["binding_selector_status"] == "validated" for check in checks.values())
    assert all(check["full_readiness"] == "not_assessed" for check in checks.values())
    assert all(task is asyncio.current_task() for task in driver.tasks)
    assert driver.lifecycle == ["start", "release", "start", "release"] and driver.is_in_transaction()
    sql, arguments = driver.calls[-1]
    assert arguments[0] == [f"snapshot-{index}" for index in range(count)]
    assert "$1::text[]" in sql and "WITH ORDINALITY" in sql and ":source_key" not in sql
    assert sql.index("byte_count<=$6::bigint") < sql.index("jsonb_agg")


def test_selector_sql_reuses_exact_singleton_predicates():
    parameters_by_name = {}
    sql = selectors._selector_set_sql()
    assert _serving_relation_available_sql("ptg2_snapshot") in sql
    assert (
        _explicit_snapshot_source_sql("source", None, parameters_by_name).replace(":source_key", "request.source_key")
        in sql
    )
    plan = _explicit_snapshot_plan_sql("plan", "market", parameters_by_name)
    plan = plan.replace("CAST(:plan_ids AS text[])", "request.plan_ids").replace(
        "AND snapshot_scope.plan_market_type = :plan_market_type",
        "AND (request.plan_market_type = '' OR snapshot_scope.plan_market_type = request.plan_market_type)",
    )
    assert plan in sql and "state = 'complete'" in sql and "activated_at IS NOT NULL" in sql


def test_ein_variants_are_bound_for_each_exact_request():
    keys = (("s1", "source", "12-3456789", "group"), ("s2", "source", "other", ""))
    arguments = selectors._selector_arguments(keys, 1000)
    assert arguments[3] == [1, 1, 2]
    assert arguments[4] == [*ein_plan_id_variants("12-3456789"), "other"]
    assert selectors._selector_arguments(keys, 1) is None
    for limit in (True, 0, -1, 1.5):
        with pytest.raises(ValueError, match="bound_invalid"):
            selectors._selector_arguments(keys, limit)


@pytest.mark.asyncio
async def test_every_cross_release_sibling_scope_must_pass():
    driver = SelectorDriver(2)
    requested = metadata(["snapshot-0", "snapshot-1"])
    second_binding_by_field = dict(requested[next(iter(requested))]["bindings"][0], plan_market_type="individual")
    requested["another-release"] = {"bindings": [second_binding_by_field]}
    driver.matches[3] = False
    checks = await rows.read_pricing_snapshot_row_checks(driver, requested, max_report_bytes=16384)
    assert checks["snapshot-0"]["binding_selector_status"] == "unavailable"
    assert checks["snapshot-1"]["binding_selector_status"] == "validated"
    assert len(driver.calls) == 4 and len(driver.calls[-1][1][0]) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("source", "market", "plan", "selector", "missing_storage"))
async def test_scope_or_selector_failure_never_invents_readiness(change):
    driver = SelectorDriver()
    requested = metadata(["snapshot-0"])
    binding = requested[next(iter(requested))]["bindings"][0]
    if change == "source":
        binding["source_key"] = "other"
    elif change == "market":
        binding["plan_market_type"] = "individual"
    elif change == "plan":
        binding["plan_id"] = "another-plan"
    elif change == "selector":
        driver.matches[1] = False
    else:
        driver.selector_storage = False
    checks = await rows.read_pricing_snapshot_row_checks(driver, requested, max_report_bytes=16384)
    assert checks["snapshot-0"]["binding_selector_status"] == "unavailable"
    assert checks["snapshot-0"]["shared_descriptor_status"] == "validated"
    assert checks["snapshot-0"]["full_readiness"] == "not_assessed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    (asyncpg.UndefinedColumnError("fixture"), asyncpg.InsufficientPrivilegeError("fixture"), asyncio.CancelledError()),
)
async def test_optional_storage_and_original_error_priority(failure):
    driver = SelectorDriver()
    driver.selector_failure = failure
    if isinstance(failure, asyncpg.UndefinedColumnError):
        checks = await rows.read_pricing_snapshot_row_checks(driver, metadata(["snapshot-0"]), max_report_bytes=16384)
        assert checks["snapshot-0"]["binding_selector_status"] == "unavailable"
    else:
        with pytest.raises(type(failure)) as caught:
            await rows.read_pricing_snapshot_row_checks(driver, metadata(["snapshot-0"]), max_report_bytes=16384)
        assert caught.value is failure
    assert driver.lifecycle[-1] == "rollback" and driver.is_in_transaction()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    (
        lambda rows: rows.append(dict(rows[0])),
        lambda rows: rows.clear(),
        lambda rows: rows[0].update(snapshot_id="other"),
        lambda rows: rows[0].update(request_no=True),
    ),
)
async def test_closed_selector_identity_rejects_corruption(mutation):
    driver = SelectorDriver()
    driver.mutate_page = mutation
    with pytest.raises(ValueError, match="identity_invalid"):
        await rows.read_pricing_snapshot_row_checks(driver, metadata(["snapshot-0"]), max_report_bytes=16384)
    assert driver.lifecycle[-1] == "rollback"


@pytest.mark.asyncio
async def test_selector_rollback_failure_keeps_original_cancellation():
    driver = SelectorDriver()
    cancellation = asyncio.CancelledError("primary")
    driver.selector_failure = cancellation
    driver.rollback_error = RuntimeError("secondary")
    with pytest.raises(asyncio.CancelledError) as caught:
        await rows.read_pricing_snapshot_row_checks(driver, metadata(["snapshot-0"]), max_report_bytes=16384)
    assert caught.value is cancellation
    assert any("selector lookup rollback was incomplete" in note for note in cancellation.__notes__)


@pytest.mark.asyncio
async def test_logical_scope_flag_cannot_bypass_native_selector():
    driver = SelectorDriver()
    requested = metadata(["snapshot-0"])
    binding = requested[next(iter(requested))]["bindings"][0]
    binding.update(plan_id="different-plan", logical_scope_present=True)
    driver.matches[1] = False
    checks = await rows.read_pricing_snapshot_row_checks(driver, requested, max_report_bytes=16384)
    assert checks["snapshot-0"]["binding_selector_status"] == "unavailable"


@pytest.mark.asyncio
async def test_input_bound_refuses_selector_sql_without_releasing_parent():
    driver = SelectorDriver()
    exact = await selectors.read_pricing_binding_selectors(driver, metadata(["snapshot-0"]), {}, max_report_bytes=1)
    assert exact == {"snapshot-0": False}
    assert driver.calls == [] and driver.lifecycle == [] and driver.is_in_transaction()


@pytest.mark.asyncio
async def test_missing_binding_identity_is_trust_failure_before_query():
    driver = SelectorDriver()
    requested = metadata(["snapshot-0"])
    requested[next(iter(requested))]["bindings"][0].pop("source_key")
    with pytest.raises(ValueError, match="binding_invalid"):
        await selectors.read_pricing_binding_selectors(driver, requested, {}, max_report_bytes=16384)
    assert driver.calls == [] and driver.lifecycle == []
