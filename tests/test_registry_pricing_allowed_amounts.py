# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual role SQL consumption, fixed native phases and trust failures; no PG."""

import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace

import asyncpg
import pytest

from api.plan_release_readiness import _ALLOWED_AMOUNT_BINDING_READINESS_SQL, has_allowed_amount_binding_coverage
from api.plan_release_serving import PlanReleaseSnapshotBinding
from process import registry_pricing_allowed_amounts as allowed
from process import registry_pricing_release_binding_lookup as lookup
from process import registry_pricing_snapshot_rows as rows
from process.network_approved_catalog_evidence import ApprovedNetworkCatalogEvidenceRecord
from tests.ptg2_manifest_tables_support import strict_snapshot_row
from tests.test_plan_release_serving import _binding_row
from tests.test_registry_pricing_release_binding_lookup import REFERENCE, REPORT
from tests.test_registry_pricing_snapshot_rows import SnapshotDriver, metadata


class AmountDriver(SnapshotDriver):
    def __init__(self):
        super().__init__()
        self.amount_matches = {}
        self.amount_storage = True
        self.amount_failure = None
        self.amount_page_change = None

    async def fetch(self, sql, *arguments):
        if any("ptg2_allowed_amount_plan" in name for name in arguments[0]):
            self.calls.append((sql, arguments))
            self.tasks.append(asyncio.current_task())
            return [{"relation_name": name, "available": self.amount_storage} for name in arguments[0]]
        return await super().fetch(sql, *arguments)

    async def fetchrow(self, sql, *arguments):
        if "ptg2_allowed_amount_plan" not in sql:
            return await super().fetchrow(sql, *arguments)
        self.calls.append((sql, arguments))
        self.tasks.append(asyncio.current_task())
        if self.amount_failure is not None:
            raise self.amount_failure
        selected_rows = [
            {"request_no": index, "snapshot_id": identity, "matches": self.amount_matches.get(index, True)}
            for index, identity in enumerate(arguments[0], 1)
        ]
        if self.amount_page_change:
            self.amount_page_change(selected_rows)
        encoded = json.dumps(selected_rows)
        return {
            "isolation": self.isolation,
            "read_only": self.read_only,
            "bounded": len(encoded.encode()) <= arguments[5],
            "rows_json": encoded if len(encoded.encode()) <= arguments[5] else None,
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("count", (1, 8, 64))
async def test_allowed_only_actual_consumer_uses_one_native_set(count):
    driver = AmountDriver()
    requested = metadata([f"allowed-{index}" for index in range(count)], role="allowed_amounts")
    checks = await rows.read_pricing_snapshot_row_checks(driver, requested, max_report_bytes=16 * 1024 * 1024)
    assert len(driver.calls) == 2 and len(checks) == count
    assert all(
        check["allowed_amounts_status"] == "validated" and check["full_readiness"] == "not_assessed"
        for check in checks.values()
    )
    assert driver.lifecycle == ["start", "release"] and driver.is_in_transaction()
    assert all(task is asyncio.current_task() for task in driver.tasks)
    sql, arguments = driver.calls[-1]
    assert arguments[0] == [f"allowed-{index}" for index in range(count)]
    assert arguments[6:] == (allowed.PTG2_ALLOWED_AMOUNT_CONTRACT, allowed.PTG2_DOMAIN_ALLOWED_AMOUNT)
    assert sql.index("byte_count<=$6::bigint") < sql.index("jsonb_agg")


def test_set_contains_exact_authoritative_five_table_expression():
    expression = _ALLOWED_AMOUNT_BINDING_READINESS_SQL.strip().removeprefix("SELECT ")
    replacements_by_parameter = {
        "CAST(:plan_ids AS text[])": "request.plan_ids",
        ":snapshot_id": "request.snapshot_id",
        ":source_key": "request.source_key",
        ":market_type": "request.plan_market_type",
        ":allowed_contract": "$7::text",
        ":allowed_data_domain": "$8::text",
    }
    for parameter, value in replacements_by_parameter.items():
        expression = expression.replace(parameter, value)
    sql = allowed._allowed_amount_set_sql()
    assert expression in sql
    assert "cardinality(provider_payment.npi) > 0" in sql and "allowed_item.file_id = plan_coverage.file_id" in sql
    assert "provider_payment.payment_hash = allowed_payment.payment_hash" in sql
    assert "allowed_payment.allowed_item_hash" in sql and "json_typeof" in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("singleton_result", (True, False))
async def test_boolean_coverage_matches_consumed_singleton(singleton_result):
    requested = metadata(["allowed"], role="allowed_amounts")
    binding = PlanReleaseSnapshotBinding(**requested[next(iter(requested))]["bindings"][0])

    class Session:
        async def scalar(self, statement, parameters):
            assert str(statement) == _ALLOWED_AMOUNT_BINDING_READINESS_SQL
            assert parameters["snapshot_id"] == binding.snapshot_id
            return singleton_result

    singleton = await has_allowed_amount_binding_coverage(Session(), binding)
    driver = AmountDriver()
    driver.amount_matches[1] = singleton_result
    actual = await allowed.read_pricing_allowed_amount_checks(driver, requested, max_report_bytes=16384)
    assert actual == {"allowed": singleton}


@pytest.mark.asyncio
async def test_unreferenced_cross_release_sibling_cannot_be_overwritten():
    driver = AmountDriver()
    requested = metadata(["allowed-0", "allowed-1"], role="allowed_amounts")
    sibling_binding_by_field = dict(
        requested[next(iter(requested))]["bindings"][0], plan_market_type="individual", required=False
    )
    requested["other-release"] = {"bindings": [sibling_binding_by_field, dict(sibling_binding_by_field)]}
    driver.amount_matches[3] = False
    checks = await rows.read_pricing_snapshot_row_checks(driver, requested, max_report_bytes=16384)
    assert checks["allowed-0"]["allowed_amounts_status"] == "unavailable"
    assert checks["allowed-1"]["allowed_amounts_status"] == "validated"
    assert len(driver.calls) == 2 and len(driver.calls[-1][1][0]) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    (
        asyncpg.UndefinedTableError("fixture"),
        asyncpg.UndefinedColumnError("fixture"),
        asyncpg.InsufficientPrivilegeError("fixture"),
        asyncio.CancelledError(),
    ),
)
async def test_optional_storage_cancellation_permission_and_primary_error(failure):
    driver = AmountDriver()
    driver.amount_failure = failure
    requested = metadata(["allowed"], role="allowed_amounts")
    if isinstance(failure, allowed._STORAGE_ERRORS):
        result = await rows.read_pricing_snapshot_row_checks(driver, requested, max_report_bytes=16384)
        assert result["allowed"]["allowed_amounts_status"] == "unavailable"
    else:
        with pytest.raises(type(failure)) as caught:
            await rows.read_pricing_snapshot_row_checks(driver, requested, max_report_bytes=16384)
        assert caught.value is failure
    assert driver.lifecycle == ["start", "rollback"] and driver.is_in_transaction()


@pytest.mark.asyncio
async def test_rollback_uncertainty_preserves_original_failure():
    driver = AmountDriver()
    primary = asyncpg.UndefinedColumnError("primary")
    driver.amount_failure = primary
    driver.rollback_error = asyncio.CancelledError("secondary")
    with pytest.raises(asyncpg.UndefinedColumnError) as caught:
        await allowed.read_pricing_allowed_amount_checks(
            driver, metadata(["allowed"], role="allowed_amounts"), max_report_bytes=16384
        )
    assert caught.value is primary and any("rollback was incomplete" in note for note in primary.__notes__)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    (
        lambda page: page.clear(),
        lambda page: page.append(dict(page[0])),
        lambda page: page[0].update(request_no=True),
        lambda page: page[0].update(snapshot_id="other"),
        lambda page: page[0].update(matches=1),
    ),
)
async def test_complete_typed_role_result_identity_is_required(change):
    driver = AmountDriver()
    driver.amount_page_change = change
    with pytest.raises(ValueError, match="identity_invalid"):
        await allowed.read_pricing_allowed_amount_checks(
            driver, metadata(["allowed"], role="allowed_amounts"), max_report_bytes=16384
        )
    assert driver.lifecycle[-1] == "rollback"


@pytest.mark.asyncio
async def test_missing_relation_and_bounds_are_unavailable_without_authority():
    driver = AmountDriver()
    requested = metadata(["allowed"], role="allowed_amounts")
    assert await allowed.read_pricing_allowed_amount_checks(driver, requested, max_report_bytes=1) == {"allowed": False}
    assert driver.calls == []
    driver.amount_storage = False
    assert await allowed.read_pricing_allowed_amount_checks(driver, requested, max_report_bytes=16384) == {
        "allowed": False
    }
    assert len(driver.calls) == 1 and driver.lifecycle == ["start", "release"]


@pytest.mark.asyncio
async def test_role_collision_preserves_network_status_and_failed_amount_sibling():
    driver = AmountDriver()
    driver.snapshots = [{**strict_snapshot_row(), "snapshot_id": "both"}]
    requested = metadata(["both"])
    requested["allowed-release"] = metadata(["both"], role="allowed_amounts")[next(iter(requested))]
    driver.amount_matches[1] = False
    actual = await rows.read_pricing_snapshot_row_checks(driver, requested, max_report_bytes=16384)
    assert actual["both"]["status"] == "published_row_validated"
    assert actual["both"]["allowed_amounts_status"] == "unavailable"
    assert actual["both"]["full_readiness"] == "not_assessed" and len(driver.calls) == 6


@pytest.mark.asyncio
async def test_retained_report_consumes_unreferenced_amount_siblings(monkeypatch):
    driver = AmountDriver()
    driver.rows = [
        _binding_row(expected_binding_count=3),
        _binding_row(role="allowed_amounts", snapshot_id="amount-a", expected_binding_count=3),
        _binding_row(role="allowed_amounts", snapshot_id="amount-b", binding_ordinal=1, expected_binding_count=3),
    ]
    driver.snapshots = [{**strict_snapshot_row(), "snapshot_id": REFERENCE["snapshot_id"]}]
    driver.amount_matches[2] = False

    async def retained_export(*args, **kwargs):
        return SimpleNamespace(
            records=(ApprovedNetworkCatalogEvidenceRecord(1, 3, 2, "retained"),),
            approved_revision=4,
            approved_map_sha256="a" * 64,
            request_sha256="b" * 64,
            request_bytes=json.dumps(
                {"networks": [{"network_id": 1, "pricing_refs": [REFERENCE], "benefit_refs": None}]}
            ).encode(),
        )

    monkeypatch.setattr(lookup, "read_approved_network_catalog_evidence", retained_export)
    actual = await lookup.append_pricing_binding_metadata(
        driver, deepcopy(REPORT), object(), control_schema="sample_control", max_report_bytes=16 * 1024 * 1024
    )
    provenance = actual["provenance"]["pricing_binding_metadata"]
    assert provenance["snapshot_row_checks"]["amount-a"]["allowed_amounts_status"] == "validated"
    assert provenance["snapshot_row_checks"]["amount-b"]["allowed_amounts_status"] == "unavailable"
    assert provenance["networks"][0]["pricing_refs"][0]["resolved"] is False
    assert provenance["full_readiness"] == "not_assessed"
    assert actual["targets"] == REPORT["targets"] and actual["totals"] == REPORT["totals"]
    assert len(driver.calls) == 8 and driver.lifecycle == ["start", "release"] * 4
    assert all(task is asyncio.current_task() for task in driver.tasks) and driver.is_in_transaction()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("read_committed", "writeable", "ended", "release_failed"))
async def test_amount_parent_transaction_and_release_are_not_optional(mode):
    driver = AmountDriver()
    expected_failure = None
    if mode == "read_committed":
        driver.isolation = "read committed"
    if mode == "writeable":
        driver.read_only = "off"
    if mode == "ended":
        driver.in_transaction = False
    if mode == "release_failed":
        expected_failure = asyncpg.PostgresError("release")
        driver.release_error = expected_failure
    with pytest.raises(asyncpg.PostgresError if expected_failure else ValueError) as caught:
        await rows.read_pricing_snapshot_row_checks(
            driver, metadata(["allowed"], role="allowed_amounts"), max_report_bytes=16384
        )
    assert expected_failure is None or caught.value is expected_failure
    assert driver.lifecycle == (
        [] if mode == "ended" else ["start", "release"] if mode == "release_failed" else ["start", "rollback"]
    )


@pytest.mark.asyncio
async def test_native_arguments_match_singleton_normalization_and_variants():
    requested = metadata(["allowed"], role="allowed_amounts")
    binding_fields = requested[next(iter(requested))]["bindings"][0]
    binding_fields.update(source_key=" Source-A ", plan_id="12-3456789", plan_market_type=" GROUP ")
    captured_params_by_name = {}

    has_coverage = True

    class Session:
        async def scalar(self, statement, parameters):
            captured_params_by_name.update(parameters)
            return has_coverage

    await has_allowed_amount_binding_coverage(Session(), PlanReleaseSnapshotBinding(**binding_fields))
    driver = AmountDriver()
    await allowed.read_pricing_allowed_amount_checks(driver, requested, max_report_bytes=16384)
    arguments = driver.calls[-1][1]
    assert arguments[0] == [captured_params_by_name["snapshot_id"]]
    assert arguments[1] == [captured_params_by_name["source_key"]]
    assert arguments[2] == [captured_params_by_name["market_type"]]
    assert arguments[4] == captured_params_by_name["plan_ids"]
    assert arguments[3] == [1] * len(arguments[4])
    assert arguments[6:] == (
        captured_params_by_name["allowed_contract"],
        captured_params_by_name["allowed_data_domain"],
    )
