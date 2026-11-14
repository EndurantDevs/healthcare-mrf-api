# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named provider input metadata and independent rate recipe regressions."""

import asyncio
import copy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import network_read_execution as execution
from api import plan_pricing_em_attribution as attribution
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process import network_em_provider_inputs as inputs
from tests import test_network_em_provider_inputs as input_fixture
from tests import test_plan_pricing_em_attribution as rate_fixture


class _InputDriver:
    def __init__(self, receipt):
        self.header = {
            "source_generations": {"em_provider_inputs": receipt["receipt_sha256"]},
            "validation_json": {"em_provider_inputs": copy.deepcopy(receipt)},
        }
        self.rows = input_fixture.catalog()
        self.events = []
        self.execute = AsyncMock(side_effect=AssertionError("provider reads must not write"))

    def is_in_transaction(self):
        return True

    async def fetchval(self, query, *_args):
        assert query == "SHOW transaction_isolation"
        return "repeatable read"

    async def fetch(self, query, *_args):
        assert query == inputs._CATALOG_SQL and self.events == ["header-read"]
        self.events.append("native-columns-and-keys")
        return self.rows

    async def fetchrow(self, query, *arguments):
        assert "network_membership_candidate" in query and "state='published'" in query
        assert "FOR UPDATE" not in query and "LOCK TABLE" not in query
        self.events.append("header-read")
        return self.header


async def _input_reader(monkeypatch):
    receipt = await inputs.prepare_em_provider_inputs(input_fixture.connection(), input_fixture.family())
    supplied = input_fixture.manifest(receipt)
    actual = replace(supplied)
    driver = _InputDriver(receipt)
    resolver = AsyncMock(return_value=actual)
    monkeypatch.setattr(inputs, "resolve_network_serving_manifest", resolver)
    return driver, supplied, actual, resolver


@pytest.mark.parametrize("api_read", [False, True])
async def test_input_header_binds_published_logical_names_before_data_read(monkeypatch, api_read):
    driver, supplied, _actual, resolver = await _input_reader(monkeypatch)
    assert (
        await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=api_read)
        == input_fixture.family()
    )
    resolver.assert_awaited_once_with(
        driver,
        generation_id=supplied.generation_id,
        control_schema="control",
        **({"api_read": True} if api_read else {}),
    )
    assert driver.events == ["header-read", "native-columns-and-keys"]
    driver.execute.assert_not_awaited()


@pytest.mark.parametrize("damage", ["manifest", "missing", "sources", "receipt", "columns", "keys"])
async def test_api_inputs_refuse_changed_logical_header_or_required_data_shape(monkeypatch, damage):
    driver, supplied, actual, resolver = await _input_reader(monkeypatch)
    if damage == "manifest":
        resolver.return_value = replace(actual, manifest_sha256="b" * 64)
    if damage == "missing":
        driver.header = None
    if damage == "sources":
        driver.header["source_generations"] = {}
    if damage == "receipt":
        driver.header["validation_json"] = {}
    if damage == "columns":
        driver.rows[0]["columns_json"] = "[]"
    if damage == "keys":
        driver.rows[inputs._ROLES.index("npi")]["keys_json"] = "[]"
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True)
    assert driver.events == (
        []
        if damage == "manifest"
        else ["header-read"] + (["native-columns-and-keys"] if damage in {"columns", "keys"} else [])
    )
    driver.execute.assert_not_awaited()


async def test_input_cancellation_remains_cancellation(monkeypatch):
    driver, supplied, _actual, resolver = await _input_reader(monkeypatch)
    resolver.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True)
    assert driver.events == []
    driver.execute.assert_not_awaited()


class _RatesDriver:
    def __init__(self):
        self.modes = ("repeatable read", "on")
        self.execute = AsyncMock(side_effect=AssertionError("API rates reads must not write"))
        self.fetch = AsyncMock(return_value=[])
        self.payload = {"count": 1, "bytes": 11, "rows_json": '[{"npi":1}]'}
        self.queries = []

    def is_in_transaction(self):
        return True

    async def fetchrow(self, query, *arguments):
        self.queries.append(query)
        if "transaction_isolation" in query:
            return self.modes
        assert "WITH source_rows AS MATERIALIZED" in query
        return self.payload


class _RatesSession:
    def __init__(self, driver):
        self.driver, self.active = driver, True

    def get_transaction(self):
        return self if self.active else None

    async def connection(self):
        return SimpleNamespace(
            get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=self.driver))
        )


def _rates_reader(monkeypatch):
    _driver, selection, receipt = rate_fixture._resolver_setup(monkeypatch)
    session = _RatesSession(_RatesDriver())
    return session, selection, receipt


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
async def test_api_rates_preserve_full_release_and_named_receipt(monkeypatch, isolation):
    session, selection, receipt = _rates_reader(monkeypatch)
    session.driver.modes = (isolation, "on")
    assert await attribution.resolve_em_attribution(session, selection, api_read=True) == receipt
    attribution.builder._selected_release.assert_awaited_once_with(
        session, selection.plan_release_id, selection.serving_revision_id
    )
    attribution._catalog.assert_not_awaited()
    attribution._content.assert_not_awaited()
    session.driver.fetch.assert_awaited_once_with(
        "SELECT binding_ordinal,npi,code_index,minimum_rate,maximum_rate,rate_count "
        f"FROM {receipt.rates_relation} LIMIT 0"
    )
    session.driver.execute.assert_not_awaited()


@pytest.mark.parametrize("damage", ["mode", "selection", "namespace", "receipt", "select"])
async def test_api_rates_refuse_changed_recipe_or_native_access(monkeypatch, damage):
    import asyncpg

    session, selection, _receipt = _rates_reader(monkeypatch)
    if damage == "mode":
        session.driver.modes = ("repeatable read", "off")
    if damage == "selection":
        attribution.builder._selected_release.return_value = replace(selection, binding_set_digest="f" * 64)
    if damage == "namespace":
        attribution._namespace.return_value = None
    if damage == "receipt":
        attribution._namespace.return_value = {"receipt": "{}"}
    if damage == "select":
        session.driver.fetch.side_effect = asyncpg.InsufficientPrivilegeError("SELECT denied")
    with pytest.raises(PlanPricingProjectionUnavailable):
        await attribution.resolve_em_attribution(session, selection, api_read=True)
    if damage != "select":
        session.driver.fetch.assert_not_awaited()
    session.driver.execute.assert_not_awaited()


@pytest.mark.parametrize("damage", ["transaction", "isolation", "readonly"])
async def test_rates_require_an_active_read_only_repeatable_snapshot(monkeypatch, damage):
    session, selection, _receipt = _rates_reader(monkeypatch)
    if damage == "transaction":
        session.active = False
    elif damage == "isolation":
        session.driver.modes = ("read committed", "on")
    else:
        session.driver.modes = ("repeatable read", "off")
    with pytest.raises(PlanPricingProjectionUnavailable):
        await attribution.resolve_em_attribution(session, selection, api_read=True)
    session.driver.fetch.assert_not_awaited()
    attribution.builder._selected_release.assert_not_awaited()


@pytest.mark.parametrize("overflow", [False, True])
async def test_named_rate_payload_keeps_current_bounded_decoder(monkeypatch, overflow):
    session, selection, receipt = _rates_reader(monkeypatch)
    await attribution.resolve_em_attribution(session, selection, api_read=True)
    if overflow:
        session.driver.payload = {"count": 1, "bytes": 8388609, "rows_json": None}
        with pytest.raises(ValueError, match="bound"):
            await execution.bounded_network_read_rows(session, f"SELECT npi FROM {receipt.rates_relation}")
    else:
        assert await execution.bounded_network_read_rows(session, f"SELECT npi FROM {receipt.rates_relation}") == [
            {"npi": 1}
        ]
    assert receipt.rates_relation in session.driver.queries[-1]


async def test_rates_cancellation_remains_cancellation(monkeypatch):
    session, selection, _receipt = _rates_reader(monkeypatch)
    attribution.builder._selected_release.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await attribution.resolve_em_attribution(session, selection, api_read=True)
    session.driver.fetch.assert_not_awaited()


@pytest.mark.parametrize("invalid", [None, 0, 1, "true"])
async def test_both_api_modes_require_boolean_flags(invalid):
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs(object(), object(), api_read=invalid)
    with pytest.raises(PlanPricingProjectionUnavailable):
        await attribution.resolve_em_attribution(object(), object(), api_read=invalid)
