# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named provider input metadata and independent rate recipe regressions."""

import asyncio
import copy
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest

from api import network_address_scope as address_scope
from api import network_read_execution as execution
from api import plan_pricing_em_attribution as attribution
from api import plan_pricing_em_distance as distance
from api import provider_service_source_policy as source_policy
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process import network_em_provider_inputs as inputs
from process import network_published_api_pin as published_pin
from tests import test_network_em_provider_inputs as input_fixture
from tests import test_plan_pricing_em_attribution as rate_fixture


class _InputDriver:
    def __init__(self, receipt):
        self.header = {
            "source_generations": {"em_provider_inputs": receipt["receipt_sha256"]},
            "validation_json": {"em_provider_inputs": copy.deepcopy(receipt)},
        }
        self.catalog = input_fixture.catalog()
        self.rows = [{"role": held.role} for held in input_fixture.family().relations]
        self.events, self.lock_error = [], None

    def is_in_transaction(self):
        return True

    async def fetchval(self, query, *_args):
        assert query == "SHOW transaction_isolation"
        return "repeatable read"

    async def execute(self, statement):
        assert self.events == ["header-read"]
        assert statement.startswith("LOCK TABLE ") and statement.count('"held_') == 9
        assert statement.endswith(" IN ACCESS SHARE MODE") and "NOWAIT" not in statement
        self.events.append("lock")
        if self.lock_error:
            raise self.lock_error

    async def fetch(self, query, relations_json):
        if query == inputs._CATALOG_SQL:
            assert self.events == ["header-read"]
            self.events.append("native-columns-and-keys")
            return self.catalog
        assert query == inputs._API_HEAPS_SQL and self.events[-1] == "lock"
        requested = json.loads(relations_json)
        assert [held["table_name"] for held in requested] == [f"held_{i}" for i in range(9)]
        assert [held["columns"] for held in requested] == [inputs._required_columns(role) for role in inputs._ROLES]
        self.events.append("heaps")
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
    # An API read holds the named inputs under a share lock; composition validates columns and keys.
    assert driver.events == ["header-read", *(["lock", "heaps"] if api_read else ["native-columns-and-keys"])]


async def test_input_header_reuses_the_published_pin_of_the_request(monkeypatch):
    driver, supplied, actual, resolver = await _input_reader(monkeypatch)
    reuse = Mock(return_value=actual)
    monkeypatch.setattr(published_pin, "active_published_api_pin", reuse)
    assert await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True) == input_fixture.family()
    reuse.assert_called_once_with(driver, supplied, control_schema="control")
    resolver.assert_not_awaited()


@pytest.mark.parametrize("damage", ["manifest", "missing", "sources", "receipt", "rotating", "row_filter", "lock"])
async def test_api_inputs_preserve_manifest_receipt_row_filter_and_lock_checks(monkeypatch, damage):
    driver, supplied, actual, resolver = await _input_reader(monkeypatch)
    if damage == "rotating":
        held = input_fixture.family().relations[1]
        monkeypatch.setattr(inputs, "_rotating_names", lambda: {(held.schema_name, held.table_name)})
    if damage == "manifest":
        resolver.return_value = replace(actual, manifest_sha256="b" * 64)
    if damage == "missing":
        driver.header = None
    if damage == "sources":
        driver.header["source_generations"] = {}
    if damage == "receipt":
        driver.header["validation_json"] = {}
    if damage == "row_filter":
        # Like the catalog query, a filtered, non-heap or column-lacking table returns no row.
        driver.rows.pop()
    if damage == "lock":
        driver.lock_error = asyncpg.LockNotAvailableError("canceling statement due to lock timeout")
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True)
    if damage == "manifest":
        assert driver.events == []
    elif damage not in {"row_filter", "lock"}:
        assert driver.events == ["header-read"]


def test_live_input_names_rotate_and_are_never_served_by_name():
    schema = inputs._MODELS["npi"].__table__.schema
    rotating = inputs._rotating_names()
    for name in ("geo_zip_lookup", "npi", "npi_old", "nucc_taxonomy_old", "npi_address", "mrf_address"):
        assert (schema, name) in rotating
    assert {("tiger", "zcta5"), ("tiger", "zip_state")} <= rotating
    assert not {(held.schema_name, held.table_name) for held in input_fixture.family().relations} & rotating


async def test_input_cancellation_remains_cancellation(monkeypatch):
    driver, supplied, _actual, resolver = await _input_reader(monkeypatch)
    resolver.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True)
    assert driver.events == []


class _RatesDriver:
    def __init__(self, receipt):
        self.receipt, self.modes, self.heaps = receipt, ("repeatable read", "on"), None
        self.columns = dict(attribution._RATE_COLUMNS)
        self.execute = AsyncMock()
        self.payload = {"count": 1, "bytes": 11, "rows_json": '[{"npi":1}]'}
        self.queries = []

    def is_in_transaction(self):
        return True

    async def fetch(self, query, relations_json):
        assert query == inputs._API_HEAPS_SQL
        self.execute.assert_awaited_once_with(f"LOCK TABLE {self.receipt.rates_relation} IN ACCESS SHARE MODE")
        (requested,) = json.loads(relations_json)
        assert requested == {
            "role": "rates",
            "schema_name": self.receipt.schema_name,
            "table_name": "rates",
            "columns": attribution._RATE_COLUMNS,
        }
        if self.heaps is not None:
            return self.heaps
        # Like the catalog query, a table without a required column or type returns no row.
        if any(self.columns.get(name) != kind for name, kind in requested["columns"].items()):
            return []
        return [{"role": "rates"}]

    async def fetchrow(self, query, *arguments):
        self.queries.append(query)
        if "transaction_isolation" in query:
            return self.modes
        assert "WITH source_rows AS MATERIALIZED" in query
        return self.payload


class _RatesSession:
    def __init__(self, driver):
        self.driver, self.active, self.info = driver, True, {}

    def get_transaction(self):
        return self if self.active else None

    async def connection(self):
        return SimpleNamespace(
            get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=self.driver))
        )


def _rates_reader(monkeypatch, receipt=None):
    _driver, selection, receipt = rate_fixture._resolver_setup(monkeypatch, receipt=receipt)
    session = _RatesSession(_RatesDriver(receipt))
    return session, selection, receipt


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
async def test_api_rates_admit_the_named_table_under_a_share_lock(monkeypatch, isolation):
    session, selection, receipt = _rates_reader(monkeypatch)
    session.driver.modes = (isolation, "on")
    assert await attribution.resolve_em_attribution(session, selection) == receipt
    attribution.builder._selected_release.assert_awaited_once_with(
        session, selection.plan_release_id, selection.serving_revision_id
    )
    attribution._catalog.assert_not_awaited()
    attribution._content.assert_not_awaited()
    session.driver.execute.assert_awaited_once_with(f"LOCK TABLE {receipt.rates_relation} IN ACCESS SHARE MODE")
    assert session.info == {}


@pytest.mark.parametrize(
    "damage", ["mode", "selection", "filtered", "namespace", "receipt", "recipe", "row_filter", "lock"]
)
async def test_api_rates_refuse_changed_recipe_or_refused_table(monkeypatch, damage):
    session, selection, _receipt = _rates_reader(monkeypatch)
    if damage == "mode":
        session.driver.modes = ("repeatable read", "off")
    if damage == "selection":
        attribution.builder._selected_release.return_value = replace(selection, binding_set_digest="f" * 64)
    if damage == "filtered":
        selection = replace(selection, bindings=(selection.bindings[0],))
    if damage == "namespace":
        attribution._namespace.return_value = None
    if damage == "receipt":
        attribution._namespace.return_value = {"receipt": "{}"}
    if damage == "recipe":
        other = rate_fixture._receipt(replace(selection, binding_set_digest="f" * 64))
        attribution._namespace.return_value = {"receipt": attribution._canonical(other.as_dict())}
    if damage == "row_filter":
        session.driver.heaps = []
    if damage == "lock":
        session.driver.execute.side_effect = asyncpg.LockNotAvailableError("canceling statement due to lock timeout")
    with pytest.raises(PlanPricingProjectionUnavailable):
        await attribution.resolve_em_attribution(session, selection)
    assert session.info == {}
    if damage not in {"row_filter", "lock"}:
        session.driver.execute.assert_not_awaited()


async def test_readiness_advertises_no_retry_when_rates_lack_a_read_column(monkeypatch):
    session, selection, _receipt = _rates_reader(monkeypatch)
    del session.driver.columns["code_index"]
    monkeypatch.setattr(address_scope, "current_network_address_scope", lambda: SimpleNamespace(manifest=object()))
    monkeypatch.setattr(source_policy, "admitted_plan_release_bindings", AsyncMock(return_value=()))
    providers = AsyncMock()
    monkeypatch.setattr(inputs, "resolve_em_provider_inputs_in_session", providers)
    assert await distance.is_em_distance_projection_ready(session, selection) is False
    providers.assert_not_awaited()


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
        await attribution.resolve_em_attribution(session, selection)
    session.driver.execute.assert_not_awaited()
    attribution.builder._selected_release.assert_not_awaited()


@pytest.mark.parametrize("overflow", [False, True])
async def test_named_rate_payload_keeps_current_bounded_decoder(monkeypatch, overflow):
    session, selection, receipt = _rates_reader(monkeypatch)
    await attribution.resolve_em_attribution(session, selection)
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
        await attribution.resolve_em_attribution(session, selection)
    session.driver.execute.assert_not_awaited()


@pytest.mark.parametrize("invalid", [None, 0, 1, "true"])
async def test_input_resolution_requires_a_boolean_api_flag(invalid):
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs(object(), object(), api_read=invalid)
