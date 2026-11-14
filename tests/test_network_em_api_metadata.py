# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Guarded E&M metadata seams; protocol doubles are not native custody proof."""

import asyncio
import copy
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest
from sanic.exceptions import ServiceUnavailable

from api import network_address_scope as address_scope
from api import network_read_execution as execution
from api import plan_pricing_em_attribution as attribution
from api import plan_pricing_em_distance as distance
from api import provider_service_source_policy as source_policy
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process import network_candidate_custody as custody
from process import network_em_provider_inputs as inputs
from process import network_published_api_pin as published_pin
from process import network_read_identity as identity
from process.network_compact_layout import CompactAddressRelation
from tests import test_network_em_provider_inputs as input_fixture
from tests import test_plan_pricing_em_attribution as rate_fixture


class _Header(dict):
    def __init__(self, driver, values):
        super().__init__(values)
        self.driver = driver

    def __getitem__(self, name):
        assert self.driver.header_guarded
        return super().__getitem__(name)


class _InputDriver:
    def __init__(self, receipt):
        self.header_guarded, self.is_valid, self.pid = False, True, 42
        self.current_control_oid, self.modes = 800, ("repeatable read", "on")
        self.header = _Header(
            self,
            {
                "source_generations": {"em_provider_inputs": receipt["receipt_sha256"]},
                "validation_json": {"em_provider_inputs": copy.deepcopy(receipt)},
            },
        )
        self.rows = [
            {"role": held.role, "relation_oid": held.relation_oid, "relfilenode": held.relfilenode}
            for held in input_fixture.family().relations
        ]
        self.events, self.lock_error = [], None

    def is_in_transaction(self):
        return True

    def get_server_pid(self):
        return self.pid

    async def fetchval(self, query, *_args):
        return "repeatable read" if query.startswith("SHOW") else True

    async def execute(self, statement):
        assert self.header_guarded and statement.startswith("LOCK TABLE ") and statement.count('"held_') == 9
        assert statement.endswith(" IN ACCESS SHARE MODE") and "NOWAIT" not in statement
        self.events.append("lock")
        if self.lock_error:
            raise self.lock_error

    async def fetch(self, query, relations_json):
        assert query == inputs._API_HEAPS_SQL and self.events[-1] == "lock"
        requested = json.loads(relations_json)
        assert [held["table_name"] for held in requested] == [f"held_{i}" for i in range(9)]
        assert [held["columns"] for held in requested] == [inputs._required_columns(role) for role in inputs._ROLES]
        self.events.append("heaps")
        return self.rows

    async def fetchrow(self, query, *arguments):
        if "transaction_isolation" in query:
            return self.modes
        if query == identity._CONSUMED_SQL:
            names, oids, pid, _modes = arguments
            assert names == ['"control"."network_membership_candidate"'] and oids == [800] and pid == 42
            self.header_guarded = True
            self.events.append("header-guard")
            return {
                "backend_matches": self.pid == pid,
                "relation_count": 1,
                "valid": self.is_valid and oids == [self.current_control_oid],
            }
        if query == identity._BOUND_NAMES_SQL:
            names, oids, pid = arguments
            assert len(names) == 9 and set(oids) == set(range(100, 109))
            return {"backend_matches": self.pid == pid, "relation_count": 9, "valid": self.is_valid}
        assert "network_membership_candidate" in query and "state='published'" in query
        assert 'FROM ONLY "control".network_membership_candidate ' in query
        assert "FOR UPDATE" not in query and "LOCK TABLE" not in query
        self.events.append("header-read")
        return self.header


async def _input_reader(monkeypatch):
    receipt = await inputs.prepare_em_provider_inputs(
        input_fixture.connection(), input_fixture.family(), runtime_roles=("runtime_reader",)
    )
    supplied = input_fixture.manifest(receipt)
    actual = replace(supplied)
    driver = _InputDriver(receipt)
    resolver = AsyncMock(return_value=actual)
    monkeypatch.setattr(inputs, "resolve_network_serving_manifest", resolver)
    pins = AsyncMock(return_value=(CompactAddressRelation("control", "network_membership_candidate", 800),))
    monkeypatch.setattr(custody, "read_network_control_relation_pins", pins)
    return driver, supplied, actual, resolver, pins


async def test_input_header_uses_the_genuine_resolved_control_pin_before_decode(monkeypatch):
    driver, supplied, _actual, resolver, pins = await _input_reader(monkeypatch)
    assert await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True) == input_fixture.family()
    resolver.assert_awaited_once_with(
        driver, generation_id=supplied.generation_id, control_schema="control", api_read=True
    )
    pins.assert_awaited_once_with(driver, ("network_membership_candidate",), control_schema="control")
    assert driver.events == ["header-read", "header-guard", "lock", "heaps"]


async def test_api_inputs_serve_the_tables_their_names_resolve_to_now(monkeypatch):
    driver, supplied, *_ = await _input_reader(monkeypatch)
    for held in driver.rows:
        held["relation_oid"] += 5000
        held["relfilenode"] += 5000
    actual = await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True)
    assert [held.relation_oid for held in actual.relations] == [5100 + i for i in range(9)]
    assert [held.relfilenode for held in actual.relations] == [5200 + i for i in range(9)]
    assert actual == inputs.EMProviderInputs(
        tuple(
            replace(held, relation_oid=held.relation_oid + 5000, relfilenode=held.relfilenode + 5000)
            for held in input_fixture.family().relations
        )
    )


async def test_input_header_reuses_the_published_pin_of_the_request(monkeypatch):
    driver, supplied, actual, resolver, _pins = await _input_reader(monkeypatch)
    reuse = Mock(return_value=actual)
    monkeypatch.setattr(published_pin, "active_published_api_pin", reuse)
    assert await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True) == input_fixture.family()
    reuse.assert_called_once_with(driver, supplied, control_schema="control")
    resolver.assert_not_awaited()


@pytest.mark.parametrize(
    "damage", ["manifest", "guard", "missing", "sources", "receipt", "rotating", "row_filter", "lock", "pid"]
)
async def test_api_inputs_preserve_manifest_receipt_row_filter_and_consumption_checks(monkeypatch, damage):
    driver, supplied, actual, resolver, pins = await _input_reader(monkeypatch)
    if damage == "rotating":
        held = input_fixture.family().relations[1]
        monkeypatch.setattr(inputs, "_rotating_names", lambda: {(held.schema_name, held.table_name)})
    if damage == "manifest":
        resolver.return_value = replace(actual, address_table_oid=999)
    if damage == "guard":
        driver.is_valid = False
    if damage == "missing":
        driver.header = None
    if damage == "sources":
        dict.__setitem__(driver.header, "source_generations", {})
    if damage == "receipt":
        dict.__setitem__(driver.header, "validation_json", {})
    if damage == "row_filter":
        driver.rows.pop()
    if damage == "lock":
        driver.lock_error = asyncpg.LockNotAvailableError("canceling statement due to lock timeout")

    async def moved(*_args, **_kwargs):
        driver.pid += 1
        return actual

    if damage == "pid":
        resolver.side_effect = moved
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True)
    if damage == "manifest":
        assert driver.events == []
        pins.assert_not_awaited()
    elif damage not in {"row_filter", "lock"}:
        assert "lock" not in driver.events


def test_live_input_names_rotate_and_are_never_served_by_name():
    schema = inputs._MODELS["npi"].__table__.schema
    rotating = inputs._rotating_names()
    for name in ("geo_zip_lookup", "npi", "npi_old", "nucc_taxonomy_old", "npi_address", "mrf_address"):
        assert (schema, name) in rotating
    assert {("tiger", "zcta5"), ("tiger", "zip_state")} <= rotating
    assert not {(held.schema_name, held.table_name) for held in input_fixture.family().relations} & rotating


async def test_input_cancellation_is_not_converted_to_metadata_unavailability(monkeypatch):
    driver, supplied, _actual, resolver, _pins = await _input_reader(monkeypatch)
    resolver.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True)


class _RatesResult:
    def __init__(self, session):
        self.session, self.before, self.closed = session, session.driver.guards, False

    def all(self):
        assert self.session.driver.guards > self.before
        return [("rate payload",)]

    def close(self):
        self.closed = True


class _RatesDriver:
    def __init__(self, receipt):
        self.receipt, self.pid, self.guards, self.is_valid = receipt, 42, 0, True
        self.modes = ("repeatable read", "on")
        self.after_guard = None
        self.current_oid, self.heaps = receipt.table_oid, None
        self.columns = dict(attribution._RATE_COLUMNS)
        self.execute = AsyncMock()

    def is_in_transaction(self):
        return True

    def get_server_pid(self):
        return self.pid

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
        return [{"role": "rates", "relation_oid": self.current_oid, "relfilenode": 1}]

    async def fetchrow(self, query, *arguments):
        if "transaction_isolation" in query:
            return self.modes
        if query == identity._BOUND_NAMES_SQL:
            names, oids, pid = arguments
            assert names == [f'"{self.receipt.schema_name}"."rates"'] and oids == [self.current_oid]
            self.guards += 1
            receipt_by_field = {"backend_matches": pid == self.pid, "relation_count": 1, "valid": self.is_valid}
            if self.after_guard:
                self.after_guard()
            return receipt_by_field
        raise AssertionError("API rates admission reads no other catalog row")


class _RatesSession:
    def __init__(self, driver):
        self.driver, self.transaction, self.info = driver, object(), {}
        self.result = None
        self.drift = False

    def in_transaction(self):
        return True

    def get_transaction(self):
        return self.transaction

    async def connection(self):
        return SimpleNamespace(
            get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=self.driver))
        )

    async def execute(self, _statement, _parameters=None):
        self.result = _RatesResult(self)
        if self.drift:
            self.driver.is_valid = False
        return self.result


def _rates_reader(monkeypatch, receipt=None):
    _old_driver, selection, receipt = rate_fixture._resolver_setup(monkeypatch, receipt=receipt)
    driver = _RatesDriver(receipt)
    session = _RatesSession(driver)
    return session, selection, receipt


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
async def test_api_rates_admit_by_name_and_install_the_current_table(monkeypatch, isolation):
    session, selection, receipt = _rates_reader(monkeypatch)
    session.driver.modes = (isolation, "on")
    session.driver.current_oid = receipt.table_oid + 1000
    assert await attribution.resolve_em_attribution(session, selection) == receipt
    attribution.builder._selected_release.assert_awaited_once_with(
        session, selection.plan_release_id, selection.serving_revision_id
    )
    attribution._catalog.assert_not_awaited()
    attribution._closed.assert_not_awaited()
    attribution._content.assert_not_awaited()
    installed = execution._installed_read_inventory(session)
    assert installed.relations == (CompactAddressRelation(receipt.schema_name, "rates", receipt.table_oid + 1000),)
    assert installed.connection is session.driver and installed.backend_pid == 42
    session.driver.execute.assert_awaited_once_with(f"LOCK TABLE {receipt.rates_relation} IN ACCESS SHARE MODE")


@pytest.mark.parametrize("damage", ["mode", "selection", "filtered", "namespace", "recipe", "row_filter", "lock"])
async def test_api_rates_do_not_publish_when_admission_fails(monkeypatch, damage):
    session, selection, _receipt = _rates_reader(monkeypatch)
    if damage == "mode":
        session.driver.modes = ("repeatable read", "off")
    if damage == "selection":
        attribution.builder._selected_release.return_value = replace(selection, binding_set_digest="f" * 64)
    if damage == "filtered":
        selection = replace(selection, bindings=(selection.bindings[0],))
    if damage == "namespace":
        attribution._namespace.return_value = None
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
    assert session.info == {}


@pytest.mark.parametrize("phase", ["selection", "install"])
@pytest.mark.parametrize("change", ["driver", "pid", "transaction"])
async def test_rates_resolution_cannot_move_the_original_snapshot(monkeypatch, change, phase):
    session, selection, receipt = _rates_reader(monkeypatch)

    def mutate():
        if change == "driver":
            session.driver = _RatesDriver(receipt)
        elif change == "pid":
            session.driver.pid += 1
        else:
            session.transaction = object()

    async def selected(*_args):
        mutate()
        return selection

    if phase == "selection":
        attribution.builder._selected_release.side_effect = selected
    else:
        session.driver.after_guard = mutate
    with pytest.raises(PlanPricingProjectionUnavailable):
        await attribution.resolve_em_attribution(session, selection)
    if phase == "selection":
        assert session.info == {}


@pytest.mark.parametrize("drift", [False, True])
async def test_installed_rate_inventory_guards_actual_buffered_rows_before_cache(monkeypatch, drift):
    session, selection, receipt = _rates_reader(monkeypatch)
    await attribution.resolve_em_attribution(session, selection)
    session.drift = drift
    cached_rows = []
    if drift:
        with pytest.raises(ServiceUnavailable):
            cached_rows.extend(
                (await execution.execute_network_read(session, f"SELECT * FROM {receipt.rates_relation}")).all()
            )
        assert session.result.closed
    else:
        cached_rows.extend(
            (await execution.execute_network_read(session, f"SELECT * FROM {receipt.rates_relation}")).all()
        )
    assert cached_rows == ([] if drift else [("rate payload",)])


async def test_rates_cancellation_does_not_publish_an_inventory(monkeypatch):
    session, selection, _receipt = _rates_reader(monkeypatch)
    attribution.builder._selected_release.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await attribution.resolve_em_attribution(session, selection)
    assert session.info == {}


async def test_rates_initial_connection_acquisition_cannot_replace_transaction(monkeypatch):
    session, selection, _receipt = _rates_reader(monkeypatch)
    acquire = session.connection

    async def changed_connection():
        session.transaction = object()
        return await acquire()

    session.connection = changed_connection
    with pytest.raises(PlanPricingProjectionUnavailable):
        await attribution.resolve_em_attribution(session, selection)
    attribution.builder._selected_release.assert_not_awaited()
    assert session.info == {}


@pytest.mark.parametrize("invalid", [None, 0, 1, "true"])
async def test_input_resolution_requires_a_boolean_api_flag(invalid):
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs(object(), object(), api_read=invalid)
