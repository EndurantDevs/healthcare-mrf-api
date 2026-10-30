# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Guarded E&M metadata seams; protocol doubles are not native custody proof."""

import asyncio
import copy
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable

from api import network_read_execution as execution
from api import plan_pricing_em_attribution as attribution
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process import network_candidate_custody as custody
from process import network_em_provider_inputs as inputs
from process import network_read_identity as identity
from process import network_serving_read as serving
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
        self.control_catalog = {
            "table_oid": 800,
            "backend_pid": 42,
            **dict.fromkeys(
                (
                    "reader",
                    "owners_closed",
                    "elevated_closed",
                    "schema_read_only",
                    "table_read_only",
                    "columns_read_only",
                    "heap",
                    "columns_valid",
                ),
                True,
            ),
        }
        self.header = _Header(
            self,
            {
                "source_generations": {"em_provider_inputs": receipt["receipt_sha256"]},
                "validation_json": {"em_provider_inputs": copy.deepcopy(receipt)},
            },
        )
        self.rows = input_fixture.catalog()
        self.events = []

    def is_in_transaction(self):
        return True

    def get_server_pid(self):
        return self.pid

    async def fetchval(self, query, *_args):
        return "repeatable read" if query.startswith("SHOW") else True

    async def fetch(self, query, *_args):
        assert query == inputs._CATALOG_SQL and self.header_guarded
        self.events.append("native-catalog")
        return self.rows

    async def fetchrow(self, query, *arguments):
        if "transaction_isolation" in query:
            return self.modes
        if query == inputs._LEGACY_CONTROL_SQL:
            assert arguments == ("control",)
            self.events.append("legacy-control-catalog")
            return self.control_catalog
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
    object.__setattr__(supplied, "_custody_control", {"candidate_table_oid": 999})
    object.__setattr__(actual, "_custody_control", {"candidate_table_oid": 800})
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
    pins.assert_awaited_once_with(
        driver, ("network_membership_candidate",), control_schema="control", expected_candidate_table_oid=800
    )
    assert driver.events == ["header-read", "header-guard", "native-catalog"]


@pytest.mark.parametrize("damage", ["manifest", "control", "guard", "missing", "sources", "receipt", "native", "pid"])
async def test_api_inputs_preserve_full_manifest_receipt_native_and_consumption_checks(monkeypatch, damage):
    driver, supplied, actual, resolver, pins = await _input_reader(monkeypatch)
    if damage == "manifest":
        resolver.return_value = replace(actual, address_table_oid=999)
    if damage == "control":
        object.__setattr__(actual, "_custody_control", [])
    if damage == "guard":
        driver.is_valid = False
    if damage == "missing":
        driver.header = None
    if damage == "sources":
        dict.__setitem__(driver.header, "source_generations", {})
    if damage == "receipt":
        dict.__setitem__(driver.header, "validation_json", {})
    if damage == "native":
        driver.rows[0]["rolcanlogin"] = True

    async def moved(*_args, **_kwargs):
        driver.pid += 1
        return actual

    if damage == "pid":
        resolver.side_effect = moved
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True)
    if damage in {"manifest", "control"}:
        assert driver.events == []
        pins.assert_not_awaited()
    elif damage != "native":
        assert "native-catalog" not in driver.events


async def test_input_cancellation_is_not_converted_to_metadata_unavailability(monkeypatch):
    driver, supplied, _actual, resolver, _pins = await _input_reader(monkeypatch)
    resolver.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True)


async def _legacy_input_reader(monkeypatch):
    driver, supplied, _actual, _resolver, sealed_pins = await _input_reader(monkeypatch)
    candidate_by_field = {
        "schema_name": supplied.schema_name,
        "source_generations": supplied.source_generations,
        "source_recipes_json": [],
        "validation_json": {},
    }
    entry_by_field = {"candidate_json": json.dumps(candidate_by_field), "manifest_json": "{}", "catalog_json": "{}"}
    readiness = (None, None, None, {"relation_oids": {"entity_address_unified": supplied.address_table_oid}})
    monkeypatch.setattr(serving, "_publication_readiness", lambda _candidate: readiness)
    monkeypatch.setattr(serving, "_retained_manifest", lambda *_args, **_kwargs: asdict(supplied))
    monkeypatch.setattr(serving, "_verified_writer_closure", lambda *_args: supplied.address_table_oid)
    read = AsyncMock(return_value=entry_by_field)
    custody_proof = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_read_manifest", read)
    monkeypatch.setattr(serving, "_candidate_custody", custody_proof)
    resolver = AsyncMock(wraps=serving.resolve_network_serving_manifest)
    monkeypatch.setattr(inputs, "resolve_network_serving_manifest", resolver)
    return driver, supplied, candidate_by_field, entry_by_field, resolver, read, custody_proof, sealed_pins


async def test_legacy_api_checks_manifest_control_and_nine_receipt(monkeypatch):
    driver, supplied, _candidate, _entry, resolver, read, custody_proof, sealed_pins = await _legacy_input_reader(
        monkeypatch
    )
    session = _RatesSession(driver)
    assert await inputs.resolve_em_provider_inputs_in_session(session, supplied, "control") == input_fixture.family()
    resolver.assert_awaited_once_with(
        driver, generation_id=supplied.generation_id, control_schema="control", api_read=True
    )
    read.assert_awaited_once_with(driver, '"control"', supplied.generation_id)
    assert custody_proof.await_args.kwargs == {"api_read": True}
    sealed_pins.assert_not_awaited()
    assert driver.events == ["legacy-control-catalog", "header-read", "header-guard", "native-catalog"]
    assert len(execution._installed_read_inventory(session).relations) == 9


@pytest.mark.parametrize("damage", ["manifest", "compact", "inventory", "contribution"])
async def test_legacy_branch_cannot_infer_acceptance_from_supplied_manifest_json(monkeypatch, damage):
    driver, supplied, candidate, entry, resolver, _read, _custody, sealed_pins = await _legacy_input_reader(monkeypatch)
    if damage == "manifest":
        supplied = replace(supplied, manifest_sha256="f" * 64)
    elif damage == "compact":
        candidate["validation_json"] = {"serving_readiness": {"component": "compact_network_serving"}}
    elif damage == "inventory":
        from process.registry_source_inventory_contract import source_inventory_root
        from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference

        reference = SourceInventoryReference(source_inventory_root(()))
        candidate["source_recipes_json"] = reference.as_dict()
        candidate["source_generations"] = {INVENTORY_DIGEST_KEY: reference.root.entries_sha256}
    else:
        from process.registry_source_contribution_store import registry_source_contribution_sha256

        receipt_by_field = {
            "component": "registry_source_contributions",
            "revision": 1,
            "table_name": "network_source_contribution",
            "sources": [],
        }
        candidate["source_generations"] = {
            "registry_source_contributions": registry_source_contribution_sha256(receipt_by_field)
        }
        candidate["validation_json"] = {"registry_source_contributions": receipt_by_field}
    entry["candidate_json"] = json.dumps(candidate)
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True)
    assert resolver.await_args.kwargs["api_read"] is True
    assert driver.events == []
    sealed_pins.assert_not_awaited()


@pytest.mark.parametrize(
    "field",
    [
        "reader",
        "owners_closed",
        "elevated_closed",
        "schema_read_only",
        "table_read_only",
        "columns_read_only",
        "heap",
        "columns_valid",
        "backend_pid",
        "table_oid",
    ],
)
@pytest.mark.parametrize("value", [False, 0, None])
async def test_legacy_current_control_refuses_role_write_topology_column_and_identity_damage(monkeypatch, field, value):
    driver, supplied, *_rest = await _legacy_input_reader(monkeypatch)
    driver.control_catalog[field] = value
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True)
    assert driver.events == ["legacy-control-catalog"]


async def test_legacy_header_oid_swap_is_refused_before_fields_or_receipt(monkeypatch):
    driver, supplied, *_rest = await _legacy_input_reader(monkeypatch)
    driver.current_control_oid = 801
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs(driver, supplied, "control", api_read=True)
    assert driver.events == ["legacy-control-catalog", "header-read", "header-guard"]


def test_legacy_control_checks_owners_writes_and_native_read_columns():
    sql = inputs._LEGACY_CONTROL_SQL
    assert "ARRAY[n.nspowner,c.relowner]" in sql and "current_user=session_user" in sql
    for mode in ("MEMBER", "USAGE", "SET"):
        assert f"pg_catalog.pg_has_role(r.oid,owner.oid,'{mode}')" in sql
    assert "INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN" in sql
    assert "has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES')" in sql
    assert "elevated.rolreplication OR elevated.rolbypassrls" in sql
    assert "inhrelid=c.oid OR inhparent=c.oid" in sql and "pg_catalog.pg_rewrite" in sql
    for column, native_type, typmod in (
        ("candidate_id", "uuid", -1),
        ("schema_name", "varchar", 68),
        ("state", "varchar", 20),
        ("source_generations", "jsonb", -1),
        ("validation_json", "jsonb", -1),
    ):
        assert f"('{column}','pg_catalog.{native_type}'::pg_catalog.regtype,{typmod})" in sql
    assert "a.attgenerated='' AND a.attidentity=''" in sql


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
        self.execute = AsyncMock(side_effect=AssertionError("API rates proof must not LOCK"))

    def is_in_transaction(self):
        return True

    def get_server_pid(self):
        return self.pid

    async def fetchrow(self, query, *arguments):
        if "transaction_isolation" in query:
            return self.modes
        if query == identity._BOUND_NAMES_SQL:
            names, oids, pid = arguments
            assert names == [f'"{self.receipt.schema_name}"."rates"'] and oids == [self.receipt.table_oid]
            self.guards += 1
            receipt_by_field = {"backend_matches": pid == self.pid, "relation_count": 1, "valid": self.is_valid}
            if self.after_guard:
                self.after_guard()
            return receipt_by_field
        assert query == "SELECT * FROM pg_roles WHERE rolname=$1"
        return rate_fixture._role()


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


def _rates_reader(monkeypatch):
    _old_driver, selection, receipt = rate_fixture._resolver_setup(monkeypatch)
    driver = _RatesDriver(receipt)
    session = _RatesSession(driver)
    return session, selection, receipt


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
async def test_api_rates_preserve_full_release_and_custody_before_install(monkeypatch, isolation):
    session, selection, receipt = _rates_reader(monkeypatch)
    session.driver.modes = (isolation, "on")
    assert await attribution.resolve_em_attribution(session, selection, api_read=True) == receipt
    attribution.builder._selected_release.assert_awaited_once_with(
        session, selection.plan_release_id, selection.serving_revision_id
    )
    attribution._closed.assert_awaited_once_with(
        session.driver, receipt.schema_oid, receipt.owner_oid, receipt.runtime_roles, None
    )
    attribution._content.assert_not_awaited()
    installed = execution._installed_read_inventory(session)
    assert installed.relations == (CompactAddressRelation(receipt.schema_name, "rates", receipt.table_oid),)
    assert installed.connection is session.driver and installed.backend_pid == 42
    session.driver.execute.assert_not_awaited()


@pytest.mark.parametrize("damage", ["mode", "selection", "namespace", "catalog", "owner", "closed"])
async def test_api_rates_do_not_publish_when_an_original_proof_fails(monkeypatch, damage):
    session, selection, _receipt = _rates_reader(monkeypatch)
    if damage == "mode":
        session.driver.modes = ("repeatable read", "off")
    if damage == "selection":
        attribution.builder._selected_release.return_value = replace(selection, binding_set_digest="f" * 64)
    if damage == "namespace":
        attribution._namespace.return_value = None
    if damage == "catalog":
        changed = rate_fixture._catalog()
        changed["relations"][0]["filenode"] += 1
        attribution._catalog.return_value = changed
    original = session.driver.fetchrow

    async def role(query, *args):
        return rate_fixture._role(rolcanlogin=True) if "pg_roles" in query else await original(query, *args)

    if damage == "owner":
        session.driver.fetchrow = role
    if damage == "closed":
        attribution._closed.side_effect = ValueError("runtime closure differs")
    with pytest.raises(PlanPricingProjectionUnavailable):
        await attribution.resolve_em_attribution(session, selection, api_read=True)
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
        await attribution.resolve_em_attribution(session, selection, api_read=True)
    if phase == "selection":
        assert session.info == {}


@pytest.mark.parametrize("drift", [False, True])
async def test_installed_rate_inventory_guards_actual_buffered_rows_before_cache(monkeypatch, drift):
    session, selection, receipt = _rates_reader(monkeypatch)
    await attribution.resolve_em_attribution(session, selection, api_read=True)
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
        await attribution.resolve_em_attribution(session, selection, api_read=True)
    assert session.info == {}


async def test_rates_initial_connection_acquisition_cannot_replace_transaction(monkeypatch):
    session, selection, _receipt = _rates_reader(monkeypatch)
    acquire = session.connection

    async def changed_connection():
        session.transaction = object()
        return await acquire()

    session.connection = changed_connection
    with pytest.raises(PlanPricingProjectionUnavailable):
        await attribution.resolve_em_attribution(session, selection, api_read=True)
    attribution.builder._selected_release.assert_not_awaited()
    assert session.info == {}


@pytest.mark.parametrize("invalid", [None, 0, 1, "true"])
async def test_both_api_modes_require_boolean_flags(invalid):
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs(object(), object(), api_read=invalid)
    with pytest.raises(PlanPricingProjectionUnavailable):
        await attribution.resolve_em_attribution(object(), object(), api_read=invalid)
