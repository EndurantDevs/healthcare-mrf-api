# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual E&M consumer protocol guards; doubles establish no native custody proof."""

import asyncio
import json
from types import SimpleNamespace

import pytest
from sanic.exceptions import ServiceUnavailable

from api import network_read_execution as execution
from api import plan_pricing_em_distance as distance
from api import plan_pricing_em_managed as managed
from api import provider_allowed_amount_policy as allowed
from api import ptg2_geo_policy as geo_policy
from api import ptg2_geo_projection as geo_projection
from api.network_address_scope import scoped_address_relation_sql
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process import network_em_provider_inputs as inputs
from process import network_read_identity as identity
from process.network_compact_layout import CompactAddressRelation
from tests import test_network_em_api_metadata as metadata
from tests import test_network_em_provider_inputs as fixture
from tests.test_provider_group_plan_network_read import held_scope


async def _input_session(monkeypatch):
    driver, manifest, _actual, resolver, _pins = await metadata._input_reader(monkeypatch)
    session = metadata._RatesSession(driver)
    driver.native_active, driver.after_bound = True, None
    driver.bound_valid, driver.guarded, driver.bound_count = True, True, 0
    driver.expected = {
        (f'"{item.schema_name}"."{item.table_name}"', item.relation_oid) for item in fixture.family().relations
    }
    original = driver.fetchrow

    async def fetchrow(query, *parameters):
        if query != identity._BOUND_NAMES_SQL:
            return await original(query, *parameters)
        names, oids, pid = parameters
        assert set(zip(names, oids, strict=True)) == driver.expected
        assert len(names) == len(driver.expected)
        driver.events.append("names-guard")
        driver.bound_count += 1
        driver.guarded = True
        if driver.after_bound:
            driver.after_bound()
        return {"backend_matches": pid == driver.pid, "relation_count": len(names), "valid": driver.bound_valid}

    monkeypatch.setattr(driver, "fetchrow", fetchrow)
    monkeypatch.setattr(driver, "is_in_transaction", lambda: driver.native_active)
    return session, manifest, resolver


async def test_session_inputs_keep_complete_native_recipe_before_install(monkeypatch):
    session, manifest, resolver = await _input_session(monkeypatch)
    actual = await inputs.resolve_em_provider_inputs_in_session(session, manifest, "control")
    assert actual == fixture.family()
    resolver.assert_awaited_once_with(
        session.driver, generation_id=manifest.generation_id, control_schema="control", api_read=True
    )
    installed = execution._installed_read_inventory(session)
    assert installed.connection is session.driver and installed.backend_pid == 42
    assert type(installed.relations) is tuple and len(installed.relations) == 9
    assert installed.relations == tuple(
        CompactAddressRelation(item.schema_name, item.table_name, item.relation_oid) for item in actual.relations
    )
    assert session.driver.events == ["header-read", "header-guard", "native-catalog", "names-guard"]


async def test_session_inputs_pin_transaction_before_initial_driver_acquisition(monkeypatch):
    session, manifest, resolver = await _input_session(monkeypatch)
    original = session.connection

    async def connection():
        session.transaction = object()
        return await original()

    monkeypatch.setattr(session, "connection", connection)
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs_in_session(session, manifest, "control")
    resolver.assert_not_awaited()
    assert session.driver.events == [] and session.info == {}


@pytest.mark.parametrize("phase", ["resolve", "install"])
@pytest.mark.parametrize("change", ["driver", "pid", "transaction", "inactive"])
async def test_session_inputs_reject_any_original_context_change(monkeypatch, phase, change):
    session, manifest, _resolver = await _input_session(monkeypatch)
    original_driver = session.driver

    def mutate():
        if change == "driver":
            session.driver = SimpleNamespace(get_server_pid=lambda: 42, is_in_transaction=lambda: True)
        elif change == "pid":
            original_driver.pid += 1
        elif change == "transaction":
            session.transaction = object()
        else:
            original_driver.native_active = False

    if phase == "resolve":
        original = inputs.resolve_em_provider_inputs

        async def resolved(*args, **kwargs):
            result = await original(*args, **kwargs)
            mutate()
            return result

        monkeypatch.setattr(inputs, "resolve_em_provider_inputs", resolved)
    else:
        original_driver.after_bound = mutate
    with pytest.raises(PlanPricingProjectionUnavailable):
        await inputs.resolve_em_provider_inputs_in_session(session, manifest, "control")
    assert session.info == {}


@pytest.mark.parametrize("phase", ["resolve", "install"])
async def test_session_inputs_preserve_cancellation_without_install(monkeypatch, phase):
    session, manifest, resolver = await _input_session(monkeypatch)
    if phase == "resolve":
        resolver.side_effect = asyncio.CancelledError()
    else:

        def cancelled():
            raise asyncio.CancelledError()

        session.driver.after_bound = cancelled
    with pytest.raises(asyncio.CancelledError):
        await inputs.resolve_em_provider_inputs_in_session(session, manifest, "control")
    assert session.info == {}


class _PageResult:
    def __init__(self, driver, rows):
        self.driver, self.rows, self.exposed, self.closed = driver, rows, False, False

    def mappings(self):
        assert self.driver.guarded
        self.exposed = True
        return SimpleNamespace(all=lambda: self.rows)

    def close(self):
        self.closed = True


@pytest.mark.parametrize("drift", [False, True])
async def test_progressive_windows_guard_nine_native_names_before_counts(monkeypatch, drift):
    session, manifest, _resolver = await _input_session(monkeypatch)
    await inputs.resolve_em_provider_inputs_in_session(session, manifest, "control")
    page_results = []

    async def execute(_statement, parameters):
        session.driver.guarded = False
        session.driver.bound_valid = not drift
        rows = [{"projection_ready": True, "candidate_count": 128 if not page_results else 1, "unique_count": 1}]
        page_result = _PageResult(session.driver, rows)
        page_results.append(page_result)
        return page_result

    monkeypatch.setattr(session, "execute", execute)
    arguments_by_name = {"radius_miles": "5"}
    pagination = SimpleNamespace(limit=2, offset=0)
    selection = SimpleNamespace(serving_revision_id="example", binding_set_digest="a" * 64)
    if drift:
        with pytest.raises(ServiceUnavailable):
            await distance._progressive_page_rows(
                session, selection, arguments_by_name, pagination, ("60611", None, None), 3
            )
        assert len(page_results) == 1 and page_results[0].closed and not page_results[0].exposed
    else:
        page = await distance._progressive_page_rows(
            session, selection, arguments_by_name, pagination, ("60611", None, None), 3
        )
        assert page == (page_results[-1].rows, True, False, 1)
        assert len(page_results) == 2 and all(page_result.exposed for page_result in page_results)
    assert session.driver.bound_count == 1 + len(page_results)


class _Envelope(dict):
    def __init__(self, driver, rows):
        encoded = json.dumps(rows)
        super().__init__(count=len(rows), bytes=len(encoded), rows_json=encoded)
        self.driver, self.accesses = driver, []

    def __getitem__(self, name):
        assert self.driver.guarded
        self.accesses.append(name)
        return super().__getitem__(name)


@pytest.mark.parametrize("change", [None, "names", "driver", "pid", "transaction", "inactive"])
async def test_native_completion_guards_opaque_envelope_before_bounds_or_json(monkeypatch, change):
    session, manifest, _resolver = await _input_session(monkeypatch)
    held = await inputs.resolve_em_provider_inputs_in_session(session, manifest, "control")
    driver = session.driver
    driver.expected.add(('"held_rates"."rates"', 900))
    await execution.install_network_read_inventory(session, (CompactAddressRelation("held_rates", "rates", 900),))
    envelope = _Envelope(
        driver, [{"npi": 1003000000, "minimum_rates": ["10"] * 6, "maximum_rates": ["20"] * 6, "rate_counts": [2] * 6}]
    )
    original = driver.fetchrow

    async def fetchrow(query, *parameters):
        if query == identity._BOUND_NAMES_SQL:
            return await original(query, *parameters)
        assert query.startswith("WITH source_rows AS MATERIALIZED (")
        assert parameters == ([1003000000], [3, 11], 3)
        assert held.relation("npi") in query and '"held_rates"."rates"' in query
        driver.guarded = False
        if change == "names":
            driver.bound_valid = False
        if change == "driver":
            session.driver = SimpleNamespace(get_server_pid=lambda: 42, is_in_transaction=lambda: True)
        if change == "pid":
            driver.pid += 1
        if change == "transaction":
            session.transaction = object()
        if change == "inactive":
            driver.native_active = False
        return envelope

    monkeypatch.setattr(driver, "fetchrow", fetchrow)
    provider_rows = [{"npi": 1003000000, "distance_miles": 1.5}]
    if change:
        with pytest.raises(ServiceUnavailable):
            await managed._complete_page(session, held, '"held_rates"."rates"', provider_rows, (3, 11), 3)
        assert envelope.accesses == []
    else:
        cards = await managed._complete_page(session, held, '"held_rates"."rates"', provider_rows, (3, 11), 3)
        assert cards[0]["minimum_negotiated_rate"] == 10 and cards[0]["distance_miles"] == 1.5
        assert {"count", "bytes", "rows_json"} <= set(envelope.accesses)


def test_managed_and_allowed_geo_emitters_use_six_verified_heaps():
    held = fixture.family()
    bindings = held.geo_bindings("mrf")
    with held_scope() as manifest:
        page = managed._page_sql(held, '"held_rates"."rates"')
        joins = allowed.allowed_geo_joins(held, "mrf")
        evidence = allowed.allowed_geo_evidence(held, "mrf")
        for binding in bindings.values():
            relation = f'"{binding["schema_name"]}"."{binding["table_name"]}"'
            for sql in (page, joins + evidence):
                assert f"FROM {relation} " not in sql and f"JOIN {relation} " not in sql
                assert f"FROM ONLY {relation} " in sql or f"JOIN ONLY {relation} " in sql
        assert f"FROM {scoped_address_relation_sql('mrf.entity_address_unified')} addr" in page
        assert f'FROM "{manifest.schema_name}"."entity_address_unified" AS geo_nppes_anchor' in evidence
        assert "ONLY (" not in page + joins + evidence
        assert "rate.binding_ordinal=ANY(CAST(:allowed_binding_ordinals AS integer[]))" in page
        assert 'FROM ONLY "held_rates"."rates" rate' in page
        raw_evidence = geo_projection.legacy_evidence_source_id_sql(
            "addr", schema_name="mrf", dependency_bindings=bindings, unified_schema_name=manifest.schema_name
        )
        assert evidence.replace("FROM ONLY ", "FROM ").replace("JOIN ONLY ", "JOIN ") == f"({raw_evidence})<>0"
    raw_joins = (
        geo_policy.provider_address_identity_reference_joins_sql(
            "addr",
            schema_name="mrf",
            geo_zip_alias="allowed_zip_ref",
            zip_state_alias="allowed_state_ref",
            dependency_bindings=bindings,
        )
        + "\nLEFT "
        + geo_policy.provider_address_point_reference_join_sql(
            "addr", zcta_alias="allowed_zcta_ref", schema_name="mrf", dependency_bindings=bindings
        )
    )
    assert joins.replace("JOIN ONLY ", "JOIN ") == raw_joins
    assert inputs.em_provider_geo_reads_sql(joins, held, "mrf") == joins
    signature = geo_projection.projection_dependency_bindings_match_sql("mrf", bindings)
    assert "to_regclass" in signature
    assert inputs.em_provider_geo_reads_sql(signature, held, "mrf") == signature
    completion = managed._completion_sql(held, '"held_rates"."rates"')
    for role in ("npi", "npi_taxonomy", "nucc_taxonomy"):
        assert "ONLY " + held.relation(role) in completion
        assert "FROM " + held.relation(role) + " " not in completion
        assert "JOIN " + held.relation(role) + " " not in completion
    assert 'FROM ONLY "held_rates"."rates" rate' in completion


@pytest.mark.parametrize("damage", ["type", "family", "binding", "sql", "schema"])
def test_direct_geo_renderer_rejects_invalid_input_shapes(damage):
    held, sql, schema = fixture.family(), "SELECT 1", "mrf"
    if damage == "type":
        held = SimpleNamespace()
    elif damage == "family":
        object.__setattr__(held, "relations", held.relations[:-1])
    elif damage == "binding":
        object.__setattr__(held.relations[1], "table_name", "invalid.name")
    elif damage == "sql":
        sql = None
    else:
        schema = "invalid.schema"
    with pytest.raises(ValueError):
        inputs.em_provider_geo_reads_sql(sql, held, schema)
